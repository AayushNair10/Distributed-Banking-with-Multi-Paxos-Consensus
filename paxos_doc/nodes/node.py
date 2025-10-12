#!/usr/bin/env python3

import argparse
import json
import os
import random
import socket
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

REPLY_BUFFER = 65536
MAJORITY = lambda n: (n // 2) + 1


def now_ts() -> float:
    return time.time()


def send_json(host: str, port: int, payload: dict, timeout: float = 2.0) -> Optional[dict]:
    """Sends a JSON message and waits for a JSON response."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        s.sendall(json.dumps(payload).encode())
        buf = b""
        while True:
            try:
                chunk = s.recv(REPLY_BUFFER)
            except socket.timeout:
                break
            if not chunk:
                break
            buf += chunk
            try:
                return json.loads(buf.decode())
            except Exception:
                continue
        if buf:
            try:
                return json.loads(buf.decode())
            except Exception:
                return None
        return None
    except Exception as e:
        return None
    finally:
        try:
            s.close()
        except Exception:
            pass


class LogEntry:
    def __init__(self, seq: int, msg: Dict[str, Any], state: str, node_id: int):
        self.seq = seq
        self.msg = msg
        self.state = state
        self.node_id = node_id
        self.timestamp = now_ts()

    def to_dict(self):
        return {
            "seq": self.seq,
            "state": self.state,
            "node_id": self.node_id,
            "timestamp": self.timestamp,
            "msg": self.msg,
        }


class NodeServer:
    def __init__(
        self,
        node_id: int,
        host: str,
        port: int,
        config_path: str,
        start_as_leader: bool = False,
        base_x: float = 1.0,
        tp: float = 5.0,
        start_timers: bool = False,
    ):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.config = self._load_config(config_path)
        # Peer nodes, keyed by their ID.
        self.peers: Dict[int, Tuple[str, int]] = {int(n["id"]): (n["host"], int(n["port"])) for n in self.config.get("nodes", [])}
        self.node_count = len(self.peers)

        try:
            sorted_keys = sorted(self.peers.keys())
            self.node_index = sorted_keys.index(self.node_id)
        except Exception:
            self.node_index = 0

        # In-memory database for client balances.
        self.initial_balance = int(self.config.get("initial_balance", 10))
        clients = [str(c).upper() for c in self.config.get("clients", [])]
        self.clients = clients
        self.db: Dict[str, int] = {c: self.initial_balance for c in clients}

        # Paxos-specific state variables.
        self.ballot: List[int] = [1, self.node_id]
        self.promised_ballot: Optional[List[int]] = None
        self.accepted: Dict[int, Tuple[List[int], Dict[str, Any], Optional[str]]] = {}
        self.committed: Dict[int, Dict[str, Any]] = {}
        self.committed_meta: Dict[int, Optional[str]] = {}

        self.next_seq = 1
        self.last_executed_seq = 0

        # State related to the current leader.
        self.is_leader = False
        self.leader_ballot: Optional[List[int]] = None
        self.leader_id: Optional[int] = None

        # Local log entries and a lock for thread safety.
        self.log_entries: List[LogEntry] = []
        self._seq_counter = 0
        self._lock = threading.Lock()

        self.accepted_quorums: Dict[int, set] = {}

        
        self.last_replies_by_req: Dict[str, Dict[str, Any]] = {}
        self.processed_req_ids: set = set()
        self.last_req_by_client: Dict[str, str] = {}

        self.committed_results: Dict[int, str] = {}

        # Timers and settings for leader election.
        self.base_x = float(base_x)
        self.tp = float(tp)
        self.t_timeout = self.base_x * (self.node_index + 1)
        self.request_timer_running = False
        self.request_timer_expire_ts: Optional[float] = None

        
        self.pending_requests: List[Dict[str, Any]] = []

        self.checkpoint_period = int(self.config.get("checkpoint_period", 100))
        self.latest_checkpoint_seq: int = 0
        self.checkpoint_db: Optional[Dict[str, int]] = None

        # For managing background threads.
        self._stop_timer_thread = threading.Event()
        self._timer_thread: Optional[threading.Thread] = None

        self.start_as_leader = start_as_leader
        self._start_timers_on_startup = bool(start_timers)

        # A log file for auditing committed transactions.
        self.commit_log_file = f"node_{self.node_id}_commits.log"
        try:
            open(self.commit_log_file, "a").close()
        except Exception:
            pass

    def _load_config(self, path: str) -> Dict[str, Any]:
        with open(path, "r") as f:
            return json.load(f)

    def _next_local_seq(self) -> int:
        with self._lock:
            self._seq_counter += 1
            return self._seq_counter

    def append_log(self, seq: int, msg: Dict[str, Any], state: str):
        le = LogEntry(seq, msg, state, self.node_id)
        self.log_entries.append(le)

    def _reset_state(self):
        """Resets the node's state back to its initial configuration."""
        with self._lock:
            self.db = {c: self.initial_balance for c in getattr(self, "clients", [])}
            self.ballot = [1, self.node_id]
            self.promised_ballot = None
            self.accepted = {}
            self.committed = {}
            self.committed_meta = {}
            self.next_seq = 1
            self.last_executed_seq = 0
            self.is_leader = False
            self.leader_ballot = None
            self.leader_id = None
            self.log_entries = []
            self._seq_counter = 0
            self.accepted_quorums = {}
            self.last_replies_by_req = {}
            self.committed_results = {}
            self.request_timer_running = False
            self.request_timer_expire_ts = None
            self.latest_checkpoint_seq = 0
            self.checkpoint_db = None
            self.processed_req_ids = set()
            self.last_req_by_client = {}
            self.pending_requests = []
        return True

    def _normalize_ballot(self, b: Optional[List[int]]) -> Tuple[int, int]:
        """Converts a ballot into a tuple for easy comparison. Badly formed ballots get (0,0)."""
        try:
            if b is None:
                return (0, 0)
            if isinstance(b, (list, tuple)) and len(b) >= 2:
                return (int(b[0]), int(b[1]))
            # Try parsing it anyway.
            return (int(b[0]), int(b[1]))
        except Exception:
            return (0, 0)

    def _neighbor_catchup(self):
        """At startup, this node checks its neighbors to find the most up-to-date one and copies its state to catch up."""
        try:
            best_candidate = None  # (node_id, ballot, checkpoint_seq, status_reply)
            for nid, (h, p) in self.peers.items():
                if nid == self.node_id:
                    continue
                try:
                    st = send_json(h, p, {"type": "ADMIN_STATUS"}, timeout=1.0)
                except Exception:
                    st = None
                if not st:
                    continue
                # Use the leader's ballot if available, otherwise the promised one.
                ballot = st.get("leader_ballot") or st.get("promised_ballot") or st.get("leader_ballot")
                cseq = int(st.get("checkpoint_seq", 0) or 0)
                btuple = self._normalize_ballot(ballot)
                if best_candidate is None or btuple > best_candidate[1] or (btuple == best_candidate[1] and cseq > best_candidate[2]):
                    best_candidate = (nid, btuple, cseq, st)
            if best_candidate is None:
                return

            best_nid = best_candidate[0]
            best_peer = self.peers.get(best_nid)
            if not best_peer:
                return
            h, p = best_peer

            # Ask the best peer for its checkpoint data.
            try:
                cp = send_json(h, p, {"type": "ADMIN_CHECKPOINT"}, timeout=1.5)
            except Exception:
                cp = None
            if not cp or not isinstance(cp.get("db"), dict):
                return

            # Clean up the received checkpoint data and see if it's newer.
            try:
                peer_cp_seq = int(cp.get("checkpoint_seq", cp.get("last_executed_seq", 0) or 0) or 0)
            except Exception:
                peer_cp_seq = 0

            if peer_cp_seq <= self.latest_checkpoint_seq:
                return

            try:
                db_norm: Dict[str, int] = {}
                for k, v in cp.get("db", {}).items():
                    try:
                        db_norm[str(k).upper()] = int(v)
                    except Exception:
                        try:
                            db_norm[str(k).upper()] = int(float(v))
                        except Exception:
                            db_norm[str(k).upper()] = 0

                # Safely update sequence numbers.
                with self._lock:
                    self.checkpoint_db = dict(db_norm)
                    self.latest_checkpoint_seq = peer_cp_seq
                    try:
                        peer_next_seq = int(cp.get("next_seq", self.next_seq) or self.next_seq)
                        self.next_seq = max(self.next_seq, peer_next_seq)
                    except Exception:
                        pass
                    self._install_checkpoint(peer_cp_seq, self.checkpoint_db)
            except Exception:
                pass
        except Exception:
            pass

    def start(self):
        self._timer_thread = threading.Thread(target=self._timer_loop, daemon=True)
        self._timer_thread.start()

        if self._start_timers_on_startup:
            self._start_request_timer_if_needed()

        try:
            self._neighbor_catchup()
        except Exception:
            pass

        if self.start_as_leader:
            threading.Thread(target=self._attempt_leadership, daemon=True).start()

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(16)
        try:
            while True:
                conn, addr = server.accept()
                t = threading.Thread(target=self._handle_conn, args=(conn, addr), daemon=True)
                t.start()
        except KeyboardInterrupt:
            pass
        finally:
            server.close()
            self._stop_timer_thread.set()

    def _timer_loop(self):
        while not self._stop_timer_thread.is_set():
            try:
                now = time.time()
                if self.request_timer_running and self.request_timer_expire_ts is not None and now >= self.request_timer_expire_ts:
                    self.request_timer_running = False
                    self.request_timer_expire_ts = None
                    self._on_request_timer_expired()
                time.sleep(0.1)
            except Exception:
                time.sleep(0.1)

    def _start_request_timer_if_needed(self):
        if not self.request_timer_running:
            self.request_timer_running = True
            jitter = random.uniform(0, max(0.1, self.base_x * 0.5))
            self.request_timer_expire_ts = time.time() + self.t_timeout + jitter

    def _stop_request_timer_if_running(self):
        if self.request_timer_running:
            self.request_timer_running = False
            self.request_timer_expire_ts = None

    def _attempt_leadership(self):
        try:
            self._on_request_timer_expired()
        except Exception:
            pass

    def _on_request_timer_expired(self):
        with self._lock:
            self.ballot[0] += 1
        my_ballot = list(self.ballot)

        try:
            if self.promised_ballot is None or list(my_ballot) > list(self.promised_ballot):
                self.promised_ballot = list(my_ballot)
        except Exception:
            pass

        payload = {"type": "PREPARE", "ballot": my_ballot, "from": self.node_id}
        self.append_log(0, {"type": "PREPARE_SENT", "ballot": my_ballot}, "PREPARE_SENT")

        replies = {}
        promise_count = 0
        accepted_accum: List[Dict[str, Any]] = []
        highest_seq = 0
        highest_checkpoint_seq = self.latest_checkpoint_seq
        for nid, (h, p) in self.peers.items():
            r = send_json(h, p, payload, timeout=1.0)
            replies[nid] = r
            if r and r.get("type") == "PROMISE" and r.get("ballot") == my_ballot:
                promise_count += 1
                try:
                    pc = int(r.get("checkpoint_seq", 0) or 0)
                    if pc > highest_checkpoint_seq:
                        highest_checkpoint_seq = pc
                except Exception:
                    pass
                for a in r.get("accepted", []):
                    accepted_accum.append(a)
                    try:
                        s = int(a.get("seq", 0))
                        if s > highest_seq:
                            highest_seq = s
                    except Exception:
                        pass

        if promise_count >= MAJORITY(self.node_count):
            self.is_leader = True
            self.leader_ballot = my_ballot
            self.leader_id = self.node_id
            self.promised_ballot = list(my_ballot)
            self.next_seq = max(self.next_seq, highest_seq + 1)
            if highest_checkpoint_seq > self.latest_checkpoint_seq:
                self.latest_checkpoint_seq = highest_checkpoint_seq
            self.append_log(0, {"type": "NEWLEADER", "ballot": my_ballot}, "NEWLEADER")

            # Build the acceptance log and send a NEWVIEW message.
            accept_map: Dict[int, Dict[str, Any]] = {}
            for a in accepted_accum:
                try:
                    s = int(a.get("seq"))
                except Exception:
                    continue
                ab = a.get("accept_num") or a.get("ballot") or [0, 0]
                reqid = a.get("req_id") if isinstance(a, dict) else None
                ab_tuple = tuple(ab)
                existing = accept_map.get(s)
                if existing is None:
                    accept_map[s] = {"ballot": list(ab_tuple), "seq": s, "txn": a.get("txn"), "req_id": reqid}
                else:
                    if tuple(existing["ballot"]) < ab_tuple:
                        accept_map[s] = {"ballot": list(ab_tuple), "seq": s, "txn": a.get("txn"), "req_id": reqid}

            accept_log_entries: List[Dict[str, Any]] = []
            for s in range(1, highest_seq + 1):
                if s in accept_map:
                    txn = accept_map[s]["txn"]
                    reqid = accept_map[s].get("req_id")
                else:
                    txn = {"noop": True}
                    reqid = None
                accept_entry = {"ballot": my_ballot, "seq": s, "txn": txn}
                if reqid:
                    accept_entry["req_id"] = reqid
                accept_log_entries.append(accept_entry)
                # As the new leader, record these accepted values.
                self.accepted[s] = (list(my_ballot), txn, accept_entry.get("req_id"))

            newview_payload = {
                "type": "NEWVIEW",
                "ballot": my_ballot,
                "accepts": accept_log_entries,
                "from": self.node_id,
                "checkpoint_seq": self.latest_checkpoint_seq,
            }

            self.append_log(0, newview_payload, "NEWVIEW_SENT")

            ack_replies = {}
            for nid, (h, p) in self.peers.items():
                r = send_json(h, p, newview_payload, timeout=1.5)
                ack_replies[nid] = r

            accept_ack_counts: Dict[int, int] = {entry["seq"]: 0 for entry in accept_log_entries}
            for entry in accept_log_entries:
                accept_ack_counts[entry["seq"]] += 1
            for nid, r in ack_replies.items():
                if not r:
                    continue
                if r.get("type") == "NEWVIEW_ACK":
                    accepted_seqs = r.get("accepted_seqs", [])
                    for s in accepted_seqs:
                        if s in accept_ack_counts:
                            accept_ack_counts[s] += 1

            for entry in accept_log_entries:
                s = entry["seq"]
                txn = entry["txn"]
                reqid = entry.get("req_id")
                if accept_ack_counts.get(s, 0) >= MAJORITY(self.node_count):
                    # If we've already processed this request, don't commit it again.
                    if reqid and reqid in self.processed_req_ids:
                        self.append_log(s, {"type": "COMMIT_SKIPPED_DUP_REQ", "seq": s, "txn": txn, "req_id": reqid}, "COMMIT_SKIPPED")
                        continue
                    self.committed[s] = txn
                    self.committed_meta[s] = reqid
                    self.append_log(s, {"type": "COMMIT", "seq": s, "txn": txn, "req_id": reqid}, "COMMITTED_BY_NEWVIEW")
                    commit_payload = {"type": "COMMIT", "seq": s, "txn": txn, "from": self.node_id}
                    if reqid:
                        commit_payload["req_id"] = reqid
                    for nid, (h, p) in self.peers.items():
                        try:
                            send_json(h, p, commit_payload, timeout=1.0)
                        except Exception:
                            pass
                    # Write the commit to a file.
                    self._log_commit_to_file(s, txn, reqid)
                else:
                    self.append_log(s, {"type": "COMMIT_NOT_REACHED", "seq": s, "acks": accept_ack_counts.get(s, 0)}, "COMMIT_NOT_REACHED")

            executed = self._try_execute()

            # Now that we're the leader, process any pending requests.
            pending_count = len(self.pending_requests)
            processed = 0
            while self.pending_requests:
                req = self.pending_requests.pop(0)
                client_id = req.get("client_id")
                req_id = req.get("req_id")
                txn = req.get("txn") or {}
                timestamp = req.get("timestamp", now_ts())

                # If this request is a duplicate, just return the cached reply.
                if req_id and req_id in self.processed_req_ids:
                    cached = self.last_replies_by_req.get(req_id)
                    if cached:
                        # This was already processed, so just update the cache.
                        self.last_replies_by_req[req_id] = cached
                        processed += 1
                        continue

                seq = self.next_seq
                self.next_seq += 1
                self.append_log(seq, {"type": "REQUEST_PROPOSE", "req_id": req_id, "client_id": client_id, "txn": txn}, "PROPOSE")

                payload_accept = {"type": "ACCEPT", "ballot": self.leader_ballot, "seq": seq, "txn": txn, "from": self.node_id}
                if req_id:
                    payload_accept["req_id"] = req_id

                local_reply = self._on_accept(payload_accept)
                accept_count = 0
                accepted_nodes = set()
                if local_reply.get("type") == "ACCEPTED":
                    accept_count += 1
                    accepted_nodes.add(self.node_id)

                for nid, (h, p) in self.peers.items():
                    if nid == self.node_id:
                        continue
                    r = send_json(h, p, payload_accept, timeout=1.0)
                    if r and r.get("type") == "ACCEPTED":
                        accept_count += 1
                        accepted_nodes.add(nid)

                self.accepted_quorums[seq] = accepted_nodes

                if accept_count >= MAJORITY(self.node_count):
                    # Avoid committing a duplicate request.
                    if req_id and req_id in self.processed_req_ids:
                        self.append_log(seq, {"type": "COMMIT_SKIPPED_DUP_REQ", "seq": seq, "txn": txn, "req_id": req_id}, "COMMIT_SKIPPED")
                        processed += 1
                        continue

                    # Store the committed transaction and its ID.
                    self.committed[seq] = txn
                    self.committed_meta[seq] = req_id
                    self.append_log(seq, {"type": "COMMIT_SENT", "seq": seq, "txn": txn, "req_id": req_id}, "COMMIT_SENT")
                    commit_payload = {"type": "COMMIT", "seq": seq, "txn": txn, "from": self.node_id}
                    if req_id:
                        commit_payload["req_id"] = req_id
                    for nid, (h, p) in self.peers.items():
                        try:
                            send_json(h, p, commit_payload, timeout=1.0)
                        except Exception:
                            pass
                    # Write the commit to a file.
                    self._log_commit_to_file(seq, txn, req_id)

                    executed_now = self._try_execute()
                    result = self.committed_results.get(seq, "FAILED")
                    reply = {"type": "REPLY", "ballot": self.leader_ballot, "timestamp": timestamp, "client_id": client_id, "req_id": req_id, "result": result, "status": "COMMITTED", "node_id": self.node_id}
                    if req_id:
                        self.last_replies_by_req[req_id] = reply
                        self.last_req_by_client[client_id] = req_id
                    self.append_log(seq, {"type": "REPLY_SENT", "req_id": req_id, "client_id": client_id, "result": result}, "REPLY_SENT")
                    if executed_now:
                        self._maybe_send_checkpoint()
                else:
                    self.append_log(seq, {"type": "COMMIT_NOT_REACHED", "seq": seq, "acks": accept_count}, "COMMIT_NOT_REACHED")
                processed += 1

            if not self.pending_requests:
                self._stop_request_timer_if_running()
        else:
            pass

    def _handle_conn(self, conn: socket.socket, addr):
        try:
            raw = conn.recv(REPLY_BUFFER)
            if not raw:
                conn.close()
                return
            try:
                msg = json.loads(raw.decode())
            except Exception:
                try:
                    conn.sendall(json.dumps({"error": "invalid json"}).encode())
                except Exception:
                    pass
                conn.close()
                return

            mtype = msg.get("type", "UNKNOWN")
            seq_local = self._next_local_seq()
            self.append_log(seq_local, msg, "RECEIVED")

            # Handle admin commands.
            if mtype == "PING":
                reply = {"type": "PONG", "node_id": self.node_id, "ts": now_ts()}

            elif mtype == "ADMIN_RESET":
                try:
                    self._reset_state()
                    reply = {"type": "ADMIN_RESET_OK", "node_id": self.node_id}
                except Exception as e:
                    reply = {"type": "ADMIN_RESET_ERR", "error": str(e), "node_id": self.node_id}

            elif mtype == "ADMIN_SHUTDOWN":
                reply = {"type": "ADMIN_SHUTDOWN_ACK", "node_id": self.node_id}
                try:
                    conn.sendall(json.dumps(reply).encode())
                except Exception:
                    pass
                try:
                    self._stop_timer_thread.set()
                except Exception:
                    pass
                os._exit(0)

            elif mtype == "ADMIN_PRINTDB":
                try:
                    with self._lock:
                        dbcopy = {str(k).upper(): int(v) for k, v in self.db.items()}
                        reply = {
                            "type": "PRINTDB_REPLY",
                            "node_id": self.node_id,
                            "db": dbcopy,
                            "next_seq": int(self.next_seq),
                            "last_executed_seq": int(self.last_executed_seq),
                            "checkpoint_seq": int(self.latest_checkpoint_seq),
                        }
                except Exception:
                    reply = {"type": "PRINTDB_ERR", "node_id": self.node_id}

            elif mtype == "ADMIN_PRINTLOG":
                reply = {"type": "PRINTLOG_REPLY", "node_id": self.node_id, "log": [le.to_dict() for le in self.log_entries[-200:]]}

            elif mtype == "ADMIN_STATUS":
                try:
                    reply = {
                        "type": "ADMIN_STATUS_REPLY",
                        "node_id": self.node_id,
                        "is_leader": bool(self.is_leader),
                        "leader_id": self.leader_id,
                        "pending_requests": len(self.pending_requests),
                        "next_seq": self.next_seq,
                        "last_executed_seq": self.last_executed_seq,
                        "request_timer_running": bool(self.request_timer_running),
                        "promised_ballot": self.promised_ballot,
                        "leader_ballot": self.leader_ballot,
                        "checkpoint_seq": int(self.latest_checkpoint_seq),
                        "committed_count": len(self.committed),
                        "db_checksum": sum(self.db.values()),  # A simple checksum for quick debugging
                    }
                except Exception:
                    reply = {"type": "ADMIN_STATUS_REPLY", "node_id": self.node_id}

            elif mtype == "ADMIN_CHECKPOINT":
                try:
                    with self._lock:
                        dbcopy = {str(k).upper(): int(v) for k, v in self.db.items()}
                        # Get a map of committed metadata up to the last checkpoint.
                        cm = {}
                        for s, rid in self.committed_meta.items():
                            try:
                                if int(s) <= int(self.latest_checkpoint_seq):
                                    cm[int(s)] = rid
                            except Exception:
                                pass
                        reply = {
                            "type": "ADMIN_CHECKPOINT_REPLY",
                            "node_id": self.node_id,
                            "db": dbcopy,
                            "next_seq": int(self.next_seq),
                            "last_executed_seq": int(self.last_executed_seq),
                            "checkpoint_seq": int(self.latest_checkpoint_seq),
                            "committed_meta": cm,
                        }
                except Exception:
                    reply = {"type": "ADMIN_CHECKPOINT_ERR", "node_id": self.node_id}

            elif mtype == "ADMIN_CATCHUP":
                try:
                    payload_db = msg.get("db", {}) or {}
                    incoming_next_seq = int(msg.get("next_seq", 1))
                    incoming_last_exec = msg.get("last_executed_seq", None)
                    incoming_commits = msg.get("commits", []) or []
                    incoming_committed_meta = msg.get("committed_meta", {}) or {}
                    incoming_ballot = msg.get("leader_ballot")
                    incoming_leader_id = msg.get("leader_id")

                    normalized = {}
                    for k, v in payload_db.items():
                        try:
                            normalized[str(k).upper()] = int(v)
                        except Exception:
                            try:
                                normalized[str(k).upper()] = int(float(v))
                            except Exception:
                                normalized[str(k).upper()] = 0

                    with self._lock:
                        old_db = dict(self.db)
                        self.db = dict(normalized)

                        old_next = self.next_seq
                        old_last_exec = self.last_executed_seq
                        self.next_seq = max(self.next_seq, int(incoming_next_seq))
                        if incoming_last_exec is not None:
                            self.last_executed_seq = max(self.last_executed_seq, int(incoming_last_exec))
                        else:
                            self.last_executed_seq = max(self.last_executed_seq, int(self.next_seq) - 1)

                        if incoming_commits and isinstance(incoming_commits, list):
                            for entry in incoming_commits:
                                try:
                                    s = int(entry.get("seq"))
                                    txn = entry.get("txn") or {}
                                    reqid = entry.get("req_id", None)

                                    self.committed[s] = txn
                                    if reqid:
                                        self.committed_meta[s] = reqid
                                        self.processed_req_ids.add(reqid)
                                        if reqid not in self.last_replies_by_req:
                                            res = self.committed_results.get(s, "SUCCESS")
                                            self.last_replies_by_req[reqid] = {
                                                "type": "REPLY", "status": res, "req_id": reqid, "node_id": self.node_id
                                            }
                                except Exception as e:
                                    pass

                        if incoming_committed_meta and isinstance(incoming_committed_meta, dict):
                            for k, rid in incoming_committed_meta.items():
                                try:
                                    s = int(k) if isinstance(k, int) else int(str(k))
                                    if rid:
                                        self.committed_meta[s] = rid
                                        self.processed_req_ids.add(rid)
                                        if rid not in self.last_replies_by_req:
                                            res = self.committed_results.get(s, "SUCCESS")
                                            self.last_replies_by_req[rid] = {
                                                "type": "REPLY", "status": res, "req_id": rid, "node_id": self.node_id
                                            }
                                except Exception:
                                    pass

                        if incoming_ballot and isinstance(incoming_ballot, (list, tuple)) and len(incoming_ballot) >= 2:
                            try:
                                leader_ballot_num = int(incoming_ballot[0])
                                leader_node_id = int(incoming_ballot[1])

                                self.ballot = [leader_ballot_num + 1, self.node_id]
                                self.promised_ballot = list(incoming_ballot)
                                self.leader_id = leader_node_id if incoming_leader_id is None else int(incoming_leader_id)
                                self.leader_ballot = list(incoming_ballot)
                                self.is_leader = False

                            except Exception as e:
                                pass

                        self._stop_request_timer_if_running()
                        self.request_timer_running = False
                        self.request_timer_expire_ts = None
                        self.pending_requests = []

                    reply = {
                        "type": "ADMIN_CATCHUP_ACK",
                        "node_id": self.node_id,
                        "next_seq": int(self.next_seq),
                        "last_executed_seq": int(self.last_executed_seq),
                        "committed_count": len(self.committed)
                    }
                except Exception as e:
                    reply = {"type": "ADMIN_CATCHUP_ERR", "error": str(e), "node_id": self.node_id}

            # Handle Paxos protocol messages.
            elif mtype == "PREPARE":
                reply = self._on_prepare(msg)

            elif mtype == "ACCEPT":
                reply = self._on_accept(msg)

            elif mtype == "ACCEPTED":
                reply = self._on_accepted(msg)

            elif mtype == "COMMIT":
                reply = self._on_commit(msg)

            elif mtype == "REQUEST":
                reply = self._on_request(msg)

            elif mtype == "NEWVIEW" or mtype == "NEW-VIEW":
                reply = self._on_newview(msg)

            elif mtype == "CHECKPOINT":
                try:
                    seq = int(msg.get("seq", 0))
                    db = msg.get("db", {}) or {}
                    norm = {}
                    for k, v in db.items():
                        try:
                            norm[str(k).upper()] = int(v)
                        except Exception:
                            try:
                                norm[str(k).upper()] = int(float(v))
                            except Exception:
                                norm[str(k).upper()] = 0
                    with self._lock:
                        self.checkpoint_db = dict(norm)
                        if seq > self.latest_checkpoint_seq:
                            self.latest_checkpoint_seq = seq
                            self._install_checkpoint(seq, self.checkpoint_db)
                    reply = {"type": "CHECKPOINT_ACK", "node_id": self.node_id, "checkpoint_seq": int(self.latest_checkpoint_seq)}
                except Exception as e:
                    reply = {"type": "CHECKPOINT_ERR", "node_id": self.node_id, "error": str(e)}

            else:
                reply = {"error": f"unknown message type {mtype}"}

            try:
                conn.sendall(json.dumps(reply).encode())
            except Exception:
                pass
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _on_prepare(self, msg: dict) -> dict:
        b = msg.get("ballot")
        from_id = msg.get("from")
        if self.promised_ballot is None or list(b) >= list(self.promised_ballot):
            self.promised_ballot = list(b)
            accept_log = []
            for s, (ab, txn, reqid) in self.accepted.items():
                entry = {"accept_num": ab, "seq": s, "txn": txn}
                if reqid:
                    entry["req_id"] = reqid
                accept_log.append(entry)
            self.append_log(0, {"type": "PROMISE", "ballot": b, "to": from_id}, "PROMISE_SENT")
            return {"type": "PROMISE", "ballot": b, "accepted": accept_log, "from": self.node_id, "checkpoint_seq": int(self.latest_checkpoint_seq)}
        else:
            return {"type": "PROMISE_REJECT", "promised_ballot": self.promised_ballot, "from": self.node_id}

    def _on_accept(self, msg: dict) -> dict:
        from_id = msg.get("from")
        if from_id is not None:
            try:
                self.leader_id = int(from_id)
            except Exception:
                pass

        b = msg.get("ballot")
        seq = int(msg.get("seq"))
        txn = msg.get("txn")
        req_id = msg.get("req_id") or None

        # If we've seen this request before, just ACK without re-processing.
        if req_id and req_id in self.processed_req_ids:
            # Still need to update the ballot to prevent issues.
            if self.promised_ballot is None or list(b) >= list(self.promised_ballot):
                self.promised_ballot = list(b)
            self.append_log(seq, {"type": "ACCEPT_DUP_SKIPPED", "ballot": b, "seq": seq, "txn": txn, "req_id": req_id, "from": from_id}, "ACCEPT_DUP_SKIPPED")
            return {"type": "ACCEPTED", "ballot": b, "seq": seq, "txn": txn, "req_id": req_id, "from": self.node_id}

        if self.promised_ballot is None or list(b) >= list(self.promised_ballot):
            self.promised_ballot = list(b)
            # Store the request ID with the accepted proposal.
            self.accepted[seq] = (list(b), txn, req_id)
            self.append_log(seq, {"type": "ACCEPT", "ballot": b, "seq": seq, "txn": txn, "req_id": req_id, "from": from_id}, "ACCEPTED")
            return {"type": "ACCEPTED", "ballot": b, "seq": seq, "txn": txn, "req_id": req_id, "from": self.node_id}
        else:
            return {"type": "ACCEPT_REJECTED", "promised_ballot": self.promised_ballot, "from": self.node_id}

    def _on_accepted(self, msg: dict) -> dict:
        seq = int(msg.get("seq", 0))
        self.append_log(seq, msg, "ACCEPTED_RECV")
        return {"type": "ACK", "from": self.node_id}

    def _on_commit(self, msg: dict) -> dict:
        from_id = msg.get("from")
        if from_id is not None:
            try:
                self.leader_id = int(from_id)
            except Exception:
                pass

        seq = int(msg.get("seq"))
        txn = msg.get("txn")
        req_id = msg.get("req_id") or None

        # If this is a duplicate commit, acknowledge but ignore it.
        if req_id and req_id in self.processed_req_ids:
            self.append_log(seq, {"type": "COMMIT_IGNORED_DUP_REQ", "seq": seq, "txn": txn, "req_id": req_id}, "COMMIT_IGNORED")
            # Record it for log consistency, but don't execute the transaction again.
            self.committed[seq] = txn
            self.committed_meta[seq] = req_id
            return {"type": "COMMIT_ACK", "seq": seq, "from": self.node_id, "skipped_duplicate": True}

        # Accept the commit.
        self.committed[seq] = txn
        if req_id:
            self.committed_meta[seq] = req_id
        else:
            existing = self.accepted.get(seq)
            if existing and existing[2] is not None:
                self.committed_meta[seq] = existing[2]
            else:
                self.committed_meta[seq] = None

        try:
            existing = self.accepted.get(seq)
            if existing and existing[2] is None and req_id:
                self.accepted[seq] = (existing[0], existing[1], req_id)
        except Exception:
            pass

        self.append_log(seq, {"type": "COMMIT", "seq": seq, "txn": txn, "req_id": req_id}, "COMMITTED")
        self._log_commit_to_file(seq, txn, req_id)

        executed = self._try_execute()
        if executed and self.is_leader:
            self._maybe_send_checkpoint()

        return {"type": "COMMIT_ACK", "seq": seq, "from": self.node_id}

    def _on_newview(self, msg: dict) -> dict:
        my_reply_accepted: List[int] = []
        b = msg.get("ballot")
        accepts = msg.get("accepts", [])
        incoming_checkpoint_seq = int(msg.get("checkpoint_seq", 0) or 0)

        try:
            leader_from = int(msg.get("from"))
            self.leader_id = leader_from
            if isinstance(b, (list, tuple)):
                self.leader_ballot = list(b)
            self.is_leader = (self.node_id == self.leader_id)
            if self.leader_ballot is not None and (self.promised_ballot is None or list(self.leader_ballot) > list(self.promised_ballot)):
                self.promised_ballot = list(self.leader_ballot)
        except Exception:
            pass

        if incoming_checkpoint_seq > self.latest_checkpoint_seq:
            try:
                if self.leader_id in self.peers:
                    h, p = self.peers[self.leader_id]
                    rcp = send_json(h, p, {"type": "ADMIN_CHECKPOINT"}, timeout=1.0)
                    if rcp and isinstance(rcp.get("db"), dict):
                        with self._lock:
                            norm = {}
                            for k, v in rcp.get("db", {}).items():
                                try:
                                    norm[str(k).upper()] = int(v)
                                except Exception:
                                    try:
                                        norm[str(k).upper()] = int(float(v))
                                    except Exception:
                                        norm[str(k).upper()] = 0
                            seq = int(rcp.get("checkpoint_seq", rcp.get("last_executed_seq", 0) or 0))
                            self.checkpoint_db = dict(norm)
                            if seq > self.latest_checkpoint_seq:
                                self.latest_checkpoint_seq = seq
                                self._install_checkpoint(seq, self.checkpoint_db)
            except Exception:
                pass

        for entry in accepts:
            seq = int(entry.get("seq"))
            ballot_for_accept = entry.get("ballot")
            txn = entry.get("txn")
            reqid = entry.get("req_id", None)
            if self.promised_ballot is None or list(ballot_for_accept) >= list(self.promised_ballot):
                self.promised_ballot = list(ballot_for_accept)
                self.accepted[seq] = (list(ballot_for_accept), txn, reqid)
                self.append_log(seq, {"type": "ACCEPT_FROM_NEWVIEW", "ballot": ballot_for_accept, "seq": seq, "txn": txn, "req_id": reqid}, "ACCEPTED_NEWVIEW")
                my_reply_accepted.append(seq)
            else:
                self.append_log(seq, {"type": "ACCEPT_REJECTED_NEWVIEW", "ballot": ballot_for_accept, "seq": seq}, "ACCEPT_REJECTED_NEWVIEW")

        reply = {"type": "NEWVIEW_ACK", "accepted_seqs": my_reply_accepted, "from": self.node_id, "checkpoint_seq": int(self.latest_checkpoint_seq)}
        return reply

    def _on_request(self, msg: dict) -> dict:
        """Handles a client's request. If this node is the leader, it processes the request. If not, it forwards it to the known leader. If no leader is known, it queues the request."""
        client_id = msg.get("client_id")
        req_id = msg.get("req_id")
        txn = msg.get("txn") or {}
        timestamp = msg.get("timestamp", now_ts())

        if req_id is None and client_id:
            req_id = self.last_req_by_client.get(client_id)

        if req_id and req_id in self.last_replies_by_req:
            return self.last_replies_by_req[req_id]

        if req_id and any(pr.get("req_id") == req_id for pr in self.pending_requests):
            return {"type": "REPLY", "status": "ALREADY_QUEUED", "node_id": self.node_id, "req_id": req_id}

        if client_id and req_id:
            self.last_req_by_client[client_id] = req_id

        if req_id and req_id in self.processed_req_ids:
            cached = self.last_replies_by_req.get(req_id)
            if cached:
                return cached

        if self.is_leader:
            if self.leader_ballot is None:
                self.leader_ballot = list(self.ballot)

            seq = self.next_seq
            self.next_seq += 1
            self.append_log(seq, {"type": "REQUEST_PROPOSE", "req_id": req_id, "client_id": client_id, "txn": txn}, "PROPOSE")

            payload = {"type": "ACCEPT", "ballot": self.leader_ballot, "seq": seq, "txn": txn, "from": self.node_id}
            if req_id:
                payload["req_id"] = req_id

            local_reply = self._on_accept(payload)
            accept_count = 0
            accepted_nodes = set()
            if local_reply.get("type") == "ACCEPTED":
                accept_count += 1
                accepted_nodes.add(self.node_id)

            for nid, (h, p) in self.peers.items():
                if nid == self.node_id:
                    continue
                r = send_json(h, p, payload, timeout=1.0)
                if r and r.get("type") == "ACCEPTED":
                    accept_count += 1
                    accepted_nodes.add(nid)

            self.accepted_quorums[seq] = accepted_nodes

            if accept_count >= MAJORITY(self.node_count):
                if req_id and req_id in self.processed_req_ids:
                    self.append_log(seq, {"type": "COMMIT_SKIPPED_DUP_REQ", "seq": seq, "txn": txn, "req_id": req_id}, "COMMIT_SKIPPED")
                    return {"type": "REPLY", "status": "DUPLICATE_IGNORED", "req_id": req_id, "node_id": self.node_id}

                self.committed[seq] = txn
                self.committed_meta[seq] = req_id
                self.append_log(seq, {"type": "COMMIT_SENT", "seq": seq, "txn": txn, "req_id": req_id}, "COMMIT_SENT")
                commit_payload = {"type": "COMMIT", "seq": seq, "txn": txn, "from": self.node_id}
                if req_id:
                    commit_payload["req_id"] = req_id
                for nid, (h, p) in self.peers.items():
                    try:
                        send_json(h, p, commit_payload, timeout=1.0)
                    except Exception:
                        pass

                self._log_commit_to_file(seq, txn, req_id)
                executed_now = self._try_execute()
                result = self.committed_results.get(seq, "FAILED")
                reply = {"type": "REPLY", "ballot": self.leader_ballot, "timestamp": timestamp, "client_id": client_id, "req_id": req_id, "result": result, "status": "COMMITTED", "node_id": self.node_id}
                if req_id:
                    self.last_replies_by_req[req_id] = reply
                    self.last_req_by_client[client_id] = req_id
                self.append_log(seq, {"type": "REPLY_SENT", "req_id": req_id, "client_id": client_id, "result": result}, "REPLY_SENT")
                if executed_now:
                    self._maybe_send_checkpoint()
                return reply
            else:
                ack = {"type": "REPLY", "status": "RECEIVED", "node_id": self.node_id}
                return ack

        
        # If we know the leader, forward the request.
        if self.leader_id is not None and self.leader_id in self.peers:
            leader_host, leader_port = self.peers[self.leader_id]
            fwd = {"type": "REQUEST", "client_id": client_id, "req_id": req_id, "txn": txn, "timestamp": timestamp}
            try:
                r = send_json(leader_host, leader_port, fwd, timeout=1.5)
            except Exception:
                r = None

            if r is not None:
                # Cache the leader's reply to handle duplicates.
                try:
                    if isinstance(r, dict):
                        rid = r.get("req_id")
                        cid = r.get("client_id")
                        if rid:
                            self.last_replies_by_req[rid] = r
                        if cid and rid:
                            self.last_req_by_client[cid] = rid
                except Exception:
                    pass
                # Pass the leader's reply back to the client.
                return r
            else:
                pass

        self.pending_requests.append(msg)
        self.append_log(0, {"type": "REQUEST_ENQUEUED", "client_id": client_id, "req_id": req_id}, "REQUEST_ENQUEUED")
        self._start_request_timer_if_needed()
        return {"type": "REPLY", "status": "QUEUED_NO_LEADER", "node_id": self.node_id, "req_id": req_id}

    def _try_execute(self):
        executed_any = False

        while True:
            nxt = self.last_executed_seq + 1

            if nxt not in self.committed:
                has_higher = any(s > nxt for s in self.committed.keys())
                if has_higher:
                    pass
                break

            txn = self.committed[nxt]

            req_id = None
            if isinstance(txn, dict):
                req_id = txn.get("req_id") or txn.get("_req_id") or None
            if req_id is None:
                req_id = self.committed_meta.get(nxt)
            if req_id is None:
                acc = self.accepted.get(nxt)
                if acc and len(acc) >= 3:
                    req_id = acc[2]

            # Skip execution if we've already processed this request ID.
            if req_id and req_id in self.processed_req_ids:
                self.committed_results[nxt] = "DUPLICATE_IGNORED"
                self.last_executed_seq = nxt
                self.append_log(nxt, {
                    "type": "EXECUTE_SKIPPED_DUP", "seq": nxt, "txn": txn, "req_id": req_id
                }, "EXEC_SKIPPED_DUP")

                # Make sure we have a reply cached for this request.
                if req_id and req_id not in self.last_replies_by_req:
                    self.last_replies_by_req[req_id] = {
                        "type": "REPLY", "status": self.committed_results[nxt], "req_id": req_id, "node_id": self.node_id
                    }
                executed_any = True
                continue

            # Execute the transaction.
            if isinstance(txn, dict) and txn.get("noop"):
                self.committed_results[nxt] = "SUCCESS"
            else:
                src = txn.get("src")
                dst = txn.get("dst")
                try:
                    amt = int(txn.get("amt", 0))
                except Exception:
                    try:
                        amt = int(float(txn.get("amt", 0)))
                    except Exception:
                        amt = 0

                # If an account doesn't exist, create it with an initial balance.
                if src not in self.db:
                    self.db[src] = self.initial_balance
                if dst not in self.db:
                    self.db[dst] = self.initial_balance

                # Log the state before the transaction for debugging.
                src_before = self.db.get(src, 0)
                dst_before = self.db.get(dst, 0)

                # Perform the transfer.
                if self.db.get(src, 0) >= amt:
                    self.db[src] -= amt
                    self.db[dst] += amt
                    self.committed_results[nxt] = "SUCCESS"
                else:
                    self.committed_results[nxt] = "FAILED"

            # Mark this request ID as processed.
            if req_id:
                self.processed_req_ids.add(req_id)
                if req_id not in self.last_replies_by_req:
                    self.last_replies_by_req[req_id] = {
                        "type": "REPLY", "status": self.committed_results[nxt], "req_id": req_id, "node_id": self.node_id
                    }

            self.last_executed_seq = nxt
            self.append_log(nxt, {
                "type": "EXECUTE", "seq": nxt, "txn": txn, "result": self.committed_results[nxt], "req_id": req_id
            }, "EXECUTED")
            executed_any = True

        return executed_any

    def _maybe_send_checkpoint(self):
        if not self.is_leader:
            return
        try:
            with self._lock:
                if self.last_executed_seq - self.latest_checkpoint_seq >= self.checkpoint_period:
                    new_cp = int(self.last_executed_seq)
                    cp_db = {str(k).upper(): int(v) for k, v in self.db.items()}
                    payload = {"type": "CHECKPOINT", "seq": new_cp, "db": cp_db, "from": self.node_id}
                    self.append_log(0, {"type": "CHECKPOINT_SENT", "seq": new_cp}, "CHECKPOINT_SENT")
                    for nid, (h, p) in self.peers.items():
                        try:
                            send_json(h, p, payload, timeout=1.0)
                        except Exception:
                            pass
                    self.checkpoint_db = cp_db
                    self.latest_checkpoint_seq = new_cp
                    self._install_checkpoint(new_cp, cp_db)
        except Exception:
            pass

    def _install_checkpoint(self, seq: int, db_snapshot: Dict[str, int]):
        try:
            with self._lock:
                self.db = dict(db_snapshot)
                if seq > self.last_executed_seq:
                    self.last_executed_seq = seq
                new_logs = [le for le in self.log_entries if le.seq > seq and le.state != "CHECKPOINT_SENT"]
                self.log_entries = new_logs
                self.accepted = {s: v for s, v in self.accepted.items() if s > seq}
                self.committed = {s: v for s, v in self.committed.items() if s > seq}
                self.committed_meta = {s: v for s, v in self.committed_meta.items() if s > seq}
                self.committed_results = {s: v for s, v in self.committed_results.items() if s > seq}
        except Exception:
            pass

    def print_db(self) -> Dict[str, int]:
        return dict(self.db)

    def print_log(self) -> List[dict]:
        return [le.to_dict() for le in self.log_entries]

    def _log_commit_to_file(self, seq: int, txn: Dict[str, Any], req_id: Optional[str]):
        try:
            entry = {"seq": seq, "txn": txn, "req_id": req_id, "ts": now_ts(), "node": self.node_id}
            with open(self.commit_log_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
                try:
                    f.flush()
                    os.fsync(f.fileno())
                except Exception:
                    # If fsync fails, it's not critical; the node should keep running.
                    pass
        except Exception:
            pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, required=True, help="node id (matches config id)")
    parser.add_argument("--port", type=int, required=False, help="port (overrides config)")
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--config", type=str, default="config.json")
    parser.add_argument("--start-as-leader", action="store_true", help="try PREPARE at startup to become leader")
    parser.add_argument("--base-x", type=float, default=1.0, help="base x for request timer t = x*(node_index+1)")
    parser.add_argument("--tp", type=float, default=5.0, help="prepare suppression window (seconds)")
    parser.add_argument("--start-timers", action="store_true", help="start request timer t immediately on startup (demo convenience)")
    args = parser.parse_args()

    if args.port is None:
        with open(args.config) as f:
            cfg = json.load(f)
        node_info = next((n for n in cfg["nodes"] if int(n["id"]) == args.id), None)
        if not node_info:
            raise SystemExit(f"Node id {args.id} not in config")
        port = node_info["port"]
    else:
        port = args.port

    server = NodeServer(
        args.id,
        args.host,
        port,
        args.config,
        start_as_leader=args.start_as_leader,
        base_x=args.base_x,
        tp=args.tp,
        start_timers=args.start_timers,
    )
    server.start()