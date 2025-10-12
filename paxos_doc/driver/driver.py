#!/usr/bin/env python3
import argparse
import csv
import itertools
import json
import os
import re
import socket
import subprocess
import sys
import time
import uuid
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Tuple

# Some basic settings for the network and timeouts.
REQ_TIMEOUT = 2.0
REPLY_BUFFER = 65536
CLIENT_LEADER_TIMEOUT = 10.0
CLIENT_RETRY_TIMEOUT = 10.0
BROADCAST_AFTER_NO_REPLY = True
SET_WAIT_TIMEOUT = 10.0
MAJORITY = lambda n: (n // 2) + 1

# Used by the function that collects and logs commits.
COMMITS_LOGFILE = "commits.log"


def send_json(host: str, port: int, payload: dict, timeout: float = 2.0) -> Optional[dict]:
    """Sends a JSON message and tries to get a complete JSON reply back."""
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
                # Still waiting for the rest of the message.
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


def parse_transaction(s: str):
    """Parses a transaction string like '(A, C, 5)' into its parts."""
    raw = "" if s is None else str(s)
    if s is None:
        return None, None, 0, raw
    s2 = str(s).strip()
    if s2 == "":
        return None, None, 0, raw
    if (s2.startswith('"') and s2.endswith('"')) or (s2.startswith("'") and s2.endswith("'")):
        s2 = s2[1:-1].strip()
    if s2.upper() in ("LF", "LEADER FAIL", "LEADERFAIL"):
        return None, None, 0, s2
    if s2.startswith("(") and s2.endswith(")"):
        s2 = s2[1:-1].strip()
    parts = [p.strip() for p in s2.split(",") if p.strip() != ""]
    if len(parts) >= 3:
        try:
            return parts[0], parts[1], int(parts[2]), raw
        except Exception:
            return parts[0], parts[1], 0, raw
    m = re.search(r"([A-Za-z0-9_\-+.]+)\s*[, \s]\s*([A-Za-z0-9_\-+.]+)\s*[, \s]\s*(\d+)", s2)
    if m:
        try:
            return m.group(1), m.group(2), int(m.group(3)), raw
        except Exception:
            return m.group(1), m.group(2), 0, raw
    return None, None, 0, raw


def parse_nodes(s: str, nodes_count: int = 5) -> List[int]:
    """Parses a list of nodes like '[n1, n2]' into a list of node IDs."""
    if s is None:
        return []
    st = str(s).strip()
    if st == "":
        return []
    if (st.startswith('"') and st.endswith('"')) or (st.startswith("'") and st.endswith("'")):
        st = st[1:-1].strip()
    if st.startswith("[") and st.endswith("]"):
        st = st[1:-1].strip()
    parts = [p.strip() for p in re.split(r"[,;]+", st) if p.strip() != ""]
    ids: List[int] = []
    for p in parts:
        low = p.lower()
        m = re.match(r"^n(\d+)$", low)
        if m:
            try:
                ids.append(int(m.group(1)) - 1)
                continue
            except Exception:
                pass
        m2 = re.match(r"^node(\d+)$", low)
        if m2:
            try:
                ids.append(int(m2.group(1)) - 1)
                continue
            except Exception:
                pass
        try:
            num = int(low)
            if 1 <= num <= nodes_count:
                ids.append(num - 1)
            else:
                ids.append(num)
            continue
        except Exception:
            pass
        digits = re.findall(r"\d+", low)
        if digits:
            try:
                nval = int(digits[0])
                if 1 <= nval <= nodes_count:
                    ids.append(nval - 1)
                else:
                    ids.append(nval)
            except Exception:
                pass
    ids = sorted(set([i for i in ids if isinstance(i, int) and i >= 0]))
    return ids


def detect_delimiter(sample: str) -> str:
    try:
        sn = csv.Sniffer()
        return sn.sniff(sample).delimiter
    except Exception:
        first = sample.splitlines()[0] if sample else ""
        return "\t" if "\t" in first else ","


def load_csv_sets(csv_path: str, nodes_count: int = 5) -> Dict[int, List[dict]]:
    """Loads a CSV file and groups the rows into sets of transactions."""
    encodings = ["utf-8-sig", "utf-8", "latin-1"]
    for enc in encodings:
        try:
            with open(csv_path, "r", encoding=enc, newline="") as f:
                sample = f.read(8192)
                f.seek(0)
                delim = detect_delimiter(sample)
                reader = csv.reader(f, delimiter=delim)
                rows = list(reader)
                if not rows:
                    return {}
                header = [h.strip() for h in rows[0]]
                header_l = [h.lower() for h in header]

                def find_index(candidates):
                    for c in candidates:
                        if c in header_l:
                            return header_l.index(c)
                    return None

                idx_set = find_index(["set number", "setnumber", "set_number", "set", "set id", "setid"])
                idx_txn = find_index(["transactions", "transaction", "txn", "txns"])
                idx_live = find_index(["live nodes", "live_nodes", "live-nodes", "live", "nodes"])

                # Try to guess the column indexes if they weren't found.
                for i, h in enumerate(header_l):
                    if idx_set is None and "set" in h:
                        idx_set = i
                    if idx_txn is None and ("trans" in h or "txn" in h):
                        idx_txn = i
                    if idx_live is None and ("live" in h or "node" in h):
                        idx_live = i

                sets: Dict[int, List[dict]] = defaultdict(list)
                current_sid: Optional[int] = None

                for r_i, row in enumerate(rows[1:], start=2):
                    row = [c if c is not None else "" for c in row]
                    raw_set = row[idx_set].strip() if idx_set is not None and idx_set < len(row) else ""
                    raw_txn = row[idx_txn].strip() if idx_txn is not None and idx_txn < len(row) else ""
                    raw_live = row[idx_live].strip() if idx_live is not None and idx_live < len(row) else ""

                    # Figure out the set ID for this row.
                    if raw_set != "":
                        try:
                            sid = int(raw_set.strip())
                        except Exception:
                            m = re.search(r"(\d+)", raw_set)
                            sid = int(m.group(1)) if m else None
                        current_sid = sid if sid is not None else current_sid
                    else:
                        if current_sid is None:
                            current_sid = 1
                        sid = current_sid

                    if sid is None:
                        sid = 1
                        current_sid = sid

                    src, dst, amt, rawtx = parse_transaction(raw_txn)
                    live_ids = parse_nodes(raw_live, nodes_count)
                    entry = {
                        "src": src,
                        "dst": dst,
                        "amt": int(amt or 0),
                        "live_ids": live_ids,
                        "raw_txn": rawtx,
                        "raw_live": raw_live,
                        "row_number": r_i,
                    }
                    sets[sid].append(entry)
                return dict(sorted(sets.items()))
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("Failed to decode CSV with tried encodings")


class ClientManager:
    def __init__(self, nodes_map: Dict[int, Tuple[str, int]], default_timeout: float = CLIENT_LEADER_TIMEOUT):
        self.nodes_map = nodes_map
        self.default_timeout = default_timeout

    def _send_to_node(self, nid: int, payload: dict, timeout: float) -> Optional[dict]:
        host, port = self.nodes_map[nid]
        return send_json(host, port, payload, timeout=timeout)

    def send_txn(self, client_id: str, txn: dict, live_node_ids: List[int], leader_hint: Optional[int], timeout_leader: float = None) -> Tuple[Optional[dict], Optional[int]]:
        """Tries the leader hint first, then broadcasts to all live nodes."""
        timeout_leader = timeout_leader or self.default_timeout
        req_id = f"{client_id}-{uuid.uuid4().hex[:8]}"
        timestamp = time.time()
        payload = {"type": "REQUEST", "client_id": client_id, "req_id": req_id, "txn": txn, "timestamp": timestamp}

        if leader_hint is not None and leader_hint in self.nodes_map:
            r = self._send_to_node(leader_hint, payload, timeout=timeout_leader)
            if r is not None:
                return r, leader_hint

        if not live_node_ids:
            live_node_ids = list(self.nodes_map.keys())

        per_node_timeout = 2.0
        replies = {}
        for nid in live_node_ids:
            try:
                r = self._send_to_node(nid, payload, timeout=per_node_timeout)
            except Exception:
                r = None
            replies[nid] = r

        for nid, r in replies.items():
            if r is not None:
                return r, nid

        return None, None


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r") as f:
        return json.load(f)


class Driver:
    def __init__(self, config_path: str, csv_path: str, start_nodes: bool = False, node_script: str = "../nodes/node.py"):
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        csv_path = os.path.join(base_dir, csv_path) if not os.path.isabs(csv_path) else csv_path
        config_path = os.path.join(base_dir, config_path) if not os.path.isabs(config_path) else config_path

        self.config = load_config(config_path)
        self.initial_balance = int(self.config.get("initial_balance", 10))

        # A map of node IDs to their host and port.
        self.nodes = {int(n["id"]): (n["host"], int(n["port"])) for n in self.config.get("nodes", [])}
        self.node_count = len(self.nodes)

        # Get client IDs from the config file.
        cfg_clients = [str(c) for c in self.config.get("clients", [])]
        self.client_ids = [c.upper() for c in cfg_clients] if cfg_clients else [chr(ord("A") + i) for i in range(10)]

        # Load the transaction sets from the CSV file.
        self.csv_sets = load_csv_sets(csv_path, nodes_count=self.node_count)

        # Clean up the list of live nodes for each row.
        node_keys_sorted = sorted(self.nodes.keys())
        node_count = len(node_keys_sorted)
        for sid, entries in self.csv_sets.items():
            for e in entries:
                normalized: List[int] = []
                for lid in e.get("live_ids", []):
                    if isinstance(lid, int):
                        if 0 <= lid < node_count:
                            normalized.append(node_keys_sorted[lid])
                        elif lid in self.nodes:
                            normalized.append(lid)
                        elif 1 <= lid <= node_count:
                            normalized.append(node_keys_sorted[lid - 1])
                e["live_ids"] = sorted(set(normalized))

        # Make sure the account names in the CSV are valid.
        valid_clients = set(self.client_ids)
        for sid, entries in self.csv_sets.items():
            for e in entries:
                src = e.get("src")
                dst = e.get("dst")
                raw = (e.get("raw_txn") or "").strip()
                is_lf = isinstance(raw, str) and raw.upper() in ("LF", "LEADER FAIL", "LEADERFAIL")
                if not is_lf:
                    if src is None or dst is None:
                        raise SystemExit(f"Invalid CSV at row {e.get('row_number')}: missing src/dst.")
                    if str(src).upper() not in valid_clients or str(dst).upper() not in valid_clients:
                        raise SystemExit(
                            f"Invalid account in CSV row {e.get('row_number')}: src={src} dst={dst}. "
                            f"Allowed accounts: {sorted(list(valid_clients))}"
                        )
                    e["src"] = str(src).upper()
                    e["dst"] = str(dst).upper()
                else:
                    # Mark leader failure rows clearly.
                    e["src"] = None
                    e["dst"] = None
                    e["amt"] = 0

        # Figure out which nodes should be live for each set.
        default_live = sorted(self.nodes.keys()) if self.nodes else []
        self.set_live_map: Dict[int, List[int]] = {}
        for sid, entries in self.csv_sets.items():
            set_live: List[int] = []
            for e in entries:
                if e.get("live_ids"):
                    set_live = list(e.get("live_ids"))
                    break
            if not set_live:
                set_live = list(default_live)
            normalized_set_live = sorted(set(set_live))
            self.set_live_map[sid] = normalized_set_live
            for e in entries:
                e["live_ids"] = list(normalized_set_live)

        # Assume the initial leader is the node with the lowest ID.
        self.leader = min(self.nodes.keys()) if self.nodes else None
        self._known_ballot: Optional[Tuple[int, int]] = None

        self.started_nodes_processes: Dict[int, subprocess.Popen] = {}
        self._node_cmds: Dict[int, List[str]] = {}
        self._intentionally_stopped: set = set()
        self.start_nodes_flag = start_nodes
        self.node_script = os.path.join(os.path.dirname(__file__), node_script) if node_script else node_script

        self.client_cycle = itertools.cycle(self.client_ids)
        self.client_inflight: Dict[str, bool] = {cid: False for cid in self.client_ids}
        self.unacked_requests: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        self.node_snapshots: Dict[int, Dict[str, int]] = {}
        self._init_node_snapshots()

        self.logged_commits: set = set()
        try:
            open(COMMITS_LOGFILE, "a").close()
        except Exception:
            pass

        self.client_manager = ClientManager(self.nodes, default_timeout=CLIENT_LEADER_TIMEOUT)

    def _init_node_snapshots(self) -> None:
        default = {cid: self.initial_balance for cid in self.client_ids}
        for nid in sorted(self.nodes.keys()):
            h, p = self.nodes[nid]
            try:
                r = send_json(h, p, {"type": "ADMIN_PRINTDB"}, timeout=1.0)
            except Exception:
                r = None
            if r and isinstance(r.get("db"), dict):
                db = {}
                for k, v in r.get("db", {}).items():
                    db[str(k).upper()] = int(v)
                for cid in self.client_ids:
                    db.setdefault(cid, default.get(cid, 10))
                self.node_snapshots[nid] = db
            else:
                self.node_snapshots[nid] = dict(default)

    def update_node_snapshots(self, target_node_ids: Optional[List[int]] = None) -> None:
        if target_node_ids is None:
            target_node_ids = sorted(self.nodes.keys())
        for nid in sorted(target_node_ids):
            if nid not in self.nodes:
                continue
            h, p = self.nodes[nid]
            try:
                r = send_json(h, p, {"type": "ADMIN_PRINTDB"}, timeout=1.0)
            except Exception:
                r = None
            if r and isinstance(r.get("db"), dict):
                db = {}
                for k, v in r.get("db", {}).items():
                    db[str(k).upper()] = int(v)
                for cid in self.client_ids:
                    db.setdefault(cid, 10)
                self.node_snapshots[nid] = db

    def start_all_nodes(self) -> None:
        if not self.start_nodes_flag:
            return

        for nid, (host, port) in self.nodes.items():
            alive = False
            try:
                r = send_json(host, port, {"type": "PING"}, timeout=0.6)
                if r and r.get("type") == "PONG":
                    alive = True
            except Exception:
                alive = False

            if alive:
                try:
                    rr = send_json(host, port, {"type": "ADMIN_RESET"}, timeout=1.0)
                except Exception as e:
                    pass
                continue

            cmd = [sys.executable, self.node_script, "--id", str(nid), "--port", str(port)]
            p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.started_nodes_processes[nid] = p
            self._node_cmds[nid] = cmd
            time.sleep(0.12)

        self.update_node_snapshots(target_node_ids=list(self.nodes.keys()))

    def stop_all_started_nodes(self) -> None:
        if not self.started_nodes_processes:
            return
        for nid, p in list(self.started_nodes_processes.items()):
            try:
                p.terminate()
            except Exception as e:
                pass
        self.started_nodes_processes.clear()
        self._intentionally_stopped.clear()

    def fail_leader(self) -> None:
        """Kills the current leader if the driver started it."""
        if self.leader in self.started_nodes_processes:
            p = self.started_nodes_processes[self.leader]
            try:
                p.terminate()
                del self.started_nodes_processes[self.leader]
                self._intentionally_stopped.add(self.leader)
            except Exception as e:
                pass
        else:
            print(
                "\nDriver was not used to start nodes. Please kill the leader node process manually\n"
                f"(node id {self.leader}, host {self.nodes.get(self.leader, (None,None))[0]}, port {self.nodes.get(self.leader, (None,None))[1]}),\n"
                "then press Enter to continue. If you cannot, the driver will still mark it stopped.\n"
            )
            input("Press Enter after you have killed the process (or press Enter to force-advance):\n")
            self._intentionally_stopped.add(self.leader)

        old = self.leader
        self.leader = None
        self._known_ballot = None

    def stop_node(self, nid: int) -> None:
        """Stops a specific node if the driver started it."""
        if nid in self.started_nodes_processes:
            p = self.started_nodes_processes.get(nid)
            if p:
                try:
                    p.terminate()
                except Exception as e:
                    pass
            self.started_nodes_processes.pop(nid, None)
            self._intentionally_stopped.add(nid)
        else:
            self._intentionally_stopped.add(nid)

    def restart_node(self, nid: int, wait_for_boot: float = 0.4) -> None:
        """Restarts a node that was intentionally stopped."""
        if nid not in self._intentionally_stopped:
            return
        cmd = self._node_cmds.get(nid)
        if cmd:
            try:
                p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                self.started_nodes_processes[nid] = p
                deadline = time.time() + max(1.0, wait_for_boot)
                boot_ok = False
                while time.time() < deadline:
                    h, port = self.nodes[nid]
                    try:
                        r = send_json(h, port, {"type": "PING"}, timeout=0.5)
                        if r and r.get("type") == "PONG":
                            boot_ok = True
                            break
                    except Exception:
                        pass
                    time.sleep(0.1)
                # Refresh the node's data snapshot.
                self.update_node_snapshots(target_node_ids=[nid])
                # Try to get the node caught up with its peers.
                try:
                    self.catchup_node(nid)
                except Exception as e:
                    pass
            except Exception as e:
                pass
        if nid in self.started_nodes_processes:
            self._intentionally_stopped.discard(nid)

    def catchup_node(self, nid: int) -> None:
        if nid not in self.nodes:
            return
        
        contactable = [x for x in sorted(self.nodes.keys())
                       if x not in self._intentionally_stopped and x != nid]
        if not contactable:
            return

        # Check the status of all reachable nodes.
        status_map: Dict[int, Optional[dict]] = {}
        for peer in contactable:
            h, p = self.nodes[peer]
            try:
                r = send_json(h, p, {"type": "ADMIN_STATUS"}, timeout=0.8)
            except Exception:
                r = None
            status_map[peer] = r

        # Find the most up-to-date peer.
        best_peer = None
        best_seq = -1
        best_status = None
        for peer, resp in status_map.items():
            if resp and isinstance(resp.get("next_seq"), (int, float)):
                try:
                    ns = int(resp.get("next_seq"))
                    if ns > best_seq:
                        best_seq = ns
                        best_peer = peer
                        best_status = resp
                except Exception:
                    pass

        if best_peer is None:
            return

        # Get leader info from the best peer.
        leader_ballot = None
        leader_id = None
        if best_status:
            leader_ballot = best_status.get("leader_ballot") or best_status.get("promised_ballot")
            leader_id = best_status.get("leader_id")

        # Get the database from the best peer.
        h_src, p_src = self.nodes[best_peer]
        try:
            rdb = send_json(h_src, p_src, {"type": "ADMIN_PRINTDB"}, timeout=1.0)
        except Exception:
            rdb = None

        if not (rdb and isinstance(rdb.get("db"), dict)):
            return

        # Clean up the database snapshot.
        db_snapshot = {}
        for k, v in rdb.get("db", {}).items():
            try:
                db_snapshot[str(k).upper()] = int(v)
            except Exception:
                try:
                    db_snapshot[str(k).upper()] = int(float(v))
                except Exception:
                    db_snapshot[str(k).upper()] = 0

        last_executed_seq = rdb.get("last_executed_seq", best_seq - 1)

        # Get the commit log from the best peer.
        commits_list: List[Dict[str, Any]] = []
        committed_meta_map: Dict[int, Optional[str]] = {}
        try:
            rlog = send_json(h_src, p_src, {"type": "ADMIN_PRINTLOG"}, timeout=1.0)
        except Exception:
            rlog = None

        if rlog and isinstance(rlog.get("log"), list):
            for entry in rlog.get("log", []):
                try:
                    msg = entry.get("msg", {}) or {}
                    state = (entry.get("state") or "").upper()
                    
                    if isinstance(msg, dict) and msg.get("type", "").upper() == "COMMIT":
                        s = int(msg.get("seq", entry.get("seq", 0)))
                        txn = msg.get("txn", {})
                        reqid = msg.get("req_id") or txn.get("req_id") or txn.get("_req_id")
                        commits_list.append({"seq": s, "txn": txn, "req_id": reqid})
                        if reqid:
                            committed_meta_map[s] = reqid
                    elif "COMMIT" in state:
                        s = int(entry.get("seq", 0))
                        txn = msg.get("txn") if isinstance(msg, dict) else None
                        reqid = None
                        if isinstance(msg, dict):
                            reqid = msg.get("req_id") or (txn.get("req_id") if isinstance(txn, dict) else None)
                        if s:
                            commits_list.append({"seq": s, "txn": txn or {}, "req_id": reqid})
                            if reqid:
                                committed_meta_map[s] = reqid
                except Exception:
                    pass

        # Send a catch-up command to the restarted node.
        payload = {
            "type": "ADMIN_CATCHUP",
            "db": db_snapshot,
            "next_seq": int(best_seq),
            "last_executed_seq": int(last_executed_seq),
            "from": f"driver:peer{best_peer}",
            "commits": commits_list,
            "committed_meta": committed_meta_map,
            "leader_ballot": leader_ballot,
            "leader_id": leader_id,
        }

        h_tgt, p_tgt = self.nodes[nid]
        try:
            rc = send_json(h_tgt, p_tgt, payload, timeout=2.0)
            time.sleep(0.1)
            self.update_node_snapshots(target_node_ids=[nid])
        except Exception as e:
            pass

    def send_request_to_node(self, leader_id: int, client_id: str, req_id: str, txn: dict, timeout: float = None) -> Optional[dict]:
        if timeout is None:
            timeout = CLIENT_LEADER_TIMEOUT
        if leader_id not in self.nodes:
            return None
        h, p = self.nodes[leader_id]
        payload = {"type": "REQUEST", "client_id": client_id, "req_id": req_id, "txn": txn, "timestamp": time.time()}
        try:
            r = send_json(h, p, payload, timeout=timeout)
        except Exception:
            r = None

        if r:
            try:
                self._consider_leader_update_from_reply(r)
            except Exception:
                pass
            return r
        else:
            return None

    def broadcast_request(self, client_id: str, req_id: str, txn: dict, target_node_ids: Optional[List[int]] = None, per_node_timeout: float = 2.0) -> Dict[int, Optional[dict]]:
        if target_node_ids is None:
            target_node_ids = sorted(self.nodes.keys())
        target_nodes = [nid for nid in target_node_ids if nid in self.nodes and nid not in self._intentionally_stopped]
        payload = {"type": "REQUEST", "client_id": client_id, "req_id": req_id, "txn": txn, "timestamp": time.time()}
        replies: Dict[int, Optional[dict]] = {}
        for nid in target_nodes:
            h, p = self.nodes[nid]
            try:
                r = send_json(h, p, payload, timeout=per_node_timeout)
            except Exception:
                r = None
            replies[nid] = r
            if r:
                try:
                    self._consider_leader_update_from_reply(r)
                except Exception:
                    pass
        return replies

    def is_node_contactable(self, nid: int, timeout: float = 0.6) -> bool:
        """Checks if a node is running and reachable."""
        if nid in self._intentionally_stopped:
            return False
        if nid not in self.nodes:
            return False
        h, p = self.nodes[nid]
        try:
            r = send_json(h, p, {"type": "PING"}, timeout=timeout)
            return bool(r and r.get("type") == "PONG")
        except Exception:
            return False

    def wait_for_stability(self, timeout: float = 10.0, poll_interval: float = 0.15) -> bool:
        deadline = time.time() + timeout
        tried = set()
        while time.time() < deadline:
            leader_candidates = []
            if self.leader in self.nodes:
                leader_candidates.append(self.leader)
            leader_candidates.extend([nid for nid in sorted(self.nodes.keys()) if nid not in leader_candidates])
            for nid in leader_candidates:
                if nid in tried and nid != self.leader:
                    pass
                h, p = self.nodes[nid]
                try:
                    r = send_json(h, p, {"type": "ADMIN_STATUS"}, timeout=1.0)
                except Exception:
                    r = None
                if not r:
                    tried.add(nid)
                    continue
                try:
                    if r.get("is_leader"):
                        candidate = int(r.get("node_id", nid))
                        if self.is_node_contactable(candidate):
                            old = self.leader
                            self.leader = candidate
                    elif "leader_id" in r and isinstance(r.get("leader_id"), int):
                        reported = int(r.get("leader_id"))
                        if self.is_node_contactable(reported):
                            old = self.leader
                            self.leader = reported
                except Exception:
                    pass

                if r.get("is_leader"):
                    pending = int(r.get("pending_requests", 0) or 0)
                    if pending == 0:
                        return True
                tried.add(nid)
            time.sleep(poll_interval)
        return False

    def query_all_nodes(self, payload: dict, target_node_ids: Optional[List[int]] = None) -> Dict[int, Optional[dict]]:
        replies: Dict[int, Optional[dict]] = {}
        if target_node_ids is None:
            target_node_ids = list(self.nodes.keys())
        for nid in sorted(self.nodes.keys()):
            if nid not in target_node_ids:
                replies[nid] = None
                continue
            h, p = self.nodes[nid]
            try:
                r = send_json(h, p, payload, timeout=2.0)
                replies[nid] = r
            except Exception:
                replies[nid] = None
        return replies

    def print_db(self) -> None:
        print("\n=== PrintDB across nodes ===")
        contactable = [nid for nid in sorted(self.nodes.keys()) if nid not in self._intentionally_stopped]
        try:
            self.update_node_snapshots(target_node_ids=contactable)
        except Exception:
            pass

        for nid in sorted(self.nodes.keys()):
            db = self.node_snapshots.get(nid)
            if nid in self._intentionally_stopped:
                if db is None:
                    print(f"Node {nid}: (no snapshot) (simulated down)")
                else:
                    print(f"Node {nid}: {json.dumps(db)} (simulated down)")
            else:
                if db is None:
                    print(f"Node {nid}: (no snapshot)")
                else:
                    print(f"Node {nid}: {json.dumps(db)}")
        print("=== End PrintDB ===\n")

    def print_log(self) -> None:
        print("\n=== PrintLog across nodes (last 200 entries each) ===")
        contactable = [nid for nid in sorted(self.nodes.keys()) if nid not in self._intentionally_stopped]
        replies = self.query_all_nodes({"type": "ADMIN_PRINTLOG"}, target_node_ids=contactable)
        for nid in sorted(self.nodes.keys()):
            if nid in self._intentionally_stopped:
                print(f"Node {nid}: (simulated down)")
                continue
            resp = replies.get(nid)
            if resp is None:
                print(f"Node {nid}: (no reply / down)")
            else:
                log = resp.get("log", [])
                print(f"Node {nid} log (entries: {len(log)}):")
                for e in log:
                    print(f"  {json.dumps(e)}")
        print("=== End PrintLog ===\n")

    def print_status(self, seq: int) -> None:
        payload = {"type": "ADMIN_PRINTLOG"}
        contactable = [nid for nid in sorted(self.nodes.keys()) if nid not in self._intentionally_stopped]
        replies = self.query_all_nodes(payload, target_node_ids=contactable)
        print(f"\n=== PrintStatus for seq {seq} ===")
        state_counts = {"E": 0, "C": 0, "A": 0, "X": 0}
        for nid in sorted(self.nodes.keys()):
            if nid in self._intentionally_stopped:
                print(f"Node {nid}: (simulated down)")
                continue
            resp = replies.get(nid)
            label = "X"
            if resp is None:
                label = "X (no reply)"
                state_counts["X"] += 1
            else:
                log = resp.get("log", [])
                states = [entry.get("state", "") for entry in log if entry.get("seq") == seq]
                if any("EXEC" in s.upper() for s in states):
                    label = "E"
                    state_counts["E"] += 1
                elif any("COMMIT" in s.upper() for s in states):
                    label = "C"
                    state_counts["C"] += 1
                elif any("ACCEPT" in s.upper() for s in states):
                    label = "A"
                    state_counts["A"] += 1
                else:
                    label = "X"
                    state_counts["X"] += 1
            print(f"Node {nid}: {label}")
        agg = "X"
        if state_counts["E"] >= MAJORITY(self.node_count):
            agg = "E"
        elif state_counts["C"] >= MAJORITY(self.node_count):
            agg = "C"
        elif state_counts["A"] >= MAJORITY(self.node_count):
            agg = "A"
        print(f"Aggregated (majority) status: {agg}")
        print("=== End PrintStatus ===\n")

    def print_view(self) -> None:
        """Prints all the NEW-VIEW messages that have been exchanged."""
        payload = {"type": "ADMIN_PRINTLOG"}
        replies = self.query_all_nodes(payload, target_node_ids=sorted(self.nodes.keys()))
        
        print("\n=== PrintView (NEW-VIEW messages found in logs) ===")
        found_any = False
        
        for nid in sorted(self.nodes.keys()):
            resp = replies.get(nid)
            if resp is None:
                status = "(simulated down)" if nid in self._intentionally_stopped else "(no reply / down)"
                print(f"Node {nid}: {status}")
                continue
            
            log = resp.get("log", [])
            newviews = []
            
            for e in log:
                try:
                    m = e.get("msg", {})
                    state = e.get("state", "")
                    
                    if isinstance(m, dict):
                        msg_type = m.get("type", "")
                        if msg_type in ("NEWVIEW", "NEW-VIEW"):
                            newviews.append(m)
                        elif "NEWVIEW" in state.upper() and msg_type == "NEWVIEW":
                            newviews.append(m)
                except Exception:
                    pass
            
            if newviews:
                found_any = True
                print(f"Node {nid} NEWVIEWs (count={len(newviews)}):")
                for nv in newviews:
                    print(f"  {json.dumps(nv)}")
        
        if not found_any:
            print("No NEW-VIEW messages found in node logs.")
        print("=== End PrintView ===\n")

    def collect_and_log_commits(self, set_id: Optional[int] = None) -> None:
        contactable = [nid for nid in sorted(self.nodes.keys()) if nid not in self._intentionally_stopped]
        if not contactable:
            return
        replies = self.query_all_nodes({"type": "ADMIN_PRINTLOG"}, target_node_ids=contactable)
        new_commits = []
        for nid in contactable:
            resp = replies.get(nid)
            if not resp:
                continue
            log = resp.get("log", [])
            for entry in log:
                try:
                    state = (entry.get("state") or "").upper()
                    msg = entry.get("msg") or {}
                    is_commit = False
                    if "COMMIT" in state:
                        is_commit = True
                    if isinstance(msg, dict) and msg.get("type", "").upper() == "COMMIT":
                        is_commit = True
                    if not is_commit:
                        continue
                    seq = int(entry.get("seq") or msg.get("seq") or 0)
                    txn = msg.get("txn") or {}
                    req_id = msg.get("req_id") or txn.get("_req_id") or txn.get("req_id") or None
                    dedupe_key = (seq, req_id if req_id is not None else json.dumps(txn, sort_keys=True))
                    if dedupe_key in self.logged_commits:
                        continue
                    commit_record = {
                        "collected_at": time.time(),
                        "set_id": set_id,
                        "node": nid,
                        "seq": seq,
                        "req_id": req_id,
                        "txn": txn,
                        "raw_entry": entry,
                    }
                    new_commits.append((dedupe_key, commit_record))
                except Exception:
                    pass

        if not new_commits:
            return

        try:
            with open(COMMITS_LOGFILE, "a", encoding="utf-8") as fh:
                for dedupe_key, record in new_commits:
                    fh.write(json.dumps(record) + "\n")
                    self.logged_commits.add(dedupe_key)
        except Exception:
            pass

    def _enqueue_unacked(self, client_id: str, req: Dict[str, Any]) -> None:
        """Saves a request that didn't get a final reply, so we can retry it later."""
        self.unacked_requests[client_id].append(req)

    def retry_unacked_for_client(self, client_id: str, live_node_ids: Optional[List[int]] = None, per_req_timeout: float = 3.0) -> None:
        """Tries to resend any requests that haven't been confirmed for a client."""
        pending = list(self.unacked_requests.get(client_id, []))
        if not pending:
            return
        remaining: List[Dict[str, Any]] = []
        for req in pending:
            txn = {"src": req["src"], "dst": req["dst"], "amt": req["amt"]}
            req_id = req["req_id"]
            r, nid, returned_req_id = self.send_client_request_with_retries(req["src"], txn, live_node_ids or sorted(self.nodes.keys()), total_timeout=min(3.0, CLIENT_RETRY_TIMEOUT), req_id=req_id)
            if r is not None:
                if isinstance(r, dict) and r.get("type") == "REPLY" and r.get("status") == "COMMITTED":
                    pass
                else:
                    remaining.append(req)
            else:
                remaining.append(req)
        self.unacked_requests[client_id] = remaining

    def send_client_request_with_retries(self, client_id: str, txn: dict, live_node_ids: List[int],
                                         total_timeout: float = None, per_try_timeout: float = 2.0,
                                         backoff: float = 0.25, req_id: Optional[str] = None
                                         ) -> Tuple[Optional[dict], Optional[int], str]:
        """Keeps trying to send a request until it gets a 'COMMITTED' reply or times out."""
        total_timeout = total_timeout if total_timeout is not None else CLIENT_RETRY_TIMEOUT
        if req_id is None:
            req_id = f"{client_id}-{uuid.uuid4().hex[:8]}"
        payload = {"type": "REQUEST", "client_id": client_id, "req_id": req_id, "txn": txn, "timestamp": time.time()}

        deadline = time.time() + float(total_timeout)
        attempt = 0

        while time.time() < deadline:
            attempt += 1
            leader_hint = self.leader if (self.leader in self.nodes and self.is_node_contactable(self.leader)) else None
            if leader_hint is not None:
                h, p = self.nodes[leader_hint]
                try:
                    r = send_json(h, p, payload, timeout=per_try_timeout)
                except Exception as e:
                    r = None
                if r is not None:
                    try:
                        self._consider_leader_update_from_reply(r)
                    except Exception:
                        pass

                    if isinstance(r, dict) and r.get("type") == "REPLY" and r.get("status") == "COMMITTED":
                        return r, leader_hint, req_id
            
            target_nodes = [nid for nid in (live_node_ids if live_node_ids else sorted(self.nodes.keys()))
                            if nid in self.nodes and nid not in self._intentionally_stopped]
            if not target_nodes:
                pass
            else:
                replies = {}
                for nid in target_nodes:
                    h, p = self.nodes[nid]
                    try:
                        r = send_json(h, p, payload, timeout=per_try_timeout)
                    except Exception:
                        r = None
                    replies[nid] = r

                for nid, r in replies.items():
                    if r is None:
                        continue
                    try:
                        self._consider_leader_update_from_reply(r)
                    except Exception:
                        pass

                    if isinstance(r, dict) and r.get("type") == "REPLY" and r.get("status") == "COMMITTED":
                        return r, nid, req_id

            to_sleep = backoff
            if time.time() + to_sleep >= deadline:
                break
            time.sleep(to_sleep)

        return None, None, req_id

    def send_client_request(self, client_id: str, txn: dict, live_node_ids: List[int]) -> Tuple[Optional[dict], Optional[int], str]:
        """A simple wrapper for sending a client request with retries."""
        return self.send_client_request_with_retries(client_id, txn, live_node_ids, total_timeout=CLIENT_RETRY_TIMEOUT)

    def run(self) -> None:
        try:
            if self.start_nodes_flag:
                self.start_all_nodes()

            for sid, entries in self.csv_sets.items():
                per_set_live_nodes = self.set_live_map.get(sid, sorted(self.nodes.keys()))

                if self.leader in self.nodes:
                    leader_info = f"node {self.leader} ({self.nodes[self.leader][0]}:{self.nodes[self.leader][1]})"
                else:
                    leader_info = f"node {self.leader} (unknown in config)"

                while True:
                    cmd = input(
                        f"\nReady to process Set {sid} ({len(entries)} txns).\n"
                        f"Leader (assumed): {leader_info}\n"
                        "Commands: [Enter=continue, fail, printDB, printLog, status <n>, view, quit]\n> "
                    ).strip()
                    if cmd == "":
                        break
                    if cmd.lower() in ("fail", "fail leader"):
                        self.fail_leader()
                    elif cmd.lower() == "printdb":
                        self.print_db()
                    elif cmd.lower() == "printlog":
                        self.print_log()
                    elif cmd.lower().startswith("status"):
                        parts = cmd.split()
                        if len(parts) == 2 and parts[1].isdigit():
                            self.print_status(int(parts[1]))
                        else:
                            print("Usage: status <seq>")
                    elif cmd.lower() == "view":
                        self.print_view()
                    elif cmd.lower() in ("quit", "exit"):
                        self.stop_all_started_nodes()
                        return
                    else:
                        print("Unknown command.")

                for nid in per_set_live_nodes:
                    if nid in self._intentionally_stopped:
                        self.restart_node(nid)

                for nid in sorted(self.nodes.keys()):
                    if nid not in per_set_live_nodes and nid not in self._intentionally_stopped:
                        self.stop_node(nid)

                contactable = [nid for nid in sorted(self.nodes.keys()) if nid not in self._intentionally_stopped]
                try:
                    self.update_node_snapshots(target_node_ids=contactable)
                except Exception:
                    pass

                print(f"Processing Set {sid} by sending {len(entries)} transactions (set live nodes: {per_set_live_nodes})...")
                set_deadline = time.time() + SET_WAIT_TIMEOUT

                for t in entries:
                    raw = (t.get("raw_txn") or "").strip()
                    is_lf = isinstance(raw, str) and raw.upper() in ("LF", "LEADER FAIL", "LEADERFAIL")
                    if is_lf:
                        self.fail_leader()
                        time.sleep(0.25)
                        continue

                    src = t.get("src")
                    dst = t.get("dst")
                    amt = int(t.get("amt") or 0)
                    if src is None or dst is None:
                        continue

                    txn = {"src": src, "dst": dst, "amt": amt}
                    client_id = src

                    try:
                        self.retry_unacked_for_client(client_id, live_node_ids=per_set_live_nodes)
                    except Exception:
                        pass

                    req_id = f"{client_id}-{uuid.uuid4().hex[:8]}"
                    r, nid, returned_req_id = self.send_client_request_with_retries(client_id, txn, per_set_live_nodes, total_timeout=CLIENT_RETRY_TIMEOUT, req_id=req_id)
                    if r is not None:
                        pass
                    else:
                        self._enqueue_unacked(client_id, {
                            "src": client_id,
                            "dst": dst,
                            "amt": amt,
                            "row_number": t.get('row_number'),
                            "req_id": req_id,
                            "set_id": sid,
                            "timestamp": time.time()
                        })

                    time.sleep(0.05)
                    _ = self.wait_for_stability(timeout=2.0)

                retry_start = time.time()
                while any(self.unacked_requests.values()) and time.time() < retry_start + SET_WAIT_TIMEOUT:
                    for cid in list(self.unacked_requests.keys()):
                        try:
                            self.retry_unacked_for_client(cid, live_node_ids=per_set_live_nodes)
                        except Exception:
                            pass
                    time.sleep(0.2)

                try:
                    self.collect_and_log_commits(set_id=sid)
                except Exception:
                    pass

                print("Waiting briefly for cluster to process transactions...")
                time.sleep(0.2)
                print(f"Finished processing Set {sid}. You may now run admin commands (printDB/printLog/status/view) or press Enter to continue to next set.")

            while True:
                cmd = input(
                    "\nAll transactions complete. Commands: [Enter=repeat message, printDB, printLog, status <n>, view, quit]\n> "
                ).strip()
                if cmd == "":
                    print("All transactions complete")
                elif cmd.lower() in ("quit", "exit"):
                    break
                elif cmd.lower() == "printdb":
                    self.print_db()
                elif cmd.lower() == "printlog":
                    self.print_log()
                elif cmd.lower().startswith("status"):
                    parts = cmd.split()
                    if len(parts) == 2 and parts[1].isdigit():
                        self.print_status(int(parts[1]))
                    else:
                        print("Usage: status <seq>")
                elif cmd.lower() == "view":
                    self.print_view()
                else:
                    print("Unknown command. Valid commands: printDB, printLog, status <n>, view, quit")

        finally:
            self.stop_all_started_nodes()

    def check_db_consistency(self) -> bool:
        contactable = [nid for nid in sorted(self.nodes.keys())
                       if nid not in self._intentionally_stopped]
        
        if len(contactable) < 2:
            return True
        
        dbs = {}
        for nid in contactable:
            h, p = self.nodes[nid]
            try:
                r = send_json(h, p, {"type": "ADMIN_PRINTDB"}, timeout=1.0)
                if r and isinstance(r.get("db"), dict):
                    dbs[nid] = {str(k).upper(): int(v) for k, v in r.get("db", {}).items()}
            except Exception:
                pass
        
        if len(dbs) < 2:
            return False
        
        reference_nid = min(dbs.keys())
        reference_db = dbs[reference_nid]
        
        consistent = True
        for nid, db in dbs.items():
            if db != reference_db:
                consistent = False
                all_keys = set(reference_db.keys()) | set(db.keys())
                for key in sorted(all_keys):
                    ref_val = reference_db.get(key, "MISSING")
                    node_val = db.get(key, "MISSING")
        
        return consistent

    def _consider_leader_update_from_reply(self, r: Optional[dict]) -> None:
        """Updates the driver's idea of the leader based on a node's reply."""
        if not r:
            return
        try:
            if isinstance(r.get("type"), str) and r.get("type").upper() == "REPLY":
                if r.get("status") in ("COMMITTED", "COMMIT") or isinstance(r.get("node_id"), int):
                    nid = r.get("node_id")
                    if isinstance(nid, int) and nid in self.nodes and self.is_node_contactable(nid):
                        old = self.leader
                        self.leader = nid
                        return

            if isinstance(r.get("is_leader"), bool) and r.get("is_leader"):
                candidate = r.get("node_id")
                if isinstance(candidate, int) and candidate in self.nodes and self.is_node_contactable(candidate):
                    old = self.leader
                    self.leader = candidate
                    return

            b = r.get("ballot") or r.get("promised_ballot") or r.get("leader_ballot") or None
            if isinstance(b, (list, tuple)) and len(b) >= 2:
                try:
                    btuple = (int(b[0]), int(b[1]))
                except Exception:
                    return
                if self._known_ballot is None or btuple > tuple(self._known_ballot):
                    candidate = int(b[1])
                    if candidate in self.nodes and self.is_node_contactable(candidate):
                        self._known_ballot = btuple
                        old = self.leader
                        self.leader = candidate
        except Exception:
            return

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", type=str, required=True, help="path to input CSV (grouped by set_id)")
    parser.add_argument("--config", type=str, default="config.json", help="path to config.json")
    parser.add_argument("--start-nodes", action="store_true", help="let driver start node processes (optional)")
    parser.add_argument("--node-script", type=str, default="../nodes/node.py", help="path to node script")
    args = parser.parse_args()

    csv_path = args.csv if os.path.isabs(args.csv) else args.csv
    config_path = args.config if os.path.isabs(args.config) else args.config

    driver = Driver(config_path=config_path, csv_path=csv_path, start_nodes=args.start_nodes, node_script=args.node_script)
    driver.run()
