#!/usr/bin/env python3
"""
client.py - client that sends REQUESTs with timestamp and waits up to 10s for leader reply.
If leader doesn't reply within 10s the client broadcasts the request to all nodes.

Usage example:
python client.py --config ../config.json --client-id c1 --txn '{"src":"A","dst":"B","amt":4}' --leader-id 0
"""
import argparse
import json
import logging
import socket
import sys
import time
import uuid
from typing import Dict, List, Optional, Tuple

REPLY_BUFFER = 65536
DEFAULT_LEADER_TIMEOUT = 10.0  # wait this long for leader reply
DEFAULT_MAX_ATTEMPTS = 3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def load_config(path: str) -> Dict:
    with open(path, "r") as f:
        return json.load(f)


def send_json(host: str, port: int, payload: dict, timeout: float) -> Optional[dict]:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        s.sendall(json.dumps(payload).encode())
        data = s.recv(REPLY_BUFFER)
        if not data:
            return None
        return json.loads(data.decode())
    except Exception as e:
        logging.debug(f"send_json error to {host}:{port}: {e}")
        return None
    finally:
        try:
            s.close()
        except Exception:
            pass


def find_node_by_id(nodes: List[dict], nid: int) -> Optional[Tuple[str, int]]:
    for n in nodes:
        if int(n.get("id")) == int(nid):
            return (n.get("host"), int(n.get("port")))
    return None


def broadcast(nodes: List[dict], payload: dict, timeout: float) -> Dict[int, Optional[dict]]:
    replies = {}
    for n in nodes:
        nid = int(n.get("id"))
        host, port = n.get("host"), int(n.get("port"))
        r = send_json(host, port, payload, timeout)
        replies[nid] = r
    return replies


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default="../config.json", help="path to config.json")
    parser.add_argument("--leader-host", type=str, help="leader host (optional)")
    parser.add_argument("--leader-port", type=int, help="leader port (optional)")
    parser.add_argument("--leader-id", type=int, help="leader node id (optional, reads config)")
    parser.add_argument("--client-id", type=str, required=True)
    parser.add_argument("--req-id", type=str, help="optional request id")
    parser.add_argument("--txn", type=str, required=True, help='JSON string, e.g. \'{"src":"A","dst":"B","amt":4}\'')
    parser.add_argument("--timeout", type=float, default=DEFAULT_LEADER_TIMEOUT, help="how long to wait for leader reply before broadcasting")
    parser.add_argument("--max-attempts", type=int, default=DEFAULT_MAX_ATTEMPTS)
    parser.add_argument("--broadcast-on-no-reply", action="store_true", help="broadcast after leader no-reply")
    args = parser.parse_args()

    cfg = load_config(args.config)
    nodes = cfg.get("nodes", [])

    req_id = args.req_id or f"{args.client_id}-{uuid.uuid4().hex[:8]}"
    try:
        txn = json.loads(args.txn)
    except Exception:
        txn = {"raw": args.txn}

    timestamp = time.time()
    payload = {"type": "REQUEST", "client_id": args.client_id, "req_id": req_id, "txn": txn, "timestamp": timestamp}

    leader_host = args.leader_host
    leader_port = args.leader_port
    if args.leader_id is not None:
        node = find_node_by_id(nodes, args.leader_id)
        if node:
            leader_host, leader_port = node

    attempt = 0
    while attempt < args.max_attempts:
        attempt += 1
        logging.info(f"[attempt {attempt}] Sending request {req_id} to leader {leader_host}:{leader_port}")

        # Try sending to the assumed leader first and WAIT up to provided timeout (10s).
        resp = None
        if leader_host and leader_port:
            resp = send_json(leader_host, leader_port, payload, timeout=args.timeout)
            if resp is not None:
                logging.info(f"Received reply from leader: {json.dumps(resp)}")
                print(json.dumps({"result": "ok", "reply": resp}))
                return
            else:
                logging.warning("No reply from leader within timeout.")
        else:
            logging.warning("No leader specified; will broadcast to all nodes.")

        # Broadcast to all nodes if requested or if leader had no reply / no leader specified
        if args.broadcast_on_no_reply or leader_host is None or leader_port is None or resp is None:
            logging.info("Broadcasting request to all nodes...")
            # Use a shorter timeout for broadcast responses to avoid long waits
            replies = broadcast(nodes, payload, timeout=2.0)
            any_successful_reply = False
            good_replies = {}
            for nid, r in replies.items():
                if r is not None:
                    any_successful_reply = True
                    good_replies[nid] = r
                    logging.info(f"Node {nid} replied: {json.dumps(r)}")
            if any_successful_reply:
                print(json.dumps({"result": "ok", "broadcast_replies": good_replies}))
                return
            else:
                logging.warning("No nodes replied to broadcast.")

        backoff = min(2.0, 0.2 * (2 ** attempt))
        logging.debug(f"Sleeping backoff {backoff}s before next attempt")
        time.sleep(backoff)

    logging.error(f"No reply after {args.max_attempts} attempts.")
    print(json.dumps({"result": "error", "reason": "no_reply"}))
    sys.exit(1)


if __name__ == "__main__":
    main()
