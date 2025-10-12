#!/usr/bin/env python3
# scripts/start_all_nodes.py
import json, subprocess, time, os, sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CFG = os.path.join(ROOT, "config.json")
if not os.path.exists(CFG):
    print("config.json not found at", CFG); sys.exit(1)

cfg = json.load(open(CFG))
nodes = cfg.get("nodes", [])

# choose tuning values here
BASE_X = 0.5   # x in seconds
TP = 3.0       # prepare suppression window seconds

# make logs dir
logs_dir = os.path.join(ROOT, "node_logs")
os.makedirs(logs_dir, exist_ok=True)

procs = {}
try:
    for n in nodes:
        nid = int(n["id"])
        host = n.get("host", "127.0.0.1")
        port = int(n["port"])
        cmd = [
            sys.executable,
            os.path.join(ROOT, "nodes", "node.py"),
            "--id", str(nid),
            "--port", str(port),
            "--base-x", str(BASE_X),
            "--tp", str(TP)
        ]
        logfile = open(os.path.join(logs_dir, f"node{nid}.log"), "a")
        print("Starting node", nid, "->", " ".join(cmd))
        p = subprocess.Popen(cmd, stdout=logfile, stderr=logfile, cwd=ROOT)
        procs[nid] = (p, logfile)
        time.sleep(0.08)
    print("All nodes started. Logs in:", logs_dir)
    print("Press Ctrl-C here to stop the starter script (nodes remain running).")
    while True:
        
        time.sleep(1.0)
except KeyboardInterrupt:
    print("Stopping started nodes...")
finally:
    for nid, (p, logfile) in procs.items():
        try:
            p.terminate()
        except Exception:
            pass
        try:
            logfile.close()
        except Exception:
            pass
