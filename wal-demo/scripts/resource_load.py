#!/usr/bin/env python3
"""Real HTTP/Mongo load, isolated data, server-only /proc resource accounting."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time
import urllib.request

root = Path(__file__).resolve().parent.parent
os.chdir(root)
run = str(time.time_ns())
directory = root / 'data' / ('resource_' + run)
directory.mkdir(parents=True)
with socket.socket() as sock:
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
url = f'http://127.0.0.1:{port}'
environment = dict(os.environ, HTTP_ADDR=f'127.0.0.1:{port}', MONGO_DATABASE='resource_actor_'+run,
                   WAL_DIRECTORY=str(directory), APP_API_TOKEN='', ACTOR_SHARDS='4', ACTOR_FLUSH_INTERVAL='1s')
ticks = os.sysconf('SC_CLK_TCK')


def resources(pid):
    fields = Path(f'/proc/{pid}/stat').read_text().split(') ', 1)[1].split()
    status = dict(line.split(':', 1) for line in Path(f'/proc/{pid}/status').read_text().splitlines())
    return (int(fields[11])/ticks, int(fields[12])/ticks,
            int(status.get('VmHWM', '0 kB').split()[0]), int(status.get('VmRSS', '0 kB').split()[0]))


def get(path):
    with urllib.request.urlopen(url+path, timeout=5) as response:
        return json.load(response)


with (directory/'server.log').open('w') as log:
    server = subprocess.Popen(['./bin/actor-server'], env=environment, stdout=log, stderr=log)
    stop = threading.Event()
    samples = []
    try:
        for _ in range(300):
            if server.poll() is not None:
                raise RuntimeError('server exited: '+str(directory/'server.log'))
            try:
                if get('/healthz')['status'] == 'ok': break
            except OSError: time.sleep(.05)
        else: raise RuntimeError('startup timed out')

        def sample():
            while not stop.wait(.05):
                samples.append(resources(server.pid))

        thread = threading.Thread(target=sample)
        thread.start()
        before = resources(server.pid)
        begin = time.monotonic()
        workload = subprocess.run(['./bin/loadgen', '-url', url, '-players', '100', '-requests', '50000', '-concurrency', '32'],
                                  env=environment, text=True, capture_output=True)
        elapsed = time.monotonic()-begin
        after = resources(server.pid)
        stop.set(); thread.join()
        report = json.loads(workload.stdout)
        stats = get('/debug/stats')
        result = dict(workload=report, exit_code=workload.returncode, workload_wall_seconds=elapsed,
                      server_user_seconds=after[0]-before[0], server_system_seconds=after[1]-before[1],
                      server_cpu_core_equivalents=(after[0]+after[1]-before[0]-before[1])/elapsed,
                      server_peak_rss_kib=after[2], server_final_rss_kib=after[3],
                      sampled_peak_rss_kib=max([s[3] for s in samples]+[after[3]]),
                      stats_after_workload=stats, retained_directory=str(directory),
                      note='CPU delta covers create+rewards+balance reads, not startup/shutdown; RSS HWM includes startup. Server process only; excludes load generator, MongoDB and OS page cache. Same-host closed-loop workload.')
        output = Path(sys.argv[1] if len(sys.argv)>1 else directory/'result.json')
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, indent=2)+'\n')
        print(json.dumps({k:v for k,v in result.items() if k not in ('workload','stats_after_workload')},indent=2))
        if workload.returncode: raise RuntimeError('load failed; raw report retained')
    finally:
        stop.set()
        if server.poll() is None:
            server.terminate()
            try: server.wait(timeout=30)
            except subprocess.TimeoutExpired: server.kill(); server.wait()
