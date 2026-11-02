#!/usr/bin/env python3
"""Run isolated test binaries; collect per-child Linux rusage without timing the compiler."""
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time

root = Path(__file__).resolve().parent.parent
os.chdir(root)
out = Path(sys.argv[1] if len(sys.argv) > 1 else 'data/benchmarks')
out.mkdir(parents=True, exist_ok=True)
go = '/workspace/.toolchains/go1.27.3/go/bin/go'
environment = dict(os.environ, GOPATH='/workspace/.cache/go', GOCACHE='/workspace/.cache/go-build', GOTOOLCHAIN='local')
for package in ['actor', 'wal']:
    subprocess.run([go, 'test', '-c', '-o', f'bin/{package}-bench.test', f'./internal/{package}'], env=environment, check=True)

temporary = root / 'data' / 'benchmark-tmp'
temporary.mkdir(parents=True, exist_ok=True)
environment['TMPDIR'] = str(temporary)
filesystem = subprocess.check_output(['stat', '-f', '-c', '%T', str(temporary)], text=True).strip()
runs = []


def execute(name, binary, pattern, iterations, extra=()):
    log = out / (name + '.txt')
    command = [binary, '-test.run=^$', '-test.bench=' + pattern, '-test.benchtime=' + iterations,
               '-test.cpu=4', '-test.benchmem', '-test.count=1', *extra]
    with log.open('w') as stream:
        begin = time.monotonic()
        child = subprocess.Popen(command, env=environment, stdout=stream, stderr=subprocess.STDOUT)
        _, status, usage = os.wait4(child.pid, 0)
        child.returncode = os.waitstatus_to_exitcode(status)
        elapsed = time.monotonic() - begin
    row = dict(name=name, command=command, exit_code=child.returncode, wall_seconds=elapsed,
               user_seconds=usage.ru_utime, system_seconds=usage.ru_stime,
               cpu_core_equivalents=(usage.ru_utime + usage.ru_stime)/elapsed,
               peak_rss_kib=usage.ru_maxrss, block_output_units=usage.ru_oublock,
               voluntary_context_switches=usage.ru_nvcsw, involuntary_context_switches=usage.ru_nivcsw,
               output=log.read_text())
    runs.append(row)
    print(name, 'exit=', child.returncode, 'wall=', round(elapsed, 3), flush=True)
    if child.returncode != 0:
        raise RuntimeError(log.read_text())


matrix = os.environ.get('BENCH_MATRIX') == '1'
try:
    if matrix:
        cases = [('uniform-'+m, m, '20000x', 256, 32) for m in
                 ['no-wal-batch64', 'wal-batch1', 'wal-batch64-0us', 'wal-batch8-100us', 'wal-batch64-100us', 'wal-batch64', 'wal-batch64-10ms']]
        cases += [(kind+'-'+m, m, '500x', players, workers) for kind,players,workers in
                  [('single-worker',256,1), ('hot-player',1,32), ('uniform-short',256,32)]
                  for m in ['wal-batch1', 'wal-batch64', 'wal-batch64-10ms']]
        high_cases = [('high-concurrency-'+m, m, '20000x', 256, 256) for m in
                      ['wal-batch8-100us', 'wal-batch64', 'wal-batch64-10ms']]
        cases = high_cases if os.environ.get('BENCH_MATRIX_HIGH_ONLY') == '1' else cases + high_cases
        for trial in range(3):
            for name,mode,iterations,players,workers in cases if trial%2==0 else list(reversed(cases)):
                environment.update(BENCH_PLAYERS=str(players), BENCH_WORKERS=str(workers))
                execute(f'{name}-{trial+1}', './bin/actor-bench.test', '^BenchmarkActorDurability$/^'+mode+'$', iterations)
                runs[-1].update(players=players, workers=workers, iterations=iterations, mode=mode)
        # No profile or microbenchmark duplication in the matrix report.
        sys.exit(0)
    modes = ['no-wal-batch64', 'wal-batch64', 'no-wal-batch1', 'wal-batch1']
    for trial in range(3):
        for mode in modes if trial % 2 == 0 else list(reversed(modes)):
            execute(f'{mode}-{trial+1}', './bin/actor-bench.test', '^BenchmarkActorDurability$/^' + mode + '$', '20000x')
    execute('wal-micro', './bin/wal-bench.test', '^BenchmarkWALAppend$', '2000x')
    for mode in ['no-wal-batch64', 'wal-batch64']:
        execute(mode+'-profile', './bin/actor-bench.test', '^BenchmarkActorDurability$/^'+mode+'$', '20000x',
                ('-test.cpuprofile='+str(out/(mode+'.cpu')), '-test.memprofile='+str(out/(mode+'.heap'))))
        for kind, sample in [('cpu', 'cpu'), ('heap', 'alloc_space')]:
            result = subprocess.run([go, 'tool', 'pprof', '-top', '-sample_index='+sample,
                                     './bin/actor-bench.test', str(out/(mode+'.'+kind))], env=environment,
                                    text=True, capture_output=True, check=True)
            (out/(mode+'-'+kind+'-top.txt')).write_text(result.stdout)
finally:
    metadata = dict(platform=platform.platform(), filesystem=filesystem, temp_directory=str(temporary), cpu_max=Path('/sys/fs/cgroup/cpu.max').read_text().strip(),
                    memory_max=Path('/sys/fs/cgroup/memory.max').read_text().strip(),
                    note='Unprofiled runs for latency. Linux per-process rusage includes setup, validation and cleanup. block_output_units are OS accounting, not physical device bytes. No HTTP or Mongo is timed.', runs=runs)
    (out/'summary.json').write_text(json.dumps(metadata, indent=2)+'\n')
