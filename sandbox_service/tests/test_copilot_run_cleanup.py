import fcntl
import os
from pathlib import Path
import subprocess
import sys
import threading
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.compact_bench_executor import CompactBenchExecutor


@pytest.fixture
def executor(tmp_path):
    executor = object.__new__(CompactBenchExecutor)
    executor._copilot_run_root = tmp_path / 'runs'
    executor._copilot_lock_root = tmp_path / 'locks'
    executor._copilot_run_root.mkdir()
    executor._copilot_lock_root.mkdir()
    executor._debug_preserve_outputs = False
    executor._output_cleanup_lock = threading.Lock()
    executor._last_copilot_cleanup_monotonic = 0
    return executor


@pytest.mark.parametrize('error', [RuntimeError('failed before metadata'), subprocess.TimeoutExpired('benchmark', 1)])
def test_failed_run_is_removed_without_result_metadata(executor, error):
    run_dir = executor._copilot_run_root / 'run-7'
    def execute(**kwargs):
        run_dir.mkdir()
        (run_dir / 'partial-checkout').write_text('partial')
        raise error
    executor._execute_task = execute
    with pytest.raises(type(error)):
        executor.execute_task(batch_id='batch', task=SimpleNamespace(agent_name='copilot', run_id=7), timeout_per_task=1)
    assert not run_dir.exists()


def test_success_cleanup_and_debug_preservation(executor):
    run_dir = executor._copilot_run_root / 'run-7'
    def execute(**kwargs):
        run_dir.mkdir()
        return 'result'
    executor._execute_task = execute
    task = SimpleNamespace(agent_name='copilot', run_id=7)
    assert executor.execute_task(batch_id='batch', task=task, timeout_per_task=1) == 'result'
    assert not run_dir.exists()
    executor._debug_preserve_outputs = True
    executor.execute_task(batch_id='batch', task=task, timeout_per_task=1)
    assert run_dir.exists()


def test_sweep_skips_locked_recent_unrelated_and_symlink_dirs(executor, tmp_path):
    for name in ['run-old', 'run-active', 'run-recent', 'repo-cache']:
        directory = executor._copilot_run_root / name
        directory.mkdir()
        if name != 'run-recent':
            os.utime(directory, (1, 1))
    target = tmp_path / 'outside'
    target.mkdir()
    (executor._copilot_run_root / 'run-link').symlink_to(target, target_is_directory=True)
    with executor._copilot_run_lock_path('run-active').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        executor._maybe_cleanup_stale_copilot_dirs()
    assert not (executor._copilot_run_root / 'run-old').exists()
    for name in ['run-active', 'run-recent', 'repo-cache', 'run-link']:
        assert (executor._copilot_run_root / name).exists()
    assert target.exists()


def test_failed_delete_is_logged(executor, monkeypatch, caplog):
    def fail(path):
        raise PermissionError('denied')
    monkeypatch.setattr('app.compact_bench_executor.shutil.rmtree', fail)
    executor._remove_run_directory(executor._copilot_run_root / 'run-7')
    assert 'Could not remove Copilot run directory' in caplog.text


def test_command_timeout_stops_descendant(executor, tmp_path):
    marker = tmp_path / 'child-wrote'
    child = f'import time; from pathlib import Path; time.sleep(1); Path({str(marker)!r}).touch()'
    parent = f'import subprocess,sys,time; subprocess.Popen([sys.executable,"-c",{child!r}]); time.sleep(20)'
    with pytest.raises(subprocess.TimeoutExpired):
        executor._run_benchmark_command([sys.executable, '-c', parent], env=os.environ.copy(), timeout=0.2)
    # Waiting in a separate subprocess also gives escaped children time to write.
    subprocess.run([sys.executable, '-c', 'import time; time.sleep(1.1)'], check=True)
    assert not marker.exists()
