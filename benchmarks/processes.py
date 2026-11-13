"""Spawned runtime adapters: startup barrier and bounded, owned-process cleanup."""
import logging
import multiprocessing as mp
import os
import time

from benchmarks.safety import BenchmarkError, database_url, verify_owned


def child(component, env, ready, start, stop):
    os.environ.update(env)
    # Drop stdout exports into the null sink; formatting/export cost still occurs in console mode.
    with open(os.devnull, "w") as sink:
        import contextlib
        with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
            try:
                url = database_url(os.environ)
                os.environ["DATABASE_URL"] = url
                logging.disable(logging.CRITICAL)
                from task_engine.db import engine
                from task_engine.telemetry import initialize
                if component == "worker":
                    from task_engine.worker import run_once
                else:
                    from task_engine.dispatcher import run_once
                initialize(component)
                with engine.connect() as connection:
                    verify_owned(connection, url)
                ready.send(True)
                while not start.wait(.05):
                    if stop.is_set():
                        return
                while not stop.is_set():
                    if not run_once():
                        stop.wait(1)  # Same idle polling cadence as runtime main().
            except BaseException:  # noqa: BLE001 - sanitized child process boundary
                # No driver exception text, connection URLs or arbitrary payloads escape.
                raise SystemExit(1) from None
            finally:
                # multiprocessing does not invoke normal atexit handlers on child exit.
                from task_engine import telemetry
                if telemetry._runtime is not None:
                    telemetry._runtime.shutdown()
                if "engine" in locals():
                    engine.dispose()
                ready.close()


class Processes:
    def __init__(self, workers, dispatchers, env, startup_timeout=30):
        self.context = mp.get_context("spawn")
        self.start = self.context.Event()
        self.stop = self.context.Event()
        self.processes = []
        self.receivers = []
        self.components = ["worker"] * workers + ["dispatcher"] * dispatchers
        self.env = env
        self.startup_timeout = startup_timeout

    def __enter__(self):
        deadline = time.perf_counter() + self.startup_timeout
        try:
            for component in self.components:
                receive, send = self.context.Pipe(duplex=False)
                self.receivers.append(receive)
                process = self.context.Process(
                    target=child, args=(component, self.env, send, self.start, self.stop))
                try:
                    process.start()
                finally:
                    send.close()
                self.processes.append(process)
            for receive in self.receivers:
                while not receive.poll(.02):
                    self.check()
                    if time.perf_counter() >= deadline:
                        raise BenchmarkError("process_startup_timeout")
                if receive.recv() is not True:
                    raise BenchmarkError("process_startup_failed")
            self.check()
            return self
        except BaseException:
            self.close()
            raise

    def check(self):
        if any(p.exitcode is not None for p in self.processes):
            raise BenchmarkError("owned_process_exited_early")

    def close(self):
        self.stop.set()
        deadline = time.perf_counter() + 5
        for process in self.processes:
            process.join(max(0, deadline - time.perf_counter()))
        for process in self.processes:
            if process.is_alive():
                process.terminate()
        for process in self.processes:
            process.join(2)
            if process.is_alive():
                process.kill()
                process.join(2)
            if process.is_alive():
                raise BenchmarkError("owned_process_cleanup_failed")
        for receive in self.receivers:
            receive.close()

    def __exit__(self, exc_type, *_):
        self.close()
        if exc_type is None and any(p.exitcode != 0 for p in self.processes):
            raise BenchmarkError("owned_process_failed_during_shutdown")
