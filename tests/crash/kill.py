import logging
import time

import psutil

TIMEOUT = 30
FILTERS = ["postgres", "vectors"]
logging.getLogger().setLevel(logging.INFO)


def process_filter(p: psutil.Process) -> bool:
    cmdline = "".join(p.cmdline())
    for case in FILTERS:
        if case not in cmdline:
            return False
    return True


if __name__ == "__main__":
    # Send kill signal to vectors process
    endpoint = time.monotonic() + TIMEOUT
    last_pids = set()
    while True:
        if time.monotonic() > endpoint:
            raise TimeoutError(f"Background worker not found in {TIMEOUT}s")
        procs = [p for p in psutil.process_iter() if process_filter(p)]
        last_pids = set(p.pid for p in procs)
        if len(procs) > 0:
            logging.info(f"Kill signal sent to {last_pids}")
            for p in procs:
                p.kill()
            break
        time.sleep(1)

    # Wait until process is not exist or recreated
    endpoint = time.monotonic() + TIMEOUT
    while True:
        if time.monotonic() > endpoint:
            raise TimeoutError(f"Background worker not killed in {TIMEOUT}s")
        procs = [p for p in psutil.process_iter() if process_filter(p)]
        pids = set(p.pid for p in procs)
        if len(pids & last_pids) == 0:
            logging.info(f"Background worker killed {last_pids}, now {pids}")
            break

    # Wait until process is recreated
    endpoint = time.monotonic() + TIMEOUT
    while True:
        if time.monotonic() > endpoint:
            raise TimeoutError(f"Background worker not recreated in {TIMEOUT}s")
        procs = [p for p in psutil.process_iter() if process_filter(p)]
        pids = set(p.pid for p in procs)
        if len(procs) == 1:
            logging.info(f"Background worker recreated {pids}")
            break
        time.sleep(1)
