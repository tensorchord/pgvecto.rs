from functools import reduce
import logging
import time
from typing import Callable

import psutil

TIMEOUT = 30
AND_FILTERS = ["postgres", "vectors"]
logging.getLogger().setLevel(logging.INFO)


def process_filter(p: psutil.Process):
    cmdline = "".join(p.cmdline())
    filter_func: Callable[[bool, str], bool] = lambda ans, e: ans and (e in cmdline)
    return reduce(filter_func, AND_FILTERS, True)


if __name__ == "__main__":
    # Send kill signal to vectors process
    timeout_start = time.time()
    while True:
        if time.time() > timeout_start + TIMEOUT:
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
    timeout_start = time.time()
    while True:
        if time.time() > timeout_start + TIMEOUT:
            raise TimeoutError(f"Background worker not killed in {TIMEOUT}s")
        procs = [p for p in psutil.process_iter() if process_filter(p)]
        pids = set(p.pid for p in procs)
        if len(procs) == 0 or pids != last_pids:
            logging.info(f"Background worker killed {last_pids}, now {pids}")
            break

    # Wait until process is recreated
    timeout_start = time.time()
    while True:
        if time.time() > timeout_start + TIMEOUT:
            raise TimeoutError(f"Background worker not recreated in {TIMEOUT}s")
        procs = [p for p in psutil.process_iter() if process_filter(p)]
        if len(procs) == 1:
            logging.info(f"Background worker recreated {[p.pid for p in procs]}")
            break
        time.sleep(1)
