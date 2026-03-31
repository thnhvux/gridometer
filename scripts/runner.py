from pathlib import Path
import subprocess

from fetch_demand import fetch_demand
from fetch_adequacy import fetch_adequacy
from logger import _setup_logging, _call_logger

CUR_PATH = Path(__file__).resolve()
ROOT_PATH = CUR_PATH.parent.parent

def main():
    _setup_logging()
    log = _call_logger("gridometer_runner")

    log.info("Starting pipeline ...")
    fetch_demand()
    log.info("Finished fetching Ontario Demand")

    fetch_adequacy()
    log.info("Finished fetching Adequacy")

    exe = ROOT_PATH/"build"/"gridometer.exe"
    if not exe.exists(): raise FileNotFoundError(f"Exe file not found: {exe}")
    cfg_path = ROOT_PATH/"gridometer_config.json"

    result = subprocess.run([str(exe), str(cfg_path)], cwd=ROOT_PATH, check=True)
    log.info("Runner finished with return code %s", result.returncode)

if __name__ == "__main__":
    main()