import json
from logging import config, getLogger
from pathlib import Path

_IS_LOG_INIT: bool = False

def _setup_logging() -> None:
  global _IS_LOG_INIT
  if _IS_LOG_INIT:
    return

  cur_path = Path(__file__).resolve()
  root_path = cur_path.parent.parent
  log_dir = root_path / "logs"
  log_dir.mkdir(parents=True, exist_ok=True)

  config_file = root_path / "config" / "logger.json"
  if not config_file.exists():
    raise FileNotFoundError(f"Unable to find logger configuration file: {config_file}")

  with open(config_file, "r", encoding="utf-8") as f_in:
    config_settings = json.load(f_in)

  config_settings["handlers"]["user_file"]["filename"] = str(log_dir / "gridometer.log")

  print("logger.py being used:", cur_path)
  print("resolved log file:", config_settings["handlers"]["user_file"]["filename"])

  config.dictConfig(config_settings)

  logger = getLogger(__name__)
  logger.debug("Loaded configuration for logger")
  _IS_LOG_INIT = True

def _call_logger(name: str):
  if not _IS_LOG_INIT:
    _setup_logging()
  return getLogger(name)