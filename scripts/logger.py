import json
import os

from logging import config, getLogger 
from pathlib import Path

_IS_LOG_INIT: bool = False

#can be called multiple times but only do the configuration once
#Usage in entry points
def _setup_logging() -> None:
  global _IS_LOG_INIT
  if _IS_LOG_INIT:
    return
  
  cur_path = Path(__file__).resolve()
  root_path = cur_path.parent.parent
  log_dir = root_path/"logs"
  log_dir.mkdir(parents=True, exist_ok=True)

  config_file = root_path/"logger.json"

  if not config_file.exists():
    raise FileNotFoundError("Unable to find logger configuration file.")
  with open(config_file) as f_in:
    config_settings = json.load(f_in)
  os.makedirs(log_dir, exist_ok=True)
  config.dictConfig(config_settings)

  logger = getLogger(__name__)
  logger.debug("Loaded configuration for logger")
  _IS_LOG_INIT = True

#Will automatically set up the logger if the logger has not existed
#Usage in modules
def _call_logger(name: str):
  if not _IS_LOG_INIT:
    _setup_logging()
  return getLogger(name)