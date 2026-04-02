import re
import requests
import time

from datetime import datetime
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from logger import _call_logger, _setup_logging

#Internal
_BASE_URL = "https://reports-public.ieso.ca/public/Demand/"
_FILENAME_RE = re.compile(
  r"PUB_Demand_(?P<year>\d{4})(?:_v(?P<version>\d+))?\.csv", 
  re.IGNORECASE
)

_CUR_PATH: Path = Path(__file__).resolve()
_ROOT_PATH: Path = _CUR_PATH.parent.parent
_DATA_RAW_DIR: Path = _ROOT_PATH/"data"/"raw"
_DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
_DATA_DEMAND_DIR: Path = _DATA_RAW_DIR/"demand"
_DATA_DEMAND_DIR.mkdir(parents=True, exist_ok=True)

#Return full names of latest files
def _filter_lastest_by_year(html_text: str, fetch_range: int = 3) -> list[str]:
  current_year: int = datetime.now().year
  target_years: list[str] = [str(current_year - i) for i in range(fetch_range + 1)]
  #Example entry: 
  #"2026": (87, "PUB_Demand_2026_v87.csv")
  f_latest_versions: dict[str, tuple[int, str]] = {} 

  for match in _FILENAME_RE.finditer(html_text):
    f_name = match.group(0)
    f_year = match.group('year')
    if (f_year not in target_years): continue

    f_version = int(match.group('version')) if (match.group('version')) else 0

    if ((f_year not in f_latest_versions) or (f_version > f_latest_versions[f_year][0])):
      f_latest_versions[f_year] = (f_version, f_name)

  to_download_files: list[str] = [f_info[1] for f_info in f_latest_versions.values()]

  return to_download_files

#External
def fetch_demand() -> None:
  _setup_logging()
  log = _call_logger("gridometer_fetch_demand")
  _html_response = requests.get(_BASE_URL)
  _html_response.raise_for_status()
  _html_text = _html_response.text

  _FETCH_RANGE = 3
  _to_download_files: list[str] = _filter_lastest_by_year(_html_text, fetch_range=_FETCH_RANGE)
  #<> Log amount of downloadable files

  with requests.Session() as _session:
    #Retry/Backoff
    retry_tact = Retry(
      total=5,
      backoff_factor=2,
      status_forcelist=[429, 500, 502, 503, 504],
      allowed_methods=["GET"]
    )

    _adapter = HTTPAdapter(max_retries=retry_tact)
    _session.mount("https://", _adapter)
    _session.mount("http://", _adapter)
    
    _session.headers.update({"User-Agent": "IESO-Data-Bot"})

    for _f_name in _to_download_files:
      _download_url = _BASE_URL + _f_name
      _f_save_path = _DATA_DEMAND_DIR/_f_name
      try:
        with _session.get(_download_url, timeout=10, stream=True) as _download_response:
          _download_response.raise_for_status()
          with open(_f_save_path, "wb") as f:
            f.write(_download_response.content)
        log.info("Downloaded %s", _f_save_path.name)
      except requests.exceptions.RequestException as e:
        log.error("Failed to download %s: %s", _f_save_path, e)
        raise
      
      time.sleep(1)

if __name__ == "__main__":
  fetch_demand()