import re
import requests
import time

from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from logger import _setup_logging, _call_logger

#Internal
_BASE_URL = "https://reports-public.ieso.ca/public/Adequacy3/"
_FILENAME_RE = re.compile(
  r"PUB_Adequacy3_(?P<timestamp>\d{8})(?:_v(?P<version>\d+))?\.xml", 
  re.IGNORECASE
)

_CUR_PATH: Path = Path(__file__).resolve()
_ROOT_PATH: Path = _CUR_PATH.parent.parent
_DATA_RAW_DIR: Path = _ROOT_PATH/"data"/"raw"
_DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
_DATA_ADEQUACY_DIR: Path = _DATA_RAW_DIR/"adequacy"
_DATA_ADEQUACY_DIR.mkdir(parents=True, exist_ok=True)

#Return full names of latest files
def _filter_lastest(html_text: str) -> list[str]:
  #Example entry: 
  #"2024010512": (2, "PUB_RealtimeTotals_2024010512_v2.csv")
  f_latest_versions: dict[str, tuple[int, str]] = {} 

  for match in _FILENAME_RE.finditer(html_text):
    f_name = match.group(0)
    f_timestamp = match.group('timestamp')
    f_version = int(match.group('version')) if (match.group('version')) else 0

    if ((f_timestamp not in f_latest_versions) or (f_version > f_latest_versions[f_timestamp][0])):
      f_latest_versions[f_timestamp] = (f_version, f_name)

  return [f_info[1] for f_info in f_latest_versions.values()]

#External
def fetch_adequacy() -> None:
  _setup_logging()
  log = _call_logger("gridometer_fetch_adequacy")

  _html_response = requests.get(_BASE_URL)
  _html_response.raise_for_status()
  _html_text: str = _html_response.text

  _to_download_files: list[str] = _filter_lastest(_html_text)
  log.info("Adequacy files selected: %s", _to_download_files)

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

    _xml_namespace: dict[str, str] = {"ieso":"http://www.ieso.ca/schema"}

    for _f_name_xml in _to_download_files:
      _download_url: str = _BASE_URL + _f_name_xml
      _f_save_path = _DATA_ADEQUACY_DIR/_f_name_xml
      try:
          with _session.get(_download_url, timeout=10, stream=True) as _download_response:
            _download_response.raise_for_status()

            with open(_f_save_path, "wb") as f:
              for chunk in _download_response.iter_content(chunk_size=8192): f.write(chunk)
              
          log.info("Downloaded %s", _f_save_path.name)
      except requests.exceptions.RequestException as e:
        log.error("Failed to download %s: %s", _f_name_xml, e)
        raise
      
      time.sleep(1)

if __name__ == "__main__":
  fetch_adequacy()