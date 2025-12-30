from pathlib import Path
from typing import Any
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import logging



class Extractor:
    def __init__(self):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )
        self.logger = logging.getLogger()
        self.session = self._configure_session()
        

    def _configure_session(self) -> requests.Session:
        retry = Retry(
            total=3,
            status_forcelist=[403, 429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            backoff_factor=0.5,
            raise_on_status=False,
        )

        adapter = HTTPAdapter(max_retries=retry)

        session = requests.Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        return session

    def make_request(self, url: str, timeout: int = 10) -> Any | None:
        self.logger.info("Making request...")
        try:
            response = self.session.get(url, timeout=timeout)
            self.logger.info(f"Status code: {response.status_code} OK")
            return response.json() if response.status_code in (200, 201) else None

        except requests.exceptions.RequestException as e:
            self.logger.error(
                "Request failed",
                extra={"url": url, "error": str(e)},
                exc_info=True,
            )
            return None

    @staticmethod
    def save_json(data: Any | None, output_dir: Path, filename: str) -> Path | None:
        logger = logging.getLogger(__name__)
        
        output_dir = Path(output_dir)
        
        if data is None:
            logger.warning("No data in json to save. Returning None")
            return None

        output_dir.mkdir(parents=True, exist_ok=True)

        if not filename.endswith(".json"):
            filename = f"{filename}.json"

        filepath = output_dir / filename

        with filepath.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)

        logger.info(f"Extraction complete! Data saved on: {filepath}")
        return filepath
