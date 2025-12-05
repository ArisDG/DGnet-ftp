import json, os
import logging
from typing import List, Dict, Callable
from models import SiteConfig, MissingFilesLog
from scanner import SiteScanner
from connectors import ConnectorFactory
from config import Config
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


class FTPSiteManager:
    def __init__(self):
        self.config = Config()
        self.sites: List[SiteConfig] = []
        self.scanner = SiteScanner()
        self._load_sites()

    def scan_all(
        self, days_back=1, progress_cb: Callable[[str], None] = None
    ) -> MissingFilesLog:
        log = MissingFilesLog()
        log.clear()
        for site in self.sites:
            if progress_cb:
                progress_cb(f"Scanning {site.name} [{site.network} {site.rate}]...")
            items = self.scanner.scan_site(site, days_back)
            log.add(site.name, items)
        if progress_cb:
            progress_cb("Scan complete")
        return log

    def auto_download_completed(self, log: MissingFilesLog, delay_minutes: int):
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(minutes=delay_minutes)
        items = []
        for site_items in log.log.values():
            for item in site_items:
                if item["status"] in [
                    "missing locally",
                    "size mismatch",
                ] and not item.get("is_current_utc"):
                    try:
                        # Parse dates and ensure they have timezone info
                        if " " in item["date"]:
                            d, t = item["date"].split()
                            file_dt = datetime.strptime(
                                f"{d} {t}", "%Y-%m-%d %H:%M"
                            ).replace(tzinfo=timezone.utc)
                        else:
                            file_dt = datetime.strptime(
                                item["date"], "%Y-%m-%d"
                            ).replace(tzinfo=timezone.utc)
                        if file_dt < cutoff:
                            items.append(item)
                    except Exception as e:
                        logger.warning(
                            f"Could not parse date '{item['date']}' for {item['file']}: {e}"
                        )
        if items:
            self.download_missing(items, lambda msg: None)

    def download_missing(self, items, progress_cb=None):
        total = len(items)
        for i, item in enumerate(items):
            if progress_cb:
                progress_cb(f"Downloading {item['file']} ({i+1}/{total})")
            conn = ConnectorFactory.get(item["site_obj"].protocol)
            success = conn.download(item["site_obj"], item["file"], item["local_path"])
            if success and os.path.exists(item["local_path"]):
                item["local_size"] = os.path.getsize(item["local_path"])
                item["status"] = "ok"
                item["local"] = "yes"
                item["size_ok"] = "yes"

    def add_site(self, **kw):
        self.sites.append(SiteConfig(**kw))
        self._save()

    def edit_site(self, i, **kw):
        for k, v in kw.items():
            setattr(self.sites[i], k, v)
        self._save()

    def delete_site(self, i):
        del self.sites[i]
        self._save()

    def _save(self):
        with open(self.config.sites_file, "w") as f:
            json.dump([s.to_dict() for s in self.sites], f, indent=2)

    def _load_sites(self):
        if os.path.exists(self.config.sites_file):
            try:
                with open(self.config.sites_file) as f:
                    data = json.load(f)
                    for d in data:
                        self.sites.append(SiteConfig.from_dict(d))
            except Exception as e:
                logger.error(f"Failed to load sites from {self.config.sites_file}: {e}")
                print(f"Load error: {e}")
