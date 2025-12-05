import datetime, os
import logging
from datetime import timezone
from typing import List, Dict
from models import SiteConfig
from connectors import ConnectorFactory

logger = logging.getLogger(__name__)


class FilePatternGenerator:
    @staticmethod
    def generate(site: SiteConfig, days_back: int) -> List[Dict]:
        expected = []
        now = datetime.datetime.now(timezone.utc)
        for day_offset in range(days_back):
            base = now - datetime.timedelta(days=day_offset)
            if site.frequency == "daily":
                dt = base.replace(hour=0, minute=0, second=0, microsecond=0)
                fname = dt.strftime(site.pattern)
                expected.append(
                    {"dt": dt, "file": fname, "date": dt.strftime("%Y-%m-%d")}
                )
            else:
                for hour in range(24):
                    dt = base.replace(hour=hour, minute=0, second=0, microsecond=0)
                    pattern = (
                        site.pattern.replace("%H", chr(97 + hour))
                        if site.use_letter_hour
                        else site.pattern
                    )
                    fname = dt.strftime(pattern)
                    expected.append(
                        {"dt": dt, "file": fname, "date": dt.strftime("%Y-%m-%d %H:00")}
                    )
        return expected


class SiteScanner:
    def scan_site(self, site: SiteConfig, days_back: int) -> List[Dict]:
        expected = FilePatternGenerator.generate(site, days_back)
        connector = ConnectorFactory.get(site.protocol)
        remote_files, remote_sizes = connector.list_and_size(site)
        remote_set = set(remote_files)
        os.makedirs(site.output_dir, exist_ok=True)

        now_utc = datetime.datetime.now(timezone.utc)
        results = []
        for exp in expected:
            fname = exp["file"]
            local_path = os.path.join(site.output_dir, fname)
            local_exists = os.path.exists(local_path)
            local_size = os.path.getsize(local_path) if local_exists else 0
            remote_exists = fname in remote_set
            remote_size = remote_sizes.get(fname, 0)
            size_match = local_exists and remote_exists and local_size == remote_size
            is_future = exp["dt"] > now_utc

            is_current_utc = False
            if " " in exp["date"]:
                file_date, file_hour = exp["date"].split()
                current_date = now_utc.strftime("%Y-%m-%d")
                current_hour = now_utc.strftime("%H")
                is_current_utc = file_date == current_date and file_hour.startswith(
                    current_hour
                )

            status = (
                "scheduled"
                if is_future
                else (
                    "new"
                    if is_current_utc and remote_exists
                    else (
                        "missing remotely"
                        if not remote_exists
                        else (
                            "missing locally"
                            if not local_exists
                            else "size mismatch" if not size_match else "ok"
                        )
                    )
                )
            )

            results.append(
                {
                    "site": site.name,
                    "date": exp["date"],
                    "file": fname,
                    "site_obj": site,
                    "local": "yes" if local_exists else "no",
                    "remote": "yes" if remote_exists else "no",
                    "local_size": local_size,
                    "remote_size": remote_size,
                    "size_ok": "yes" if size_match else "no",
                    "status": status,
                    "future": is_future,
                    "is_current_utc": is_current_utc,
                    "local_path": local_path,
                }
            )
        return results
