import ftplib
import logging
from paramiko import Transport, SFTPClient
from models import SiteConfig

logger = logging.getLogger(__name__)


class FTPConnector:
    @staticmethod
    def list_and_size(site):
        try:
            port = getattr(site, "port", 21)
            ftp = ftplib.FTP(timeout=30)
            ftp.connect(site.host, port)
            ftp.login(site.user, site.password)
            ftp.cwd(site.path)
            files = []
            sizes = {}
            try:
                for entry in ftp.mlsd():
                    name, facts = entry
                    if "type" in facts and facts["type"] == "file":
                        files.append(name)
                        sizes[name] = int(facts.get("size", 0))
                ftp.quit()
                return files, sizes
            except:
                pass
            files = ftp.nlst()
            for f in files:
                try:
                    size = ftp.size(f)
                    sizes[f] = size if size is not None else 0
                except:
                    sizes[f] = 0
            ftp.quit()
            return files, sizes
        except (ftplib.error_perm, ftplib.error_temp) as e:
            # 550 errors are often "no files found" - not critical
            error_msg = str(e)
            if "550" in error_msg and (
                "no files" in error_msg.lower() or "not found" in error_msg.lower()
            ):
                logger.info(
                    f"FTP directory empty or no matching files for {site.host}:{site.path}"
                )
            else:
                logger.error(f"FTP error for {site.host}: {e}")
            return [], {}
        except Exception as e:
            logger.error(f"FTP list_and_size failed for {site.host}: {e}")
            return [], {}

    @staticmethod
    def download(site, fname, local_path):
        try:
            port = getattr(site, "port", 21)
            ftp = ftplib.FTP(timeout=60)
            ftp.connect(site.host, port)
            ftp.login(site.user, site.password)
            ftp.cwd(site.path)
            with open(local_path, "wb") as f:
                ftp.retrbinary(f"RETR {fname}", f.write)
            ftp.quit()
            return True
        except Exception as e:
            logger.error(f"FTP download failed for {site.host}/{fname}: {e}")
            return False


class SFTPConnector:
    @staticmethod
    def list_and_size(site):
        if not site.host:
            logger.warning(f"SFTP site {site.name} has no host configured, skipping")
            return [], {}
        try:
            port = getattr(site, "port", 22)
            transport = Transport((site.host, port))
            transport.connect(username=site.user, password=site.password, timeout=30)
            sftp = SFTPClient.from_transport(transport)
            sftp.chdir(site.path)
            attrs = sftp.listdir_attr()
            files = [a.filename for a in attrs if a.st_size >= 0]
            sizes = {a.filename: a.st_size for a in attrs}
            sftp.close()
            transport.close()
            return files, sizes
        except Exception as e:
            logger.error(f"SFTP list_and_size failed for {site.host}: {e}")
            return [], {}

    @staticmethod
    def download(site, fname, local_path):
        if not site.host:
            logger.warning(f"SFTP site {site.name} has no host configured, skipping")
            return False
        try:
            port = getattr(site, "port", 22)
            transport = Transport((site.host, port))
            transport.connect(username=site.user, password=site.password, timeout=60)
            sftp = SFTPClient.from_transport(transport)
            remote_path = f"{site.path.rstrip('/')}/{fname}"
            sftp.get(remote_path, local_path)
            sftp.close()
            transport.close()
            return True
        except Exception as e:
            logger.error(f"SFTP download failed for {site.host}/{fname}: {e}")
            return False


class ConnectorFactory:
    @staticmethod
    def get(p):
        return FTPConnector if p == "ftp" else SFTPConnector
