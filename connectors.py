import ftplib
import logging
from paramiko import Transport, SFTPClient

logger = logging.getLogger(__name__)


class FTPConnector:
    @staticmethod
    def list_and_size(site):
        ftp = None
        try:
            port = int(getattr(site, "port", 21))
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
                return files, sizes
            except (ftplib.error_perm, ftplib.error_temp, ftplib.error_reply):
                # MLSD not supported, fall back to NLST
                pass
            files = ftp.nlst()
            for f in files:
                try:
                    size = ftp.size(f)
                    sizes[f] = size if size is not None else 0
                except (ftplib.error_perm, ftplib.error_temp):
                    sizes[f] = 0
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
        finally:
            if ftp:
                try:
                    ftp.quit()
                except Exception:
                    try:
                        ftp.close()
                    except Exception:
                        pass

    @staticmethod
    def download(site, fname, local_path):
        ftp = None
        try:
            port = int(getattr(site, "port", 21))
            ftp = ftplib.FTP(timeout=60)
            ftp.connect(site.host, port)
            ftp.login(site.user, site.password)
            ftp.cwd(site.path)
            with open(local_path, "wb") as f:
                ftp.retrbinary(f"RETR {fname}", f.write)
            return True
        except Exception as e:
            logger.error(f"FTP download failed for {site.host}/{fname}: {e}")
            return False
        finally:
            if ftp:
                try:
                    ftp.quit()
                except Exception:
                    try:
                        ftp.close()
                    except Exception:
                        pass


class SFTPConnector:
    @staticmethod
    def list_and_size(site):
        if not site.host:
            logger.warning(f"SFTP site {site.name} has no host configured, skipping")
            return [], {}
        transport = None
        sftp = None
        try:
            port = int(getattr(site, "port", 22))
            transport = Transport((site.host, port))
            transport.connect(username=site.user, password=site.password, timeout=30)
            sftp = SFTPClient.from_transport(transport)
            sftp.chdir(site.path)
            attrs = sftp.listdir_attr()
            files = [a.filename for a in attrs if a.st_size >= 0]
            sizes = {a.filename: a.st_size for a in attrs}
            return files, sizes
        except Exception as e:
            logger.error(f"SFTP list_and_size failed for {site.host}: {e}")
            return [], {}
        finally:
            if sftp:
                try:
                    sftp.close()
                except Exception:
                    pass
            if transport:
                try:
                    transport.close()
                except Exception:
                    pass

    @staticmethod
    def download(site, fname, local_path):
        if not site.host:
            logger.warning(f"SFTP site {site.name} has no host configured, skipping")
            return False
        transport = None
        sftp = None
        try:
            port = int(getattr(site, "port", 22))
            transport = Transport((site.host, port))
            transport.connect(username=site.user, password=site.password, timeout=60)
            sftp = SFTPClient.from_transport(transport)
            remote_path = f"{site.path.rstrip('/')}/{fname}"
            sftp.get(remote_path, local_path)
            return True
        except Exception as e:
            logger.error(f"SFTP download failed for {site.host}/{fname}: {e}")
            return False
        finally:
            if sftp:
                try:
                    sftp.close()
                except Exception:
                    pass
            if transport:
                try:
                    transport.close()
                except Exception:
                    pass


class ConnectorFactory:
    @staticmethod
    def get(p):
        return FTPConnector if p == "ftp" else SFTPConnector
