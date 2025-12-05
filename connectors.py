import ftplib
import logging
import socket
import time
from functools import wraps
from paramiko import Transport, SFTPClient

logger = logging.getLogger(__name__)

# Timeout configuration
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 30
DOWNLOAD_TIMEOUT = 60

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds


def retry_on_network_error(max_retries=MAX_RETRIES):
    """Decorator to retry operations on transient network errors"""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (socket.timeout, socket.error, OSError, EOFError) as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        delay = RETRY_DELAY * (2**attempt)  # exponential backoff
                        logger.warning(
                            f"{func.__name__} attempt {attempt + 1} failed: {e}. Retrying in {delay}s..."
                        )
                        time.sleep(delay)
                    else:
                        logger.error(
                            f"{func.__name__} failed after {max_retries} attempts: {e}"
                        )
                except Exception as e:
                    # Non-retryable errors
                    logger.error(
                        f"{func.__name__} failed with non-retryable error: {e}"
                    )
                    raise
            raise last_exception

        return wrapper

    return decorator


class FTPConnector:
    @staticmethod
    @retry_on_network_error()
    def list_and_size(site):
        ftp = None
        try:
            port = site.port
            # Create socket with connect timeout, then use for FTP
            sock = socket.create_connection((site.host, port), timeout=CONNECT_TIMEOUT)
            ftp = ftplib.FTP()
            ftp.sock = sock
            ftp.af = sock.family
            ftp.file = ftp.sock.makefile("r", encoding=ftp.encoding)
            ftp.welcome = ftp.getresp()
            ftp.sock.settimeout(READ_TIMEOUT)
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
    @retry_on_network_error()
    def download(site, fname, local_path):
        ftp = None
        try:
            port = site.port
            # Create socket with connect timeout, then use for FTP
            sock = socket.create_connection((site.host, port), timeout=CONNECT_TIMEOUT)
            ftp = ftplib.FTP()
            ftp.sock = sock
            ftp.af = sock.family
            ftp.file = ftp.sock.makefile("r", encoding=ftp.encoding)
            ftp.welcome = ftp.getresp()
            ftp.sock.settimeout(DOWNLOAD_TIMEOUT)
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
    @retry_on_network_error()
    def list_and_size(site):
        if not site.host:
            logger.warning(f"SFTP site {site.name} has no host configured, skipping")
            return [], {}
        transport = None
        sftp = None
        try:
            port = site.port
            # Create socket with connect timeout
            sock = socket.create_connection((site.host, port), timeout=CONNECT_TIMEOUT)
            transport = Transport(sock)
            transport.connect(
                username=site.user, password=site.password, timeout=READ_TIMEOUT
            )
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
    @retry_on_network_error()
    def download(site, fname, local_path):
        if not site.host:
            logger.warning(f"SFTP site {site.name} has no host configured, skipping")
            return False
        transport = None
        sftp = None
        try:
            port = site.port
            # Create socket with connect timeout
            sock = socket.create_connection((site.host, port), timeout=CONNECT_TIMEOUT)
            transport = Transport(sock)
            transport.connect(
                username=site.user, password=site.password, timeout=DOWNLOAD_TIMEOUT
            )
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
