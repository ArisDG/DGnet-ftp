import ftplib
from paramiko import Transport, SFTPClient
from models import SiteConfig

class FTPConnector:
    @staticmethod
    def list_and_size(site):
        try:
            ftp = ftplib.FTP(site.host, timeout=30)
            ftp.login(site.user, site.password)
            ftp.cwd(site.path)
            files = []; sizes = {}
            try:
                for entry in ftp.mlsd():
                    name, facts = entry
                    if 'type' in facts and facts['type'] == 'file':
                        files.append(name)
                        sizes[name] = int(facts.get('size', 0))
                ftp.quit()
                return files, sizes
            except: pass
            files = ftp.nlst()
            for f in files:
                try:
                    size = ftp.size(f)
                    sizes[f] = size if size is not None else 0
                except: sizes[f] = 0
            ftp.quit()
            return files, sizes
        except: return [], {}

    @staticmethod
    def download(site, fname, local_path):
        try:
            ftp = ftplib.FTP(site.host, timeout=60)
            ftp.login(site.user, site.password)
            ftp.cwd(site.path)
            with open(local_path, 'wb') as f:
                ftp.retrbinary(f'RETR {fname}', f.write)
            ftp.quit()
            return True
        except: return False

class SFTPConnector:
    @staticmethod
    def list_and_size(site):
        try:
            transport = Transport((site.host, 22))
            transport.connect(username=site.user, password=site.password)
            sftp = SFTPClient.from_transport(transport)
            sftp.chdir(site.path)
            attrs = sftp.listdir_attr()
            files = [a.filename for a in attrs if a.st_size >= 0]
            sizes = {a.filename: a.st_size for a in attrs}
            sftp.close()
            transport.close()
            return files, sizes
        except: return [], {}

    @staticmethod
    def download(site, fname, local_path):
        try:
            transport = Transport((site.host, 22))
            transport.connect(username=site.user, password=site.password)
            sftp = SFTPClient.from_transport(transport)
            remote_path = f"{site.path.rstrip('/')}/{fname}"
            sftp.get(remote_path, local_path)
            sftp.close()
            transport.close()
            return True
        except: return False

class ConnectorFactory:
    @staticmethod
    def get(p): return FTPConnector if p == 'ftp' else SFTPConnector
