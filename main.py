from gui import FTPSiteGUI
from manager import FTPSiteManager
if __name__ == "__main__":
    manager = FTPSiteManager()
    app = FTPSiteGUI(manager)
    app.run()
