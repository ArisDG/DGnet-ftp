import logging
from gui import FTPSiteGUI
from manager import FTPSiteManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("dgnet-ftp.log"), logging.StreamHandler()],
)

if __name__ == "__main__":
    manager = FTPSiteManager()
    app = FTPSiteGUI(manager)
    app.run()
