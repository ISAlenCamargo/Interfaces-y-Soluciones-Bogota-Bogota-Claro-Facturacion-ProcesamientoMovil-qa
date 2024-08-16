import logging
import os
 
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
LOG_FILE = "docs/logs/log.log"  # Ruta y nombre del archivo de registro
RUTA_LOG = "docs/logs/"

if not os.path.exists(RUTA_LOG):
        os.makedirs(RUTA_LOG)

logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


