import logging
import modA.other as other

# create and configure main logger
logger = logging.getLogger(__name__)

def myfunction():
    logger.debug('message from main module')

def otherfunction():
    # This will give me logs from other.py
    other.some_function()