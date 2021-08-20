import logging

# create logger
module_logger = logging.getLogger(__name__) 

def some_function():
    module_logger.debug('message from auxiliary module')