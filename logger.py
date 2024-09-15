import logging

logger = logging.getLogger("PyPipelineWorker")
logger.setLevel(logging.DEBUG)

# Disable log message propagation to parent (root) loggers
logger.propagate = False

# Create console handler and set level to debug
ch = logging.StreamHandler()

# Create formatter with process name, thread name, and other details
formatter = logging.Formatter(
    '%(asctime)s - %(processName)s - %(threadName)s - %(levelname)s - %(message)s'
)

# Add formatter to the console handler
ch.setFormatter(formatter)

# Add console handler to logger
logger.addHandler(ch)
