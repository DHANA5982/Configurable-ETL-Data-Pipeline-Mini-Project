import os
import logging

def setup_logger(name='pipeline'):
    """Configures and returns a named logger"""

    os.makedirs('D:/GitHub/Big Data Engineer/ETL Mini Project/logs/', exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:  # Prevent duplicate handlers
        handler = logging.FileHandler('D:/GitHub/Big Data Engineer/ETL Mini Project/logs/Pipeline.log')
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
