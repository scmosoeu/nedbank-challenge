import logging

def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the given name.
    
    Args:
        name: The name of the logger.

    Returns:
        A logger with the given name.
    """
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )
    
    return logger