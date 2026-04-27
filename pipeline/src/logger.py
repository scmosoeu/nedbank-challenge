import logging


def get_console_handler() -> logging.StreamHandler:
    """
    Get a console handler for displaying messages
    on the console

    Returns
    -------
    A stream handler to display messages on the 
    console
    """

    formatter = logging.Formatter(
        fmt='%(levelname)s: %(asctime)s - %(message)s', 
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)

    return ch


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
    
    ch = get_console_handler()
    logger.addHandler(ch)

    return logger