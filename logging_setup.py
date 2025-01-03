import logging

def setup_logging(level=logging.INFO):
    """
    Sets up the logging configuration for the entire application.
    """
    logging.basicConfig(
        level=level,
        format='[%(levelname)s] %(asctime)s %(name)s: %(message)s'
    )
    logging.info("Logging is configured.")
