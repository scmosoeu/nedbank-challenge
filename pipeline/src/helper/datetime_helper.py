from datetime import datetime


def get_datetime_now(datetime_format: str) -> str:
    """
    Get the current date when the function is called

    Args:
        datetime_format: The format of how the datetime
            field will be recorded
    Returns:
        The date when the pipeline ran
    """
    
    datetime_now = datetime.now()

    return datetime.strftime(datetime_now, datetime_format)