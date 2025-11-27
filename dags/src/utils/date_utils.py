from datetime import datetime
import logging
import pendulum

def get_list_of_dates(start_date:str|datetime, manual_end_date:datetime = None):
    """
    Gets the list of dates from start date (depending on manual input, full/incremental load ts) to end date (current date).

    Args:
        start_date (str|datetime): Start date in format YYYY-MM-DD
        manual_end_date (str, optional): End date in format YYYY-MM-DD. Defaults to None.

    Returns:
        list[str]: List of dates in format YYYY-MM-DD
    """
    if isinstance(start_date, str):                         # works for version 1 (start_date is a string)
        start_date = pendulum.parse(start_date)
    elif isinstance(start_date, datetime):                  # works for version 2 (start_date is a datetime datetime)
        start_date = pendulum.instance(start_date)

    if manual_end_date not in ['None', None]:
        end_date = pendulum.parse(manual_end_date)
    else:
        end_date = pendulum.now().subtract(years=5)
    
    if end_date.format('YYYY-MM-DD') == start_date.format('YYYY-MM-DD'):
        dates_list = [end_date.format('YYYY-MM-DD')]
    else:
        interval = pendulum.interval(start_date, end_date)
        dates_list = [d.format('YYYY-MM-DD') for d in (interval.range('days'))]

    logging.info(f'Fetching data between {start_date} and {end_date}')
    logging.info(dates_list)
    return dates_list