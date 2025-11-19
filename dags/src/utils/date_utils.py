import logging
import pendulum

def get_list_of_dates(self, start_date, manual_end_date=None):
    """
    Gets the list of dates from start date (pulled from XCOM) to end date (current date).

    Args:
        **kwargs: XCOM start date for full or incremental load

    Returns:
        list[str]: List of dates in format YYYY-MM-DD
    """

    start_date = pendulum.parse(start_date)

    if manual_end_date != 'None':
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