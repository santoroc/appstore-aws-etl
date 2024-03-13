from datetime import datetime, timedelta


def start_end_date(days_to_fetch: int, days_behind: int = 1) -> tuple:
    """
    Calculate the start and end dates based on the number of days to fetch and the number of days behind.

    Parameters:
        - days_to_fetch (int): The number of days to fetch.
        - days_behind (int): The number of days behind the current date. Default is 1.

    Returns:
        - tuple: A tuple containing the start date and end date in the format '%Y-%m-%d'.
    """

    FORMAT = '%Y-%m-%d'
    days_to_fetch = int(days_to_fetch)
    current_date = datetime.now().date()
    end_date = current_date - timedelta(days=days_behind)
    
    start_date = end_date - timedelta(days=days_to_fetch)
    return (
        start_date.strftime(FORMAT),
        end_date.strftime(FORMAT)
    )


def date_list_builder(start_date: str, end_date: str) -> list:
    """
    Builds a list of dates between the given start and end dates.

    Args:
        start_date (str): The start date in the format 'YYYY-MM-DD'.
        end_date (str): The end date in the format 'YYYY-MM-DD'.

    Returns:
        list: A list of dates between the start and end dates, including both dates.

    Example:
        >>> date_list_builder('2022-01-01', '2022-01-05')
        ['2022-01-01', '2022-01-02', '2022-01-03', '2022-01-04', '2022-01-05']
    """

    FORMAT = '%Y-%m-%d'
    start_date = datetime.strptime(start_date, FORMAT).date()
    end_date = datetime.strptime(end_date, FORMAT).date()

    date_list = []
    d = start_date
    while d <= end_date:
        date_list.append(d.strftime(FORMAT))
        d = d + timedelta(days=1)
    
    return date_list


def start_end_date_list(days_to_fetch: int, days_behind: int=1) -> list:
    """
    Calculate the start and end dates based on the number of days to fetch and the number of days behind, and build a list of dates between the start and end dates.

    Parameters:
        - days_to_fetch (int): The number of days to fetch.
        - days_behind (int): The number of days behind the current date. Default is 1.

    Returns:
        - list: A list of dates between the start and end dates, including both dates.
    """

    start_date, end_date = start_end_date(days_to_fetch, days_behind)
    return date_list_builder(start_date, end_date)