# This contains functions for time management
import datetime


def get_now_utc_timestamp() -> float:
    return datetime.datetime.utcnow().timestamp()  # Get the current timestamp in seconds since the


def timestamp_to_datetime_isoformat(timestamp: float) -> str:
    return datetime.datetime.fromtimestamp(timestamp).isoformat()


def get_timestamp_from_date_and_time(year: int, month: int, day: int, hh: int = 0, mm: int = 0, ss: int = 0) -> float:
    return datetime.datetime(year, month, day, hh, mm, ss).timestamp()


def get_remaining_time_estimate(start_timestamp: float, total_assessed_docs: int, total_files: int) -> str:
    """
    This function return the elapsed time between a given starting timestamp and the current timestamp.
    The elapsed time is in a str format
    :param start_timestamp:
    :param total_assessed_docs:
    :param total_files:
    :return: y:m:d:h:m:s  # year:month:day:hour:minute:seconds
    """

    estimate_remaining_time = "Estimated remaining time: "
    if not total_assessed_docs > 0:
        estimate_remaining_time += "Undefined. No chunk was complete yet."
    else:

        elapsed_time_sec = get_now_utc_timestamp() - start_timestamp

        average_download_speed = total_assessed_docs / elapsed_time_sec  # number of downloaded files per seconds
        remaining_time_sec = (total_files - total_assessed_docs) / average_download_speed  # in seconds
        estimate_remaining_time += format_remaining_time(time_seconds=remaining_time_sec)

    info_color = '\033[94m'  # blue
    reset_color = '\033[0m'  # default `white`
    return f"{info_color}{estimate_remaining_time}{reset_color} "


def format_remaining_time(time_seconds: float, next_unite: str = "Y", result: str = "") -> str:
    unites = {
        "s": 1,
        "m": 60,
        "h": 60 * 60,
        "D": 24 * 60 * 60,
        "M": 30 * 24 * 60 * 60,
        "Y": 12 * 30 * 24 * 60 * 60,
    }
    current_unit = next_unite

    unite = unites[current_unit]
    tm = time_seconds / unite

    if int(tm) >= 1:
        tm = int(tm)
        result += f"{tm}{current_unit}:" if tm >= 10 else f"0{tm}{current_unit}:"
        time_seconds -= tm * unite

    if current_unit != "s":
        unite_keys_list = list(unites.keys())
        current_unite_index = unite_keys_list.index(current_unit)
        next_unite = unite_keys_list[current_unite_index - 1]

        return format_remaining_time(time_seconds=time_seconds, next_unite=next_unite, result=result)

    return result.removesuffix(":") if result else "Undefined"
