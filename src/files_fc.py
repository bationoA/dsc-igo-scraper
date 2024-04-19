# This file contains functions related to file such as json, yaml,
import os
import datetime
import json
from json import JSONDecodeError

import yaml
from enum import Enum  # For defining enumeration class such LogLevel

from src.time_fc import get_now_utc_timestamp


def is_python_object_a_valid_json(obj: list | dict) -> bool:
    try:
        # Attempt to convert the Python object to a JSON string
        json.dumps(obj)
    except TypeError as e:
        print("Invalid JSON:", e)
        return False
    return True


def save_to_json(obj: list | dict, filepath: str) -> bool:
    save_status = True
    # Check object is in a valid json format
    if is_python_object_a_valid_json(obj=obj):
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(obj, f, ensure_ascii=False, indent=4)
        except BaseException as e:
            print(e.__str__())
            save_status = False
    else:
        save_status = False

    return save_status


def add_to_log(obj, log_file_name: str, logs_dir: str) -> bool:
    """
    :param obj: json list
    :param log_file_name: json file name
    :param logs_dir: directory to save the json file
    """
    log_file_path = os.path.join(logs_dir, log_file_name)

    file = []
    # If the file already exists, then load it
    if os.path.exists(log_file_path) and os.path.isfile(log_file_path):
        # file = load_json(log_file_path)

        # Read the JSON file
        with open(log_file_path, 'r') as file:
            json_data = file.read()

        if json_data.endswith(']'):
            try:
                data = json.loads(json_data)
                file = data
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON file: {e}. Resetting log file")
                file = []
        else:
            last_dict_index = json_data.rfind('}')  # This line finds the index of the last occurrence of the closing
            # curly brace "}" in the JSON data string. If no closing curly brace is found,
            # last_dict_index will be set to -1.
            if last_dict_index != -1:
                json_data = json_data[:last_dict_index + 1] + ']'  # This line creates a modified version of the JSON
                # data string by extracting the portion of the string from the beginning up to the index of the last
                # closing curly brace
                try:
                    data = json.loads(json_data)
                    file = data
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON file: {e}. . Resetting log file")
                    file = []
            else:
                print("Invalid JSON file format: Incomplete closing brackets. Resetting log file")
                file = []

    file.append(obj)

    return save_to_json(file, log_file_path)


def load_json(filepath: str) -> json:
    with open(filepath, 'r') as f:
        file = json.load(f)

    return file


def load_yaml(filepath: str) -> yaml:
    # Read YAML file
    with open(filepath, 'r', encoding='utf-8') as f:
        file = yaml.safe_load(f)

    return file


def save_yaml(obj: yaml, filepath: str) -> bool:
    save_status = True
    filepath += ".yaml" if not filepath.endswith(".yaml") else ""
    try:
        # Save modified data to YAML file
        with open(filepath, 'w') as file:
            yaml.safe_dump(obj, file)
    except BaseException as e:
        print(e.__str__())
        save_status = False

    return save_status


def init_logs():
    # Initialize logs directory
    if not os.path.exists("logs"):
        os.makedirs("logs")

    # Initialize logs file
    logs_file_path = os.path.join("logs", "log_events.json")
    if not os.path.exists(logs_file_path):
        logs_file = []
        save_to_json(obj=logs_file, filepath=logs_file_path)

    # Last session error file
    lst_path = os.path.join("logs", "lst_err.json")
    if not os.path.exists(lst_path):
        lst_file = {
            "session": {
                "id": None,
                "errors_number": 0
            }
        }
        save_to_json(obj=lst_file, filepath=lst_path)


init_logs()
CONFIG = load_yaml(filepath="config.yaml")
try:
    SESSION_ERRORS = load_json(os.path.join("logs", "lst_err.json"))
except JSONDecodeError:
    # If decode error during loading then recreate the file
    lst_err = {
        "session": {
            "id": 0,
            "errors_number": 0
        }
    }
    save_to_json(obj=lst_err, filepath=os.path.join("logs", "lst_err.json"))

    SESSION_ERRORS = load_json(os.path.join("logs", "lst_err.json"))


def update_lst_err():
    save_to_json(SESSION_ERRORS, filepath=os.path.join("logs", "lst_err.json"))


# ---- Logs Classes
class LogLevel(Enum):
    # enumeration class is defined using the Enum base class from the enum module.
    # Each log level is defined as a member of the enumeration, with the desired string representation as its value.
    DEBUG = 'DEBUG'
    INFO = 'INFO'
    WARNING = 'WARNING'
    ERROR = 'ERROR'
    CRITICAL = 'CRITICAL'


class LogEvent:
    # Constant attributes
    _log_event_dir_name = 'logs'
    _log_event_file_name = 'log_events.json'

    def __init__(self, level: LogLevel, message, function_name="", exception=None):
        self.session_id = SESSION_ERRORS["session"]["id"]
        self.timestamp = get_now_utc_timestamp()  # Get the current timestamp in seconds since the
        # epoch (January 1, 1970)
        self.datetime = datetime.datetime.fromtimestamp(self.timestamp).isoformat()  # In strings, Indicating the
        # date and time of the event.YYYY-MM-DD HH:m:ss
        self.level = level  # DEBUG, INFO, WARNING, ERROR, CRITICAL
        self.message = message  # The main content of the log event, providing details about the event.
        self.function_name = function_name  # The name of the function or method where the log event originated.
        self.exception = exception  # Contains information about the exception, such as the exception type, or message

        # Create the directory for logs only if it doesn't exist
        if not os.path.exists(self.log_event_dir_name):
            os.makedirs(self.log_event_dir_name)
            print(f"Directory '{self.log_event_dir_name}' created successfully.")

    @property
    def log_event_dir_name(self):
        return self._log_event_dir_name

    @property
    def log_event_file_name(self):
        return self._log_event_file_name

    @property
    def filepath_log_event(self):
        return os.path.join(self._log_event_dir_name, self._log_event_dir_name)

    def save(self):
        # get_config()
        # If the event's level is ERROR then increment the number of error in the config. It will then been used
        # to update the number of errors occurred in the session object
        if self.level == LogLevel.ERROR.value:
            SESSION_ERRORS['session']['errors_number'] += 1
            update_lst_err()

        if CONFIG['general']['save_log_events']:
            return add_to_log(
                obj=self.get_event(),
                log_file_name=self.log_event_file_name,
                logs_dir=self.log_event_dir_name)

    def get_event(self):
        return {
            'Session_id': self.session_id,
            'Timestamp': self.timestamp,
            'Datetime': self.datetime,
            'Level': self.level,
            'Message': self.message,
            'Function_name': self.function_name,
            'Exception': self.exception
        }
