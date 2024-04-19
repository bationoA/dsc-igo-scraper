from . import SESSION_ERRORS
from .common import *
from .db_handler import DatabaseHandler
from .time_fc import timestamp_to_datetime_isoformat, get_now_utc_timestamp


class Session:

    def __init__(self, _id: int = None, started_at: str = "", ended_at: str = "", errors_number: int = 0):
        self.id = _id
        self.db_handler = DatabaseHandler()
        self.started_at = started_at
        self.ended_at = ended_at
        self.errors_number = errors_number

        if self.id is None:
            # New session
            print("Starting program...")
            self.started_at = timestamp_to_datetime_isoformat(timestamp=get_now_utc_timestamp())
            # Insert new session in the table
            self.insert()
            # Get new session ID and update the id of the current instance of the session object
            self._get_last_inserted_session()

            SESSION_ERRORS['session']['errors_number'] = 0

    def insert(self) -> bool:
        table_name = CONFIG["general"]["sessions_table"]
        data = remove_keys_from_list_of_dicts(data=[self.to_dict()], keys_list=["id"], to_remove=True)
        return self.db_handler.insert_data_into_table(
            table_name=table_name,
            data=data[0]
        )

    def _get_last_inserted_session(self):
        session = self.db_handler.select_columns(
            table_name=CONFIG["general"]["sessions_table"],
            columns=["*"],
            condition=f"id = (SELECT MAX(id) FROM {CONFIG['general']['sessions_table']})"
        )
        session = session[0]

        self.id = session['id']
        self.started_at = session['started_at']
        self.ended_at = session['ended_at']
        self.errors_number = session['errors_number']

    def to_dict(self) -> dict:
        return {
            'id': self.id,
            'started_at': self.started_at,
            'ended_at': self.ended_at,
            'errors_number': self.errors_number
        }

    def update_session(self, data: dict):
        self.db_handler.update_table(table_name=CONFIG["general"]["sessions_table"],
                                     data=data,
                                     condition=f"id = ?",
                                     condition_vals=(self.id,)
                                     )

    def new_error(self):
        """
        Increment by one at each new error that occurs during the session
        :return:
        """
        # TODO: Still thinking of the usefulness of this function ;D ??
        self.errors_number += 1

    def interrupt(self):
        self.errors_number = SESSION_ERRORS['session']['errors_number']
        self.ended_at = timestamp_to_datetime_isoformat(timestamp=get_now_utc_timestamp())
        data = {
            "ended_at": self.ended_at,
            "errors_number": self.errors_number
        }
        self.update_session(data=data)
