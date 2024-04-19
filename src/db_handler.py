import inspect
import os
import sqlite3
from sqlite3 import Connection
import pandas as pd

from src.files_fc import CONFIG, LogEvent, LogLevel


# CONFIG = load_yaml(filepath="config.yaml")


class DatabaseHandler:
    def __init__(self):
        self.db_file_path = CONFIG['general']['database']['sql_lite']['azure_path'] if \
            CONFIG["on_azure_jupyter_cloud"] else CONFIG['general']['database']['sql_lite']['path']
        self.db_file = os.path.join(*self.db_file_path,
                                    CONFIG['general']['database']['sql_lite']['name'])
        self.connection = None
        self.cursor = None

    def connect(self):
        self.connection = sqlite3.connect(self.db_file)
        self.cursor = self.connection.cursor()

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.connection:
            self.connection.close()
            self.connection = None

    def execute_query(self, query, parameters=None):
        connection_already_existed = True
        if self.connection is None:
            # If no connection is available then create it
            self.connect()
            connection_already_existed = False

        if parameters:
            self.cursor.execute(query, parameters)
        else:
            self.cursor.execute(query)
        self.connection.commit()

        if not connection_already_existed:
            # if the method has created the connection it can close, otherwise, it must leave that as it is
            self.disconnect()

    def fetch_data(self, query, parameters=None):
        self.connect()
        if parameters:
            self.cursor.execute(query, parameters)
        else:
            self.cursor.execute(query)

        # Allows accessing attributes by their name (e.g rows[0]['id'], rows[0]['pdf_link'], ...)
        self.cursor.row_factory = sqlite3.Row

        data = self.cursor.fetchall()
        self.disconnect()
        return data

    def table_exists(self, table_name):
        self.connect()
        q = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        self.execute_query(query=q, parameters=(table_name,))
        result = self.cursor.fetchone()
        self.disconnect()
        return result is not None

    def select_columns(self, table_name: str, columns: list, condition: str = "", condition_vals: tuple = None) -> list:
        """
        Selects the specified columns' values from the table based on the condition.
        Returns a list of rows, where each row is a tuple of column values.
        """
        # Generate the SQL query to select data
        column_names = ', '.join(columns)
        query = f"SELECT {column_names} FROM {table_name}"

        # Append the condition if provided
        if condition:
            query += f" WHERE {condition}"

        if condition and condition_vals is not None:
            return self.fetch_data(query=query, parameters=condition_vals)
        else:
            return self.fetch_data(query=query)

    def insert_data_into_table(self, table_name: str, data: dict) -> bool:
        """
        Inserts data into the specified table using the keys as column names and values as values.
        """
        self.connect()  # We establish we want to check the number of inserted rows before closing the connection

        # Generate the SQL query to insert data
        columns = ', '.join(data.keys())
        values = ', '.join('?' * len(data))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

        params = tuple(data.values())

        try:
            self.execute_query(query=query, parameters=params)
        except sqlite3.Error as e:
            msg = f"Insertion failed: {e.__str__()}"
            print(msg)
            # Save event in logs
            LogEvent(level=LogLevel.ERROR.value,
                     message=f"{e.__str__()} - Query: {query} - Data: {data}",
                     function_name=inspect.currentframe().f_code.co_name,
                     exception=e.__str__()).save()

            self.disconnect()  # disconnect before leaving the function

            # If error, return False
            return False

        # Get the rowcount
        if not self.cursor.rowcount > 0:
            msg = "Insertion failed."
            print(msg)
            LogEvent(level=LogLevel.ERROR.value,
                     message=f"{msg} - Query: {query}",
                     function_name=inspect.currentframe().f_code.co_name).save()

            self.disconnect()  # disconnect before leaving the function
            return False

        self.disconnect()  # disconnect before leaving the function

        return True

    def update_table(self, table_name: str, data: dict, condition: str = "", condition_vals: tuple = None) -> bool:
        """
        Updates the specified columns' values in the table based on the condition.
        """
        # Generate the SQL query to update data
        set_values = ', '.join([f"{column} = ?" for column in data])
        query = f"UPDATE {table_name} SET {set_values}"
        params = tuple(data.values())

        # Append the condition if provided
        if condition:
            query += f" WHERE {condition}"

            if condition_vals is not None:
                params = tuple(list(params) + list(condition_vals))

        try:
            self.execute_query(query=query, parameters=params)
        except sqlite3.Error as e:
            msg = f"Update failed: {e.__str__()}"
            print(f"query: {query}")
            print(f"params: {params}")
            print(msg)
            # Save event in logs
            LogEvent(level=LogLevel.ERROR.value,
                     message=f"{e.__str__()} - Query: {query} - Data: {data}",
                     function_name=inspect.currentframe().f_code.co_name,
                     exception=e.__str__()).save()

            # If error, return False
            return False

        return True

    def delete_from_table(self, table_name: str, condition: str = "", condition_vals: tuple = None) -> bool:
        """
        Delete row(s) from a table based on the condition.
        """
        # Generate the SQL query to update data
        query = f"DELETE FROM {table_name}"

        # Append the condition if provided
        if condition:
            query += f" WHERE {condition}"

        try:
            if condition_vals is not None:
                self.execute_query(query=query, parameters=condition_vals)
            else:
                self.execute_query(query=query)
        except sqlite3.Error as e:
            msg = f"Delete failed: {e.__str__()}"
            print(msg)
            # Save event in logs
            LogEvent(level=LogLevel.ERROR.value,
                     message=f"{e.__str__()} - Query: {query}",
                     function_name=inspect.currentframe().f_code.co_name,
                     exception=e.__str__()).save()

            # If error, return False
            return False

        return True


def organizations_from_csv_to_organizations_table(file_path: str):
    """

    :param file_path:
    :return:
    """
    db_handler = DatabaseHandler()
    organizations_df = pd.read_csv(filepath_or_buffer=file_path)

    # Only keep columns of interest
    columns_to_keep = ["acronym", "name", "region", "home_page_url", "link"]

    organizations_df = organizations_df[columns_to_keep]

    grp_organizations_df = organizations_df.groupby(["acronym", "region"]).size().reset_index(name='link_count')
    # Get organizations with more than one publication's links
    many_link_organizations = grp_organizations_df[grp_organizations_df['link_count'] >= 2]

    for ind, row in many_link_organizations.iterrows():
        # Get an organization with all its links
        org_multi_links = organizations_df[(organizations_df['acronym'] == row['acronym']) &
                                           (organizations_df['region'] == row['region'])]

        # Store the links in a string separated with semicolons(;)
        links = "; ".join(org_multi_links['link'].values)
        organizations_df.loc[(organizations_df['acronym'] == row['acronym']) &
                             (organizations_df['region'] == row['region']), 'link'] = links

    # Remove duplicate organizations based on acronym, name, and region
    organizations_df.drop_duplicates(subset=["acronym", "region"], inplace=True)

    # Rename column 'link' to 'publication_urls'
    organizations_df.rename(columns={'link': 'publication_urls'}, inplace=True)

    # insert only the organizations that do not already exist in the organizations_table
    # Create an 'id' column as the index for the DataFrame
    organizations_df['id'] = organizations_df.index
    db_handler.connect()
    organizations_df.to_sql(CONFIG["general"]["organizations_table"],
                            db_handler.connection,
                            if_exists='replace',
                            index=False
                            )


def init_database() -> bool:
    """
    This function will initialize the database by creating a new database if it does not exist with all its tables.
    :return:
    """
    db_handler = DatabaseHandler()  # Initialize database instance

    # If database does not exist, then create it with all its tables
    if not os.path.exists(db_handler.db_file) or not os.path.isfile(db_handler.db_file):
        initialize_sql_lite_database_folder()  # create directories if they don't exist

    # ------- Create tables if they don't exist
    # Creating table organizations_table
    table = CONFIG["general"]["sessions_table"]
    if not db_handler.table_exists(table_name=table):
        info = f"--------- Creating SQL Lite database table '{table}' in '{db_handler.db_file}'"
        print(info)

        # Save event in logs
        LogEvent(level=LogLevel.INFO.value,
                 message=info,
                 function_name=inspect.currentframe().f_code.co_name).save()
        try:
            query = f'''CREATE TABLE {table} (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    started_at TEXT,
                                    ended_at TEXT,
                                    errors_number INTEGER
                                )'''

            db_handler.execute_query(query=query)
        except BaseException as e:
            msg = f"--------- An error occurred  while creating table '{table}' in SQL Lite database at " \
                  f"{db_handler.db_file} "
            print(msg)
            # Save event in logs
            LogEvent(level=LogLevel.ERROR.value,
                     message=msg,
                     function_name=inspect.currentframe().f_code.co_name,
                     exception=e.__str__()).save()

            # If error, close connection and return False
            # conn.close()
            return False

    # Creating table organizations_table
    table = CONFIG["general"]["organizations_table"]
    if not db_handler.table_exists(table_name=table):
        info = f"--------- Creating SQL Lite database table '{table}' in '{db_handler.db_file}'"
        print(info)

        # Save event in logs
        LogEvent(level=LogLevel.INFO.value,
                 message=info,
                 function_name=inspect.currentframe().f_code.co_name).save()
        try:
            query = f'''CREATE TABLE {table} (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                acronym TEXT,
                                name TEXT,
                                region TEXT,
                                home_page_url TEXT,
                                publication_urls TEXT
                            )'''

            db_handler.execute_query(query=query)
        except BaseException as e:
            msg = f"--------- An error occurred  while creating table '{table}' in SQL Lite database at " \
                  f"{db_handler.db_file} "
            print(msg)
            # Save event in logs
            LogEvent(level=LogLevel.ERROR.value,
                     message=msg,
                     function_name=inspect.currentframe().f_code.co_name,
                     exception=e.__str__()).save()

            # If error, close connection and return False
            # conn.close()
            return False

    # Creating table CONFIG["general"]["documents_table"]
    table = CONFIG["general"]["documents_table"]
    if not db_handler.table_exists(table_name=table):
        msg = f"--------- Creating SQL Lite database table '{table}' in '{db_handler.db_file}'"
        print(msg)

        try:
            query = f'''CREATE TABLE {table} (
                                id TEXT PRIMARY KEY,
                                session_id INTEGER,
                                organization_id INTEGER,
                                language TEXT,
                                tags TEXT,
                                publication_date TEXT,
                                downloaded_at TEXT,
                                publication_url TEXT,
                                pdf_link TEXT,
                                error INTEGER DEFAULT 0,
                                FOREIGN KEY (organization_id) REFERENCES organization(id),
                                FOREIGN KEY (session_id) REFERENCES sessions(id)
                            )'''

            # Commit the changes to the database
            # conn.commit()
            db_handler.execute_query(query=query)
        except BaseException as e:
            msg = f"--------- An error occurred  while creating table '{table}' in SQL Lite database at " \
                  f"{db_handler.db_file} "
            print(msg)
            # Save event in logs
            LogEvent(level=LogLevel.ERROR.value,
                     message=msg,
                     function_name=inspect.currentframe().f_code.co_name,
                     exception=e.__str__()).save()

            # If error, close connection and return False
            # conn.close()
            return False

    # Creating table temp_publications_urls_table
    table = CONFIG["general"]["temp_publications_urls_table"]
    if not db_handler.table_exists(table_name=table):
        msg = f"--------- Creating SQL Lite database table '{table}' in '{db_handler.db_file}'"
        print(msg)

        try:
            query = f'''CREATE TABLE {table} (
                                    id INTEGER PRIMARY KEY,
                                    url TEXT
                                )'''

            # Commit the changes to the database
            # conn.commit()
            db_handler.execute_query(query=query)
        except BaseException as e:
            msg = f"--------- An error occurred  while creating table '{table}' in SQL Lite database at " \
                  f"{db_handler.db_file} "
            print(msg)
            # Save event in logs
            LogEvent(level=LogLevel.ERROR.value,
                     message=msg,
                     function_name=inspect.currentframe().f_code.co_name,
                     exception=e.__str__()).save()

            # If error, close connection and return False
            # conn.close()
            return False

    # Creating table temp_documents_table
    table = CONFIG["general"]["temp_documents_table"]
    if not db_handler.table_exists(table_name=table):
        msg = f"--------- Creating SQL Lite database table '{table}' in '{db_handler.db_file}'"
        print(msg)

        try:
            query = f'''CREATE TABLE {table} (
                                id_temp INTEGER PRIMARY KEY AUTOINCREMENT,
                                id TEXT UNIQUE,
                                session_id INTEGER,
                                organization_id INTEGER,
                                language TEXT,
                                tags TEXT,
                                publication_date TEXT,
                                downloaded_at TEXT,
                                publication_url TEXT,
                                pdf_link TEXT,
                                error INTEGER DEFAULT 0
                            )'''

            # Commit the changes to the database
            # conn.commit()
            db_handler.execute_query(query=query)
        except BaseException as e:
            msg = f"--------- An error occurred  while creating table '{table}' in SQL Lite database at " \
                  f"{db_handler.db_file} "
            print(msg)
            # Save event in logs
            LogEvent(level=LogLevel.ERROR.value,
                     message=msg,
                     function_name=inspect.currentframe().f_code.co_name,
                     exception=e.__str__()).save()

            # If error, close connection and return False
            # conn.close()
            return False

    # ------- Insert organizations list from csv file into organizations' table
    organizations_list_csv_file_path = os.path.join("assets", "data", "organizations_list.csv")  # Get csv file path
    # Start inserting...
    organizations_from_csv_to_organizations_table(file_path=organizations_list_csv_file_path)

    return True


def initialize_sql_lite_database_folder():
    parent_folder = ""
    db_file_path_dirs = CONFIG['general']['database']['sql_lite']['azure_path'] if \
        CONFIG["on_azure_jupyter_cloud"] else CONFIG['general']['database']['sql_lite']['path']
    for folder in db_file_path_dirs:
        directory = os.path.join(parent_folder, folder)

        # Create the directory only if it doesn't exist
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Directory '{directory}' created successfully.")

        parent_folder = directory


def reset_temp_documents_table():
    db_handler = DatabaseHandler()
    temp_documents_table = CONFIG["general"]["temp_documents_table"]
    db_handler.delete_from_table(table_name=temp_documents_table)

    # -- Reset the auto-increment sequence to start from 1
    data = {"seq": 0}
    condition = f"name = '{temp_documents_table}'"
    db_handler.update_table(table_name="sqlite_sequence", data=data, condition=condition)


def reset_temp_publications_urls_table():
    db_handler = DatabaseHandler()
    db_handler.delete_from_table(table_name=CONFIG["general"]["temp_publications_urls_table"])

    # -- Reset the auto-increment sequence to start from 1
    data = {"seq": 0}
    condition = f"name = ?"
    condition_vals = (CONFIG['general']['temp_publications_urls_table'],)
    db_handler.update_table(table_name="sqlite_sequence",
                            data=data,
                            condition=condition,
                            condition_vals=condition_vals
                            )
    # UPDATE sqlite_sequence SET seq = 0 WHERE name = 'table_name';


def get_total_temp_documents() -> int:
    db_handler = DatabaseHandler()
    temps_docs = db_handler.select_columns(table_name=CONFIG["general"]["temp_documents_table"], columns=["*"])
    return len(temps_docs)


def get_total_temp_publications_urls() -> int:
    db_handler = DatabaseHandler()
    temps_docs = db_handler.select_columns(table_name=CONFIG["general"]["temp_publications_urls_table"], columns=["*"])
    return len(temps_docs)


def get_chunk_temp_publications_urls(from_id: int, limit: int = 1) -> list:
    """
    Get a set of temporary documents
    :param from_id:
    :param limit:
    :return:
    """
    db_handler = DatabaseHandler()
    columns = ["*"]
    condition = f" id >= ? LIMIT ?"
    temp_publications = db_handler.select_columns(table_name=CONFIG["general"]["temp_publications_urls_table"],
                                                  columns=columns,
                                                  condition=condition,
                                                  condition_vals=(from_id, limit)
                                                  )

    return temp_publications


def get_chunk_temp_documents_as_dict(from_id_temp: int, limit: int = 1) -> list:
    """
    Get a set of temporary documents
    :param from_id_temp:
    :param limit:
    :return:
    """
    db_handler = DatabaseHandler()
    columns = ["*"]
    condition = f" id_temp >= ? LIMIT ?"
    return db_handler.select_columns(CONFIG["general"]["temp_documents_table"],
                                     columns=columns,
                                     condition=condition,
                                     condition_vals=(from_id_temp, limit)
                                     )
