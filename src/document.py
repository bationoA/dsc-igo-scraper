import inspect

from .common import format_file_name, remove_keys_from_list_of_dicts, download_and_save_pdfs, \
    download_and_save_pdfs_multiprocessing
from .db_handler import DatabaseHandler, get_total_temp_documents, get_chunk_temp_documents_as_dict
from .dir_fc import is_pdf_already_exist
from .organizations import get_organization_by_id
from .files_fc import LogEvent, LogLevel, CONFIG
from .time_fc import get_now_utc_timestamp, get_remaining_time_estimate


class Document:

    def __init__(self, _id: str, session_id: int, organization_id: int, tags: str, publication_date: str,
                 publication_url: str, downloaded_at: str, pdf_link: str, title: str = "", formatted_title: str = "",
                 lang="", error=0):

        self.id = _id
        self.session_id = session_id
        self.organization_id = organization_id
        self.lang = lang
        self.tags = tags  # comma separated
        self.publication_date = publication_date
        self.publication_url = publication_url
        self.title = title
        self.formatted_title = formatted_title
        self.db_handler = DatabaseHandler()

        # TODO: Get organization acronym by id if acronym is not provided
        self.organization = get_organization_by_id(_id=self.organization_id)

        if title == "" and formatted_title != "":
            msg = "ERROR: title and formatted_title cannot be None at the same time, please provide one of them"
            print(msg)
            # Save event in logs
            LogEvent(level=LogLevel.ERROR.value,
                     message=msg,
                     function_name=inspect.currentframe().f_code.co_name).save()
        elif formatted_title == "" and self.title != "":
            self.formatted_title = format_file_name(org_acronym=self.organization.acronym,
                                                    org_region=self.organization.region,
                                                    publication_title=self.title,
                                                    lang=self.lang)
        self.downloaded_at = downloaded_at
        self.pdf_link = pdf_link
        self.error = error

    def insert(self) -> bool:
        """
        Insert a new document into documents_table
        :return:
        """
        data = remove_keys_from_list_of_dicts(data=[self.to_dict()], keys_list=["formatted_title"], to_remove=True)
        return self.db_handler.insert_data_into_table(
            table_name=CONFIG["general"]["documents_table"],
            data=data[0]
        )

    def update(self):
        """
        Update document's metadata in the database
        :return:
        """
        data = remove_keys_from_list_of_dicts(
            data=[self.to_dict()], keys_list=["id", "formatted_title"], to_remove=True)
        return self.db_handler.update_table(
            table_name=CONFIG["general"]["documents_table"],
            data=data[0],
            condition="id=?",
            condition_vals=(self.id,)
        )

    def exist_on_disk(self) -> bool:
        return is_pdf_already_exist(self.formatted_title, config=CONFIG)

    def exist_in_database(self) -> bool:
        result = self.db_handler.select_columns(table_name=CONFIG["general"]["documents_table"],
                                                columns=["*"],
                                                condition="id=?",
                                                condition_vals=(self.id,)
                                                )
        return len(result) != 0

    def exist_with_error_in_database(self) -> bool:
        condition = "id=? AND error = ?"
        result = self.db_handler.select_columns(table_name=CONFIG["general"]["documents_table"],
                                                columns=["*"],
                                                condition=condition,
                                                condition_vals=(self.id, 1)
                                                )
        return len(result) != 0

    def to_dict(self) -> dict:
        return {
            'id': self.id,
            'session_id': self.session_id,
            'organization_id': self.organization_id,
            'language': self.lang,
            'tags': self.tags,
            'formatted_title': self.formatted_title,
            'publication_date': self.publication_date,
            'downloaded_at': self.downloaded_at,
            'publication_url': self.publication_url,
            'pdf_link': self.pdf_link,
            'error': self.error
        }

    # --- Methods for temporary tables
    def insert_in_temporary_table(self) -> bool:
        """
        Insert a new document into temp_documents_table
        :return:
        """
        data = remove_keys_from_list_of_dicts(data=[self.to_dict()],
                                              keys_list=["formatted_title"],
                                              to_remove=True)
        return self.db_handler.insert_data_into_table(
            table_name=CONFIG["general"]["temp_documents_table"],
            data=data[0]
        )

    def exist_in_temporary_table(self) -> bool:
        result = self.db_handler.select_columns(table_name=CONFIG["general"]["temp_documents_table"],
                                                columns=["*"],
                                                condition="id=?",
                                                condition_vals=(self.id,)
                                                )
        return len(result) != 0

    def delete_from_temporary_table(self):
        """
        Update document's metadata in the database
        :return:
        """

        return self.db_handler.delete_from_table(
            table_name=CONFIG["general"]["temp_documents_table"],
            condition="id=?",
            condition_vals=(self.id,)
        )


def dict_to_document_object(_dict) -> Document:
    return Document(
        _id=_dict['id'],
        session_id=_dict['session_id'],
        organization_id=_dict['organization_id'],
        lang=_dict['language'],
        tags=_dict['tags'],
        publication_date=_dict['publication_date'],
        downloaded_at=_dict['downloaded_at'],
        publication_url=_dict['publication_url'],
        pdf_link=_dict['pdf_link'],
        error=_dict['error']
    )


def get_bunch_temp_documents(from_id_temp: int, limit: int = 1) -> list:
    """
    Get a set of temporary documents
    :param from_id_temp:
    :param limit:
    :return:
    """
    db_handler = DatabaseHandler()
    columns = ["*"]
    condition = f" id_temp >= ? LIMIT ?"
    temps_docs = db_handler.select_columns(table_name=CONFIG["general"]["temp_documents_table"],
                                           columns=columns,
                                           condition=condition,
                                           condition_vals=(from_id_temp, limit)
                                           )

    return [dict_to_document_object(_dict=tmp_doc) for tmp_doc in temps_docs]


def start_downloads(pdf_files_directory: str) -> dict:
    total_of_pdfs_found = 0
    total_of_pdfs_downloaded = 0
    total_assessed_docs = 0  # total number of documents on which download operation were performed with success or not.

    download_start_timestamp = get_now_utc_timestamp()

    length_temp_documents = get_total_temp_documents()  # Total pdfs to download
    print(f"\n Downloads ({length_temp_documents} files):")

    if not length_temp_documents:
        msg = "No new PDFs publications are available for download at the moment."
        print("\r", msg, end="")
        LogEvent(level=LogLevel.WARNING.value,
                 message=msg,
                 function_name=inspect.currentframe().f_code.co_name).save()
    else:

        # Retrieve temporary documents by chunks
        chunk_size = CONFIG["general"]["max_publication_urls_chunk_size"]
        chunk_total = length_temp_documents / chunk_size
        chunk_total = int(chunk_total) + 1 if int(chunk_total) < chunk_total else int(chunk_total)

        start_id = 0
        for i in range(chunk_total):
            elapse_time_msg = " "
            if i > 0:
                remaining_time = get_remaining_time_estimate(
                    start_timestamp=download_start_timestamp,
                    total_assessed_docs=total_of_pdfs_downloaded,
                    total_files=length_temp_documents)
                elapse_time_msg = f" - {remaining_time}"

            print(f"  Chunk: {i + 1} / {chunk_total}{elapse_time_msg}")

            result_temp_documents = get_chunk_temp_documents_as_dict(from_id_temp=start_id, limit=chunk_size)
            max_tmp_doc_id_tmp = max([tmp_doc['id_temp'] for tmp_doc in result_temp_documents])
            max_id_tmp_tmp_doc = [tmp_doc for tmp_doc in result_temp_documents if tmp_doc['id_temp'] ==
                                  max_tmp_doc_id_tmp]
            max_id_tmp_tmp_doc = max_id_tmp_tmp_doc[0]
            start_id = max_id_tmp_tmp_doc['id_temp'] + 1
            list_publications_to_download = [dict_to_document_object(_dict=dict_doc) for dict_doc in
                                             result_temp_documents]

            if CONFIG["general"]["allow_parallel_downloads"]:
                n_fd, n_dwd = download_and_save_pdfs_multiprocessing(
                    list_publications_details=list_publications_to_download,
                    pdfs_file_dir=pdf_files_directory
                )
            else:
                n_fd, n_dwd = download_and_save_pdfs(list_publications_details=list_publications_to_download,
                                                     pdfs_file_dir=pdf_files_directory
                                                     )
            total_of_pdfs_found += n_fd
            total_of_pdfs_downloaded += n_dwd
            total_assessed_docs += len(list_publications_to_download)
            print("\n")

    return {"total_of_pdfs_found": total_of_pdfs_found, "total_of_pdfs_downloaded": total_of_pdfs_downloaded}
