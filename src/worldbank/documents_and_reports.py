"""This is the scraper class for the ` WorldBank's Documents and Reports` repository. It collects all publications
and their details, and download the ones missing in the SQLite database """
import datetime
import inspect
import time

import requests
import urllib3
from bs4 import BeautifulSoup

from src import CONFIG
from src.dir_fc import generate_organization_download_pdf_directory_path
from src.session import Session
from src.common import filter_list_publications_and_details, get_page_from_url, generate_document_id, \
    add_base_url_if_missing, format_language, clean_text
from src.db_handler import get_total_temp_publications_urls, get_chunk_temp_publications_urls
from src.document import start_downloads, Document
from src.files_fc import LogEvent, LogLevel
from src.organizations import get_organization_by_condition
from src.time_fc import get_timestamp_from_date_and_time, timestamp_to_datetime_isoformat

# Disable the InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class DocumentsReportsScraper:
    _organization_acronym: str = "World Bank"
    _organization_region: str = "Documents and Reports"
    _download_base_url = "https://documents.worldbank.org/"

    def __init__(self, session: Session):
        self.session = session
        self.organization = get_organization_by_condition(
            condition=f"acronym='{self.organization_acronym}' AND region='{self._organization_region}'")
        self.publications_page_url = self.organization.publication_urls
        self.starting_page = 1
        self.max_pb_per_page = 500  # Maximum number of publications per page
        self.total_publications_online = 0
        self.total_number_of_pages = 0
        self.number_of_pdfs_found_in_current_session = 0
        self.number_of_downloaded_pdfs_in_current_session = 0

        self.pdf_files_directory = generate_organization_download_pdf_directory_path(
            organization_acronym_region=self.organization_acronym + "-" + self.organization_region,
            config=CONFIG)

    @property
    def organization_acronym(self):
        return self._organization_acronym

    @property
    def organization_region(self):
        return self._organization_region

    @property
    def download_base_url(self):
        return self._download_base_url

    def run(self):
        # step 1: Get links of all publications and details
        self.get_publications_details_from_api()

        # step 2: Apply filter to the list of publications list
        filter_list_publications_and_details()

        # step 3: Download new publications
        results = start_downloads(pdf_files_directory=self.pdf_files_directory)
        self.number_of_pdfs_found_in_current_session = results['total_of_pdfs_found']
        self.number_of_downloaded_pdfs_in_current_session = results['total_of_pdfs_downloaded']

        return True

    def get_publication_details(self, response) -> list:
        """
        Return the details of a publication such as:
        Title
        publication_date
        """
        results = []
        try:
            docs = response['documents']
        except:
            return []

        docs_keys_list = list(docs.keys())[0:-1]

        doc_details_list = [docs[key] for key in docs_keys_list]

        if not doc_details_list:
            return []

        # ---------- Get the details
        for publication in doc_details_list:
            # --- Get download link
            try:
                link = publication['pdfurl']
            except:
                link = ""

            if not link:  # Skip if no link was found
                continue

            # --- Get publication's title
            try:
                # Note: The title is not returned by the API, therefore we'll use the publication url as the title
                publication_title = clean_text(text=publication['display_title'])
            except:
                publication_title = ""

            # --- Get Tags
            try:
                tags_list_text = clean_text(text=publication['topicv3'])
                tags_list = "; ".join(tags_list_text.split(",")) if tags_list_text else ""
            except:
                tags_list = ""

            # --- Publication's date
            try:
                publication_date = publication['docdt']
            except:
                publication_date = ""

            # --- Get current file's language
            try:
                lang = publication['lang']
            except:
                lang = ""

            # --- Get current publication's url
            try:
                publication_url = publication['url_friendly_title']
            except:
                publication_url = ""

            # Create and id for the current pdf
            document_id = generate_document_id(organization_acronym=self.organization_acronym,
                                               org_region=self.organization_region,
                                               publication_title=publication_title,
                                               pdf_download_link=link)

            results.append(
                Document(_id=document_id,
                         session_id=self.session.id,
                         organization_id=self.organization.id,
                         title=publication_title,
                         tags=tags_list,
                         publication_date=f"{publication_date}",
                         publication_url=publication_url,
                         downloaded_at=datetime.datetime.utcnow().isoformat(),
                         pdf_link=link,
                         lang=lang
                         )
            )

        return results

    def get_publications_details_from_api(self):
        """
        This function stores the list of all publications and their corresponding details in the database
        """

        year_from = 1946
        print("\r", f"Retrieving publication details (year - {year_from}): 0", end="")

        total_retrieved_publication = 0
        year_to = datetime.datetime.utcnow().year
        for year in range(year_from, year_to + 1, 1):
            last_page = False
            skip_ = 0
            while not last_page:
                # Get the url of the page
                api_url = self.get_api_url(year=year, skip=skip_)

                # Request
                response = self.get_response_from_api(url=api_url)

                if not response or not response.ok:
                    break

                try:
                    response = response.json()
                except:
                    break

                try:
                    docs_dict = response['documents']
                    doc_list_len = len(list(docs_dict.keys())) - 1
                except:
                    doc_list_len = 0

                if not doc_list_len:
                    break

                if doc_list_len < self.max_pb_per_page:
                    last_page = True

                # Gets publications
                publication_details = self.get_publication_details(response=response)

                for doc in publication_details:
                    # Check if already in temporary documents table
                    if not doc.exist_in_temporary_table():
                        doc.insert_in_temporary_table()
                        total_retrieved_publication += 1

                skip_ += self.max_pb_per_page

                if skip_ > 100_000:  # The maximum value allowed by the API is 100_000
                    break

                print(end=f"\r Retrieving publication details "
                          f"(years {year_from} to {year}): {total_retrieved_publication} ")

    def get_api_url(self, year: int, skip: int) -> str:
        """
        This function generate the url of a page for UN publications based the page number and publication's type
        param: skip: number of publication to skip. From 0 to 100_000
        param: year: Year of the publication. From 1st January to 31st December
        """
        api_url = "https://search.worldbank.org/api/v2/wds?format=json&fl=docdt,docty&strdate={}-01-01&enddate={" \
                  "}-12-31&os={}&rows={}&sort=docdt&order=asc"

        return api_url.format(year, year, skip, self.max_pb_per_page)

    def get_response_from_api(self, url, max_attempt=1, max_waiting_time_sec=0,
                              timeout=CONFIG['general']['request_time_out_in_second']):

        response = None
        max_attempt = 1 if not max_attempt else max_attempt
        waiting_time_step = 0  # 0 second
        current_waiting_time = 0  # in seconds
        if max_attempt and max_waiting_time_sec:
            waiting_time_step = int(max_waiting_time_sec / max_attempt)

        try:
            for atp in range(max_attempt + 1):
                time.sleep(current_waiting_time)  # wait before next request attempt
                response = requests.get(url, timeout=timeout)

                if response.status_code == 429:  # If error is due to 'Too many requests'
                    if atp < max_attempt - 1:
                        current_waiting_time += waiting_time_step
                        print(f"\n  Too many requests: Will retry in {current_waiting_time} second(s)")

        except BaseException as e:
            is_success = False
            print(e)
            # Save the error in logs
            LogEvent(level=LogLevel.ERROR.value,
                     message=e.__str__(),
                     function_name=inspect.currentframe().f_code.co_name,
                     exception=e.__str__()).save()
            return None

        return response
