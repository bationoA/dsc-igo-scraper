"""This is the scraper class for `WHO Western Pacific` website. It collects all publications and their details using
WHO's API, and download the ones missing in the SQLite database """

import datetime
import inspect

from bs4 import BeautifulSoup

from src import Session, CONFIG
from src.common import filter_list_publications_and_details, generate_document_id, is_valid_url
from src.dir_fc import generate_organization_download_pdf_directory_path
from src.document import start_downloads, Document
from src.files_fc import LogEvent, LogLevel
from src.organizations import get_organization_by_condition

from src.time_fc import get_timestamp_from_date_and_time


class WhoWesternPacificScraper:
    _organization_acronym: str = "WHO"
    _organization_region: str = "Western Pacific"
    _api_rqst_max_items: int = 100  # This is the maximum number of publications returned by the API per request

    def __init__(self, session: Session):
        self.session = session
        self.organization = get_organization_by_condition(
            condition=f"acronym='{self.organization_acronym}' AND region='{self._organization_region}'")
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
    def api_rqst_max_items(self):
        return self._api_rqst_max_items

    def run(self):
        # step 1: Get details togethers with the download links of all publication
        self.get_all_publications_details_from_api()

        # # step 2: Apply filter to the list of publications list
        filter_list_publications_and_details()

        # step 3: Download new publications
        results = start_downloads(pdf_files_directory=self.pdf_files_directory)
        self.number_of_pdfs_found_in_current_session = results['total_of_pdfs_found']
        self.number_of_downloaded_pdfs_in_current_session = results['total_of_pdfs_downloaded']

        return True

    def get_publications_list_from_api(self, skip: int = 0) -> dict:
        """
        This method retrieve the links of all the existing publications.
        Note: The API returns a maximum of 50 publications per request
        :return:
        """

        import requests

        # API URL
        sf_site = "5113ecce-98b3-4708-a03a-c2de96913488"  # Specific to Europe
        sf_provider = "OpenAccessProvider"  # Specific to Europe
        filter_ = "publishingoffices%2Fany(x%3Ax%20eq%208fbbfc75-7625-469f-b844-494c5d930e85)"
        api_url = f"https://www.who.int/api/hubs/publications?sf_site={sf_site}" \
                  f"&sf_provider={sf_provider}&sf_culture=en&$orderby=PublicationDateAndTime%20desc&$select" \
                  f"=Title,ItemDefaultUrl,FormatedDate,Tag,ThumbnailUrl,DownloadUrl," \
                  f"TrimmedTitle&$format=json&$skip={skip}&$top={self.api_rqst_max_items}&$filter={filter_}&$count=true"
        ""

        try:
            # Send a GET request to the API
            response = requests.get(api_url, timeout=CONFIG["general"]["request_time_out_in_second"])

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                # Parse JSON data
                json_data = response.json()

                return json_data
            else:
                # If the request was not successful, print the status code
                print(f"Error: Request failed with status code {response.status_code}")

        except requests.exceptions.RequestException as e:
            # If there was an error with the request, print the error message
            msg = f"Error while retrieving data. Param skip = {skip}: {e}"
            print(msg)
            LogEvent(level=LogLevel.ERROR.value,
                     message=msg,
                     function_name=inspect.currentframe().f_code.co_name).save()
            return {}

    def get_all_publications_details_from_api(self):
        """
        Get publication with details using the API

        :return:
        """

        temp_max_publications = []  # will be used to store publications until they reach the max bunch size
        # (See config file). Then there are inserted in a temporary table in the database. It is then set back to [] for
        # another tour
        load_more = True  # Becomes False
        i = 0
        print("\r", f"Retrieving publication using API: 0", end="")
        total_publications_retrieved = 0
        total_unique_documents_retrieved = 0
        while load_more:
            response = self.get_publications_list_from_api(skip=total_publications_retrieved)

            if response is None or "value" not in response or not len(response["value"]):
                break

            publications_list = response["value"]

            documents_details_list = self.extract_required_details_from_publications_list(publications_list)

            new_documents_details_list = []
            # insert them into the temporary table `temp_documents_table`
            for doc in documents_details_list:
                if not doc.exist_in_temporary_table():
                    doc.insert_in_temporary_table()
                    new_documents_details_list.append(doc)

            total_publications_retrieved += len(publications_list)
            total_unique_documents_retrieved += len(new_documents_details_list)
            print(end=f"\r Retrieving publication using API: {total_publications_retrieved}")

            if len(publications_list) < self.api_rqst_max_items:
                # if the maximum number of publications returned is less than 50 then do not try to load more as we
                # reached the end of the available publication
                load_more = False

            i += 1

    def extract_required_details_from_publication(self, publication: dict) -> Document:
        """
        This function take a publication that came directly from the API, and it extracts only the values that we're
        interested in and return as a Document.
        :param publication:
        :return:
        """

        publication_title = publication["TrimmedTitle"]
        download_link = publication["DownloadUrl"].strip()
        publication_timestamp = ""
        try:
            d, m, y = publication["FormatedDate"].split(' ')  # month_name, day, year
            d = d.replace(',', '')  # remove the comma next to the day
            d, y = int(d), int(y)
            month_number = datetime.datetime.strptime(m, '%B').month  # get the integer representation of a
            # month
            publication_timestamp = get_timestamp_from_date_and_time(year=y, month=month_number, day=d)
        except:
            pass

        # Create and id for the current pdf
        document_id = generate_document_id(organization_acronym=self.organization_acronym,
                                           org_region=self.organization_region,
                                           publication_title=publication_title,
                                           pdf_download_link=download_link)

        return Document(_id=document_id,
                        session_id=self.session.id,
                        organization_id=self.organization.id,
                        tags=publication["Tag"],
                        publication_date=publication_timestamp,
                        publication_url="",
                        downloaded_at=datetime.datetime.utcnow().isoformat(),
                        pdf_link=download_link,
                        lang=""
                        )

    def extract_required_details_from_publications_list(self, publications_list: list) -> list:
        """
        This function take a list of publications that came directly from the API, and for each of them it extracts only
        the values that we're interested in.
        :param publications_list:
        :return:
        """
        return [self.extract_required_details_from_publication(publication=p) for p in publications_list if
                p["DownloadUrl"] is not None and p["DownloadUrl"] != ""]
