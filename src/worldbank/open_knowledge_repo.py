"""This is the scraper class for the ` WorldBank's Open Knowledge Repository` website. It collects all publications
and their details, and download the ones missing in the SQLite database """
import datetime
import inspect

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


class OpenKnowledgeRepoScraper:
    _organization_acronym: str = "World Bank"
    _organization_region: str = "Openknowledge"
    _download_base_url = "https://openknowledge.worldbank.org/"

    def __init__(self, session: Session):
        self.session = session
        self.organization = get_organization_by_condition(
            condition=f"acronym='{self.organization_acronym}' AND region='{self._organization_region}'")
        self.publications_page_url = self.organization.publication_urls
        self.starting_page = 1
        self.max_pb_per_page = 100  # Maximum number of publications per page
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
        # step 1: Get links of all publications
        self.get_all_publications_links()

        # step 2: Get details togethers with the download links of each publication on the current page
        self.get_publications_details_from_urls()

        # step 3: Apply filter to the list of publications list
        filter_list_publications_and_details()

        # step 4: Download new publications
        results = start_downloads(pdf_files_directory=self.pdf_files_directory)
        self.number_of_pdfs_found_in_current_session = results['total_of_pdfs_found']
        self.number_of_downloaded_pdfs_in_current_session = results['total_of_pdfs_downloaded']

        return True

    def get_all_publications_links(self):
        """
        This method retrieve the links of all the existing publications
        :return:
        """
        # For each page,
        print(f" Retrieving publications: 0", end="")
        nbr_retrieved_publications = 0  # Number of retrieved publications
        max_nbr_none = 3  # Maximum number of consecutive None values for `current_page_soup` before exiting the
        # `while` loop
        last_page = False
        page = 1  # 1 is the first page
        nbr_none = 0  # Number of consecutive None values for `current_page_soup`
        while not last_page:
            if nbr_none >= max_nbr_none:
                break
            # Get the url of the page
            page_ulr = self.get_page_url(page_number=page)

            # Get the current page (as a BeautifulSoup object)
            current_page_soup = get_page_from_url(url=page_ulr)

            if current_page_soup is None:
                nbr_none += 1
                msg = f"current_page_soup was None for the page ulr: {page_ulr}. \n " \
                      f"Check your internet connection and/or the page ulr.",
                print(msg)
                LogEvent(level=LogLevel.WARNING.value,
                         message=msg,
                         function_name=inspect.currentframe().f_code.co_name).save()
                continue
            else:
                # reset the counter of the Number of consecutive None values for `current_page_soup`
                nbr_none = 0

            # --- Check if the current page is the last one
            # Find the img tag with attribute 'aria-disabled' not set. I.e. the link is not disabled
            next_page = current_page_soup.find('a', attrs={'href': '', 'aria-label': 'Next', 'aria-disabled': None})

            # If next_page is None then we've reached the last page
            if next_page is None:
                last_page = True
            # ---

            # Get list of publications with their link on the current page
            publ_links = self.get_list_of_publication_links_from_page(soup_page=current_page_soup, url=page_ulr)

            for publ_link in publ_links:
                existing_url = self.session.db_handler.select_columns(
                    columns=["*"],
                    table_name=CONFIG["general"]["temp_publications_urls_table"],
                    condition="url = ?",
                    condition_vals=(publ_link,)
                )

                if not existing_url:
                    self.session.db_handler.insert_data_into_table(
                        table_name=CONFIG["general"]["temp_publications_urls_table"],
                        data={"url": publ_link})
                    nbr_retrieved_publications += 1

            print(end=f"\r Retrieving publications: {nbr_retrieved_publications}")

            page += 1
        print("")

    def get_publication_tags_list(self, publication_page_soup: BeautifulSoup) -> str:
        publication_tag_links_list = publication_page_soup.find_all('a', class_=['tag-link'])

        tags_list = ""
        if publication_tag_links_list:
            tags_list_ = [link.text for link in publication_tag_links_list]
            tags_list = "; ".join(tags_list_)

        return tags_list

    def get_publications_details_from_api(self, pub_id: int = 0) -> list:
        """
        This method retrieve the links of all the existing publications.
        Note: The API returns a maximum of 50 publications per request
        :return:
        """
        # API URL
        api_url = f"https://digitallibrary.un.org/api/v1/file?recid={pub_id}&file_types=[]&hidden_types=[" \
                  "%22pdf%3Bpdfa%22%2C%22tif%22%2C%22tiff%22]&ln=en&hr=1"
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
                return []

        except requests.exceptions.RequestException as e:
            # If there was an error with the request, print the error message
            msg = f"Error while retrieving data. Param recid = {pub_id}: {e}"
            print(msg)
            LogEvent(level=LogLevel.ERROR.value,
                     message=msg,
                     function_name=inspect.currentframe().f_code.co_name).save()
            return []

    def get_publication_details(self, publication_url: str) -> list:
        """
        Return the details of a publication
        """
        results = []

        # Get publication's page
        pub_page = get_page_from_url(url=publication_url,
                                     ssl_verify=False,
                                     max_attempt=CONFIG["general"]["max_request_attempt"],
                                     max_waiting_time_sec=CONFIG["general"]["max_waiting_time_sec"]  # 15 minutes
                                     )

        if not pub_page:  # No page related to the publication's url were returned
            return []

        # --- Get publication's title
        # Note: The title is not returned by the API, therefore we'll use the publication url as the title
        publication_title = pub_page.find('he', class_="item-page-title-field mr-auto")
        publication_title = publication_url if publication_title is None else publication_title

        # --- Get Tags
        tags_list = pub_page.find('div', class_="collections")
        tags_list_text = clean_text(text=tags_list.text) if tags_list.text else tags_list.text
        tags_list = "; ".join(tags_list_text.split(",")) if tags_list_text else ""

        # --- Publication's date
        publication_date = pub_page.find('span', class_="dont-break-out ng-star-inserted")  # e.g. '1947-09-12'
        publication_date = clean_text(text=publication_date.text) if publication_date is not None else ""
        try:
            y, m, d = [int(dt) for dt in publication_date.split("-")]
            publication_timestamp = get_timestamp_from_date_and_time(year=y, month=m, day=d)
            publication_date = timestamp_to_datetime_isoformat(timestamp=publication_timestamp)
        except:
            publication_date = ""
            pass

        # -- Get versions languages and file urls
        pub_versions_links_n_lang = self.get_links_n_lang_from_page(soup_page=pub_page)

        for pub_version in pub_versions_links_n_lang:
            # --- Get download link
            link = pub_version['url']

            # --- Get current file's language
            lang = pub_version['lang']
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

        return results

    def get_links_n_lang_from_page(self, soup_page: BeautifulSoup):
        """
        This method retrieve language and link of all available versions .

        :param soup_page:
        :return:
        """
        list_links = []

        # Find all links on the page
        ds_file_list = soup_page.find_all('ds-file-download-link')

        if ds_file_list is None:
            return []

        # Extract 'a' tags
        a_tag_list = []
        for ds_file_ in ds_file_list:
            a_tag = ds_file_.find('a')
            if a_tag:
                a_tag_list.append(a_tag)

        if not len(a_tag_list):
            return []

        # Extract the link URLs from the 'a' tags
        for link in a_tag_list:
            if link is not None and link.get("href") is not None:
                link_text = add_base_url_if_missing(base_url=self.download_base_url, url=link.get("href").strip())

                lang_n_file_type = link.get('data-text')  # Get file's type and language

                lang = ""
                f_type = ""
                if lang_n_file_type:
                    lang_n_file_type = clean_text(text=lang_n_file_type)
                    try:
                        lang, f_type = lang_n_file_type.split(" ")
                    except:
                        pass

                if not f_type:  # If file's type were not found where expected, try this second location
                    try:
                        attr_data_customlink = link.get('data-customlink')
                        f_type = attr_data_customlink.split("::")[-1]
                    except:
                        pass

                # Check if current file type is supported. If not then skip
                if f_type.lower() in [tp for tp in CONFIG["general"]["file_types"]]:
                    lang = format_language(lang=clean_text(lang)) if clean_text(lang) else ""

                    list_links.append({
                        'lang': lang,
                        'url': link_text
                    })

        return list_links

    def get_list_of_publication_links_from_page(self, soup_page: BeautifulSoup, url="") -> list:
        """
        This function retrieve the links related to each publication of
        a specific page and returns them in a list
        """
        publication_a_tag_list = soup_page.find_all('a', class_="list-element-title ng-star-inserted")

        if not publication_a_tag_list:
            msg = f"No publication found on this page url: {url}"
            print(msg)
            LogEvent(level=LogLevel.ERROR.value,
                     message=msg,
                     function_name=inspect.currentframe().f_code.co_name).save()
            return []

        list_publ_link = []  # this will contain the links of each publication on the give page

        # Collect links
        for a_tag in publication_a_tag_list:
            # Get publication's link
            p_link = ""

            try:
                p_link = a_tag['href']
                p_link = add_base_url_if_missing(base_url=self.download_base_url, url=p_link)
                list_publ_link.append(p_link)
            except:
                pass

            if p_link:
                list_publ_link.append(p_link)

        return list_publ_link

    def get_publications_details_from_urls(self):
        """
        This function takes a list of publication links and
        returns a list of their corresponding download url
        """
        # length_publications_urls = len(publications_urls)
        length_publications_urls = get_total_temp_publications_urls()
        print("\r", f"Retrieving publication details: 0%", end="")

        # Retrieve publications by chunks
        chunk_size = CONFIG["general"]["max_publication_urls_chunk_size"]
        chunk_total = length_publications_urls / chunk_size
        chunk_total = int(chunk_total) + 1 if int(chunk_total) < chunk_total else int(chunk_total)

        start_id = 0
        ind = 0
        for i in range(chunk_total):
            result_publications_urls = get_chunk_temp_publications_urls(from_id=start_id,
                                                                        limit=chunk_size)

            publications_urls = [purl['url'] for purl in result_publications_urls]
            last_url = result_publications_urls[-1]
            start_id = last_url['id'] + 1

            for page_url in publications_urls:
                publication_details = self.get_publication_details(
                    publication_url=page_url)  # Get the download link
                if not publication_details:
                    print("Warning. A pdf will be missing: Download link was not found for: ", page_url)
                else:
                    # if link found, then store it
                    for pub_d in publication_details:
                        # Check if already in temporary documents table
                        if not pub_d.exist_in_temporary_table():
                            pub_d.insert_in_temporary_table()

                ind += 1

                print(end=f"\r Retrieving publication details: {round(100 * ind / length_publications_urls, 2)}% ")

    def get_page_url(self, page_number: int) -> str:
        """
        This function generate the url of a page for UN publications based the page number and publication's type
        page_number: the page number
        pub_type: publication's type. e.g 'Reports', 'publications'
        """
        url = "https://openknowledge.worldbank.org/search?spc.sf=score&spc.sd=DESC&spc.page={}&spc.rpp={}"

        return url.format(page_number, self.max_pb_per_page)
