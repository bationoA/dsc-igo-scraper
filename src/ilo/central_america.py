"""This is the scraper class for the ` ILO's Central America` website. It collects all publications
and their details, and download the ones missing in the SQLite database """
import datetime
import inspect
import requests
import urllib3
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options  # Options while setting up the webdriver with chrome
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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


class IloCentralAmericaScraper:
    _organization_acronym: str = "ILO"
    _organization_region: str = "Central America"
    _download_base_url = "https://www.ilo.org"

    def __init__(self, session: Session):
        self.session = session
        self.organization = get_organization_by_condition(
            condition=f"acronym='{self.organization_acronym}' AND region='{self._organization_region}'")
        self.publications_page_url = self.organization.publication_urls
        self.starting_page = 0
        self.max_pb_per_page = 9  # Maximum number of publications per page
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
        page = self.starting_page  # 0 is the first page
        nbr_none = 0  # Number of consecutive None values for `current_page_soup`
        while not last_page:
            if nbr_none >= max_nbr_none:
                break
            # Get the url of the page
            page_ulr = self.get_page_url(page_number=page)

            # Get the current page (as a BeautifulSoup object)
            current_page_soup = get_page_from_url(url=page_ulr, ssl_verify=False)

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

            # Check if there are some publications on the current page
            publication_urls_h5s = current_page_soup.find_all("h5", class_="item-title")
            if not publication_urls_h5s:
                break

            if page == 0:
                # Dynamic set of elf.max_pb_per_page
                # We suppose that the first page will give the default maximum number of publication per page
                self.max_pb_per_page = len(publication_urls_h5s)

            # --- Check if the current page is the last one
            # If the number of publications on the current page is less than the maximum number of publications that
            # supposed to be on one page, then there is no `next page`. We are on the last page
            if len(publication_urls_h5s) < self.max_pb_per_page:
                last_page = True
            # ---

            # Get list of publications with their link on the current page
            try:
                publ_links = [add_base_url_if_missing(base_url=self.download_base_url, url=h5.find("a").get("href"))
                              for h5 in publication_urls_h5s]
            except:
                publ_links = []

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
        publication_title = self.get_title(soup_page=pub_page)
        publication_title = publication_url if publication_title is None else publication_title

        # --- Get Tags
        tags_list = self.get_tags(soup_page=pub_page)

        # --- Publication's date
        publication_date = self.get_publication_date(soup_page=pub_page)

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

    def get_title(self, soup_page: BeautifulSoup) -> str:
        try:
            return clean_text(soup_page.find("h1", class_="long-title"
                                             ).find('h1').text)
        except:
            return ""

    def get_tags(self, soup_page: BeautifulSoup) -> str:
        tags_list = []

        # Find the <aside> tag with class "meta-data"
        aside_tags = soup_page.find_all('aside', class_='meta-data')

        # Get the text after the <strong> tag within the <aside> tag
        if aside_tags:
            strong_tag = [aside_tag.find('strong') for aside_tag in aside_tags if aside_tag.find('strong')]
            if strong_tag:
                strong_tag = strong_tag[0]
                text_after_strong = strong_tag.next_sibling.strip()

                text_after_strong = text_after_strong.split(":")
                tags = text_after_strong[0] if len(text_after_strong) < 2 else "".join(text_after_strong[1:])
                tags_list = [clean_text(tag) for tag in tags.split(",")]

        tags_list = "; ".join(tags_list) if len(tags_list) else ""

        return tags_list

    def get_publication_date(self, soup_page: BeautifulSoup) -> str:

        try:
            ths = soup_page.find_all("th")
            date_th = [th for th in ths if "Date issued" in th.text]
            date_th = date_th[0]
            p_date = date_th.next_sibling.text.strip()
            d, m, y = p_date.split(" ")
            month_number = datetime.datetime.strptime(m, '%B').month  # get the integer representation of a month
            d, y = int(d), int(y)
            publication_timestamp = get_timestamp_from_date_and_time(year=y, month=month_number, day=d)
            p_date = timestamp_to_datetime_isoformat(timestamp=publication_timestamp)

            return p_date if p_date else ""
        except:
            return ""

    def get_links_n_lang_from_page(self, soup_page: BeautifulSoup):
        """
        This method retrieve language and link of all available versions .

        :param soup_page:
        :return:
        """

        # Find all links on the page
        all_links = soup_page.find_all("a")

        if all_links is None:
            return []

        # Extract 'a' tags those href point on at least one supported file types of our list
        a_tag_list = [atag for atag in all_links
                      if atag.get("href") and
                      any([atag.get("href").endswith(ftype) for ftype in CONFIG["general"]["file_types"]])
                      ]
        if not len(a_tag_list):
            return []

        # Extract the link langs and links
        list_links = [
            {
                'lang': "",  # No language are provided
                'url': add_base_url_if_missing(base_url=self.download_base_url, url=atag.get("href"))
            }
            for atag in a_tag_list
        ]

        return list_links

    def get_publications_details_from_urls(self):
        """
        This function takes a list of publication links and
        returns a list of their corresponding download url
        """
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
        This function generate the url of a page based on the page number
        page_number: the page number
        """
        url = "https://www.ilo.org/sanjose/publicaciones/lang--en/nextRow--{}/index.htm"

        return url.format(self.max_pb_per_page * page_number)
