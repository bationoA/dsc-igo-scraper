"""This is the scraper class for the ` UNWTO's Global` website. It collects all publications
and their details, and download the ones missing in the SQLite database """

# TODO: This scraper actual works but the IP address will be flagged and blocked from accessing the server. If this
#  issue is solved by the UNWTO in the future, we will just need to follow the instructions `Uncomment if restriction
#  removed` in the `run` method.

import datetime
import inspect
import requests
import urllib3
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options  # Options while setting up the webdriver with chrome
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from src import CONFIG
from src.dir_fc import generate_organization_download_pdf_directory_path
from src.session import Session
from src.common import filter_list_publications_and_details, get_page_from_url, generate_document_id, \
    add_base_url_if_missing, clean_text, selenium_get_page_from_url, get_lan_from_text
from src.db_handler import get_total_temp_publications_urls, get_chunk_temp_publications_urls
from src.document import start_downloads, Document
from src.files_fc import LogEvent, LogLevel
from src.organizations import get_organization_by_condition
from src.time_fc import get_timestamp_from_date_and_time, timestamp_to_datetime_isoformat

# Disable the InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class UnwtoGlobalScraper:
    _organization_acronym: str = "UNWTO"
    _organization_region: str = "Global"
    _download_base_url = "https://www.e-unwto.org"

    def __init__(self, session: Session):
        self.session = session
        self.organization = get_organization_by_condition(
            condition=f"acronym='{self.organization_acronym}' AND region='{self._organization_region}'")
        self.publications_page_url = self.organization.publication_urls
        self.starting_page = 0
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

        # TODO: This scraper actual works but the IP address will be flagged and blocked from accessing the server.
        #  If this issue is solved by the `UNWTO` in the future, we just need to follow the instructions `Uncomment
        #  if restriction removed`
        forbidden_text = "\nThis scraper actual works but the IP address will be flagged and blocked from \n" \
                         "accessing the server. If this issue is solved by `https://www.e-unwto.org/` in the \n" \
                         "future, we will just need to follow the instructions `Uncomment if restriction removed` \n" \
                         "in the method called `run` of the scraper class `UnwtoGlobalScraper` and then run the \n" \
                         "scraping program."
        warning_color = '\033[93m'
        reset_color = '\033[0m'
        print(f"{warning_color}{forbidden_text}{reset_color}")

        # step 1: Get links of all publications
        # self.get_all_publications_links()  # TODO: Uncomment if restriction removed

        # step 2: Get details togethers with the download links of each publication on the current page
        # self.get_publications_details_from_urls()  # TODO: Uncomment if restriction removed

        # step 3: Apply filter to the list of publications list
        # filter_list_publications_and_details()  # TODO: Uncomment if restriction removed

        # step 4: Download new publications
        # TODO: Uncomment if restriction removed
        # results = start_downloads(pdf_files_directory=self.pdf_files_directory)
        # TODO: Uncomment if restriction removed
        # self.number_of_pdfs_found_in_current_session = results['total_of_pdfs_found']
        # TODO: Uncomment if restriction removed
        # self.number_of_downloaded_pdfs_in_current_session = results['total_of_pdfs_downloaded']

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
        total_pages = 0  # the total number of pages that will be accessed
        while not last_page:
            if nbr_none >= max_nbr_none:
                break
            # Get the url of the page
            page_ulr = self.get_page_url(page_number=page)

            # ## Get the current page (as a BeautifulSoup object)
            headers = [{
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/58.0.3029.110 Safari/537.3"}]
            # Wait until a form of id 'browsePublicationsForm' is present on the page
            wait_el_loc_xpath = (By.XPATH, "//form[@id='browsePublicationsForm']")
            current_page_soup = selenium_get_page_from_url(url=page_ulr,
                                                           headers=headers,
                                                           wait_element_located_xpath=wait_el_loc_xpath)

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

            if page == self.starting_page:
                total_pages = self.get_total_pages(soup=current_page_soup)

            # Check if there are some publications on the current page
            publication_urls = self.get_publications_urls_list_from_page(soup=current_page_soup)
            if not publication_urls:
                break

            # --- Check if the current page is the last one
            if page - 1 >= total_pages:
                last_page = True
            # ---

            # Add base url to each publication's url where missing
            publ_links = [add_base_url_if_missing(base_url=self.download_base_url, url=url)
                          for url in publication_urls]

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
            break
        print("")

    def get_total_pages(self, soup: BeautifulSoup) -> int:
        """
        Return the total number of pages that will be accessed
        :param soup:
        :return:
        """

        return int(soup.find("a", class_="lastPage").text)

    def get_publications_urls_list_from_page(self, soup: BeautifulSoup) -> list:
        """
        Return the list of all publication's urls with full access present on a specific page.
        `Full access` means they can be downloaded without requiring logging in.
        :param soup:
        :return: list
        """
        results_form = soup.find_all("form")[4]
        if not results_form:
            return []

        tab_rows_list = results_form.find_all("tr")
        if not tab_rows_list:
            return []
        # Filter the rows to keep only the publications with full access
        full_access_rows = [tr for tr in tab_rows_list if tr.find("img") and
                            "fullAccess" in tr.find("img").get("class")]
        if not full_access_rows:
            return []

        # Retrieve publication's urls
        results = [tr.find("a").get("href") for tr in full_access_rows if tr.find("a") and tr.find("a").get("href")]
        if not results:
            return []

        return results

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
        headers = [{
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/58.0.3029.110 Safari/537.3"}]
        # Wait until a div of class 'tocListWidgetContainer' is present on the page
        wait_el_loc_xpath = (By.XPATH, "//div[@class='tocListWidgetContainer']")
        pub_page = selenium_get_page_from_url(url=publication_url,
                                              headers=headers,
                                              wait_element_located_xpath=wait_el_loc_xpath)

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
            return clean_text(soup_page.find("h3", class_="tocListHeader").text)
        except:
            return ""

    def get_tags(self, soup_page: BeautifulSoup) -> str:
        tags_list = []
        try:
            tags_container = soup_page.find("div", class_="keywordContainer")
            tags_list = [clean_text(a_tag.text) for a_tag in tags_container.find_all("a")]
        except:
            pass

        tags_list = "; ".join(tags_list) if len(tags_list) else ""

        return tags_list

    def get_publication_date(self, soup_page: BeautifulSoup) -> str:
        """
        Extract and format publication date
        :param soup_page:
        :return:
        """
        try:
            date_and_nbr_page_div = soup_page.find("div", class_="PublishDateAndPagesCount")

            # Find the <strong> tag with the text "Published: "
            published_tag = date_and_nbr_page_div.find("strong", text="Published: ")

            # Get the next sibling, which should be the date
            date = published_tag.next_sibling.text.strip().replace("\xa0", " ")
            date = date.split(" ")
            d, m, y = 1, None, None  # The day is not provided in the dates, So we'll the `1` as the day of the month
            if len(date) == 2:
                m, y = date
                m = clean_text(text=m)
            else:
                m, y = date[0], "January"  # If month is not provided then use `January`

            y = int(y)
            month_number = datetime.datetime.strptime(m, '%B').month  # get the integer representation of a
            publication_timestamp = get_timestamp_from_date_and_time(year=y, month=month_number, day=d)
            publication_date = timestamp_to_datetime_isoformat(timestamp=publication_timestamp)

            return publication_date if publication_date else ""
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

        # Extract 'a' tags ending with the string "download" or "download/"
        a_tag_list = [atag for atag in all_links
                      if atag.get("href") and
                      any([doc_type in atag.get("href") for doc_type in CONFIG["general"]["file_types"]])
                      ]
        if not len(a_tag_list):
            return []

        # Extract the link langs and links
        list_links = [
            {
                'lang': get_lan_from_text(text_=clean_text(atag.text)) if clean_text(atag.text) else "",
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
        url = "https://www.e-unwto.org/action/showPublications?startPage={}&target=browse&pageSize={}"

        return url.format(page_number, self.max_pb_per_page)
