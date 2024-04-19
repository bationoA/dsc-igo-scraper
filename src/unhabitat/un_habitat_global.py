"""This is the scraper class for the ` UN-Habitat's Global` website. It collects all publications
and their details, and download the ones missing in the SQLite database """
import datetime
import inspect
import urllib3
from bs4 import BeautifulSoup
from src import CONFIG
from src.dir_fc import generate_organization_download_pdf_directory_path
from src.session import Session
from src.common import filter_list_publications_and_details, get_page_from_url, generate_document_id, \
    add_base_url_if_missing, clean_text, get_lan_from_text
from src.document import start_downloads, Document
from src.files_fc import LogEvent, LogLevel
from src.organizations import get_organization_by_condition
from src.time_fc import get_timestamp_from_date_and_time, timestamp_to_datetime_isoformat

# Disable the InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class UnHabitatGlobalScraper:
    _organization_acronym: str = "UNHabitat"
    _organization_region: str = "Global"
    _download_base_url = "https://unhabitat.org"

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
        # step 1: Get links of all publications and their details
        self.get_publications_and_details()

        # step 3: Apply filter to the list of publications list
        filter_list_publications_and_details()

        # step 4: Download new publications
        results = start_downloads(pdf_files_directory=self.pdf_files_directory)
        self.number_of_pdfs_found_in_current_session = results['total_of_pdfs_found']
        self.number_of_downloaded_pdfs_in_current_session = results['total_of_pdfs_downloaded']

        return True

    def get_publications_and_details(self):
        """
        This method retrieve the links of all the existing publications and their details
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
            publication_divs = self.get_pub_divs_list(soup=current_page_soup)
            if not publication_divs:
                break

            # If first iteration then update the maximum number of publication on a page using the info from the
            # first page. Just in case that value changes after an update on the website
            if page == self.starting_page:
                self.max_pb_per_page = len(publication_divs)
            # --- Check if the current page is the last one
            # If the number of publications on the current page is less than the maximum number of publications that
            # supposed to be on one page, then there is no `next page`. We are on the last page
            if len(publication_divs) < self.max_pb_per_page:
                last_page = True
            # ---

            for publication_div in publication_divs:
                # Get list of publications with their details on the current page
                publication_details = self.get_publication_details(
                    publication_div=publication_div)  # Get the download link
                if not publication_details:
                    page_url = self.get_publication_url_from_pub_div(publication_div=publication_div)
                    print("Warning. A pdf will be missing: Download link was not found for: ", page_url)
                else:
                    # if link found, then store it
                    for pub_d in publication_details:
                        # Check if already in temporary documents table
                        if not pub_d.exist_in_temporary_table():
                            pub_d.insert_in_temporary_table()
                            nbr_retrieved_publications += 1

            print(end=f"\r Retrieving publications: {nbr_retrieved_publications}")

            page += 1
        print("")

    def get_pub_divs_list(self, soup: BeautifulSoup) -> list:
        """"
        Return a list of publications divs
        """
        results = []
        publication_tab = soup.find("div", class_="tab-content container")

        for i, div in enumerate(publication_tab.find_all("div", class_="views-row")):
            try:
                if "Learn more" in div.find("div", class_="col-md-8").text:
                    results.append(div.find("div", class_="col-md-8"))
            except:
                pass

        return results

    def get_publication_tags_list(self, publication_page_soup: BeautifulSoup) -> str:
        publication_tag_links_list = publication_page_soup.find_all('a', class_=['tag-link'])

        tags_list = ""
        if publication_tag_links_list:
            tags_list_ = [link.text for link in publication_tag_links_list]
            tags_list = "; ".join(tags_list_)

        return tags_list

    def get_publication_details(self, publication_div: BeautifulSoup) -> list:
        """
        Return the details of a publication
        """
        results = []
        publication_url = self.get_publication_url_from_pub_div(publication_div=publication_div)

        # --- Get publication's title
        publication_title = self.get_title(soup_page=publication_div)
        publication_title = publication_url if publication_title is None else publication_title

        # --- Get Tags
        tags_list = self.get_tags(soup_page=publication_div)

        # --- Publication's date
        publication_date = self.get_publication_date(soup_page=publication_div)

        # -- Get versions languages and file urls
        pub_versions_links_n_lang = self.get_links_n_lang_from_page(soup_page=publication_div)

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

    def get_publication_url_from_pub_div(self, publication_div: BeautifulSoup) -> str:
        try:
            url = publication_div.find("a", class_="btn btn-lg btn-light").get("href")
            return add_base_url_if_missing(base_url=self.download_base_url,
                                           url=url)
        except:
            return ""

    def get_title(self, soup_page: BeautifulSoup) -> str:
        try:
            return clean_text(soup_page.find("h5").text)
        except:
            return ""

    def get_tags(self, soup_page: BeautifulSoup) -> str:
        tags_list = []
        try:
            tags_container = soup_page.find("div", class_="knowledge-type mb-3").text
            tags_list = [clean_text(tag) for tag in tags_container.split(",")]
        except:
            pass

        tags_list = "; ".join(tags_list) if len(tags_list) else ""

        return tags_list

    def get_publication_date(self, soup_page: BeautifulSoup) -> str:
        try:
            p_date_year = soup_page.find("div", class_="knowledge-year").text
            p_date_year = int(p_date_year)
            pub_timestamp = get_timestamp_from_date_and_time(year=p_date_year, month=1, day=1)

            return timestamp_to_datetime_isoformat(timestamp=pub_timestamp)
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

        # Extract 'a' tags ending with one of the supported file types
        a_tag_list = [atag for atag in all_links
                      if atag.get("href") and
                      any([atag.get("href").endswith(suff_) for suff_ in CONFIG["general"]["file_types"]])
                      ]

        if not len(a_tag_list):
            # If no links pointing to a supported file were found, then open the publication page and try to find the
            # links there
            publication_url = self.get_publication_url_from_pub_div(publication_div=soup_page)
            # Get publication's page
            pub_page = get_page_from_url(url=publication_url,
                                         ssl_verify=False,
                                         max_attempt=CONFIG["general"]["max_request_attempt"],
                                         max_waiting_time_sec=CONFIG["general"]["max_waiting_time_sec"]  # 15 minutes
                                         )

            if not pub_page:  # No page related to the publication's url were returned
                return []

            # Find all links on the page
            all_links = pub_page.find_all("a")

            if all_links is None:
                return []

            # Extract 'a' tags ending with one of the supported file types
            a_tag_list = [atag for atag in all_links
                          if atag.get("href") and
                          any([atag.get("href").endswith(suff_) for suff_ in CONFIG["general"]["file_types"]])
                          ]
            if not len(a_tag_list):
                return []

        # Extract the link langs and links
        list_links = [
            {
                'lang': get_lan_from_text(text_=atag.get("href")),
                'url': add_base_url_if_missing(base_url=self.download_base_url, url=atag.get("href"))
            }
            for atag in a_tag_list
        ]

        return list_links

    # def get_publications_details_from_urls(self):
    #     """
    #     This function takes a list of publication links and
    #     returns a list of their corresponding download url
    #     """
    #     # length_publications_urls = len(publications_urls)
    #     length_publications_urls = get_total_temp_publications_urls()
    #     print("\r", f"Retrieving publication details: 0%", end="")
    #
    #     # Retrieve publications by chunks
    #     chunk_size = CONFIG["general"]["max_publication_urls_chunk_size"]
    #     chunk_total = length_publications_urls / chunk_size
    #     chunk_total = int(chunk_total) + 1 if int(chunk_total) < chunk_total else int(chunk_total)
    #
    #     start_id = 0
    #     ind = 0
    #     for i in range(chunk_total):
    #         result_publications_urls = get_chunk_temp_publications_urls(from_id=start_id,
    #                                                                     limit=chunk_size)
    #
    #         publications_urls = [purl['url'] for purl in result_publications_urls]
    #         last_url = result_publications_urls[-1]
    #         start_id = last_url['id'] + 1
    #
    #         for page_url in publications_urls:
    #             publication_details = self.get_publication_details(
    #                 publication_url=page_url)  # Get the download link
    #             if not publication_details:
    #                 print("Warning. A pdf will be missing: Download link was not found for: ", page_url)
    #             else:
    #                 # if link found, then store it
    #                 for pub_d in publication_details:
    #                     # Check if already in temporary documents table
    #                     if not pub_d.exist_in_temporary_table():
    #                         pub_d.insert_in_temporary_table()
    #
    #             ind += 1
    #
    #             print(end=f"\r Retrieving publication details: {round(100 * ind / length_publications_urls, 2)}% ")

    def get_page_url(self, page_number: int) -> str:
        """
        This function generate the url of a page based on the page number
        page_number: the page number
        """
        url = "https://unhabitat.org/knowledge/research-and-publications?page={}"

        return url.format(page_number)
