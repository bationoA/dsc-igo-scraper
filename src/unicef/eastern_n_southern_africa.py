"""This is the scraper class for the `UNICEF Eastern and Southern Africa`'s website. It collects all publications
and their details, and download the ones missing in the SQLite database """
import datetime
import inspect
import re
from bs4 import BeautifulSoup
from src import CONFIG
from src.db_handler import get_total_temp_publications_urls, get_chunk_temp_publications_urls
from src.dir_fc import generate_organization_download_pdf_directory_path
from src.files_fc import LogEvent, LogLevel
from src.session import Session
from src.common import filter_list_publications_and_details, generate_document_id, add_base_url_if_missing, \
    get_page_from_url, format_language
from src.document import start_downloads, Document
from src.organizations import get_organization_by_condition
from selenium import webdriver  # for simulating user action such as a click on a button
from selenium.webdriver.chrome.options import Options  # Options while setting up the webdriver with chrome
from src.time_fc import get_timestamp_from_date_and_time, timestamp_to_datetime_isoformat


class EasternAndSouthernAfricaScraper:
    _organization_acronym: str = "UNICEF"
    _organization_region: str = "Eastern and Southern Africa"
    _download_base_url = "https://www.unicef.org/esa/"

    def __init__(self, session: Session):
        self.session = session
        self.organization = get_organization_by_condition(
            condition=f"acronym='{self.organization_acronym}' AND region='{self._organization_region}'")
        self.publications_page_url = self.organization.publication_urls
        self.max_pb_per_page = 12  # The maximum number of publications per page is 12 for this website
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
        print("\r", f"Retrieving publications links (page - 1): 0", end="")

        total_retrieved_publication = 0
        last_page = False
        page = 0  # page number starts by 0 for this website

        while not last_page:
            # Get the url of the page
            page_ulr = self.get_page_url(page_number=page)

            # Request
            soup_page = get_page_from_url(url=page_ulr)

            if not soup_page:
                break

            try:
                publication_divs = soup_page.find_all('div', class_="list-wrapper grey-lighter-bc")
            except:
                publication_divs = []

            nbr_publications = len(publication_divs)

            if not nbr_publications:
                break

            if nbr_publications < self.max_pb_per_page:
                last_page = True

            # Get list of publications with their link on the current page\
            publ_links = self.get_list_of_publication_links_from_page(publication_divs_list=publication_divs,
                                                                      url=page_ulr)

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
                    total_retrieved_publication += 1
            page += 1

            print(end=f"\r Retrieving publications links (page - {page}): {total_retrieved_publication} ")

        print("")

    def get_list_of_publication_links_from_page(self, publication_divs_list: list, url: str) -> list:
        """
        This function retrieve the links related to each publication of a specific page
        and returns them in a list
        """

        if not len(publication_divs_list):
            msg = f"No publication found on this page url: {url}"
            print(msg)
            LogEvent(level=LogLevel.ERROR.value,
                     message=msg,
                     function_name=inspect.currentframe().f_code.co_name).save()
            return []

        list_publ_link = []
        # Collect links
        for child_div in publication_divs_list:
            # Get publication's link
            p_link = ""

            try:
                p_link_a = child_div.find('a', class_="list-hero-link grey_darker")
                # Find the 'a' tag and get the 'href' attribute
                p_link = p_link_a.get('href').strip()
                if len(p_link):
                    p_link = add_base_url_if_missing(base_url=self.download_base_url, url=p_link)
            except:
                pass

            if p_link:
                list_publ_link.append(p_link)

        return list_publ_link

    def get_publication_details(self, publication_url: str) -> list:
        """
        Return the details of a publication
        """

        # Get publication's page
        p_page = get_page_from_url(url=publication_url)

        if not p_page:
            return []

        # get article's block
        article_block = p_page.find('article', class_="cview-full ctype-publication")

        if not article_block:
            article_block = p_page.find('article', class_="cview-full ctype-document")

            if not article_block:
                article_block = p_page.find('article', class_="cview-full ctype-page")

                if not article_block:
                    return []

        results = []

        # --- Get publication's title
        publication_title = self.get_title(soup_block=article_block)

        # --- Get Tags
        tags_list = self.get_tags(soup_block=article_block)

        # --- Publication's date
        publication_date = self.get_date(soup_block=article_block)

        # ---------- Get the languages anf download links
        for lang_link in self.get_lang_link(soup_block=article_block):
            # --- Get download link
            link = lang_link['link']

            if not link:  # Skip if no link was found
                continue

            # --- Get current file's language
            lang = lang_link['lang']

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
        This function generate the url of a page for UN publications based the page number
        """

        return f"https://www.unicef.org/esa/reports/publications?page={page_number}#in-page-search"

    def get_title(self, soup_block: BeautifulSoup) -> str:
        try:
            return soup_block.find('h1', class_="h1").text.strip()
        except:
            return ""

    def get_tags(self, soup_block: BeautifulSoup) -> str:
        try:
            tags_divs = soup_block.find_all('div', class_="content-list-item grey-dark-bg grey-bg-focus")
            tags_list = [div.find('a').text.strip() for div in tags_divs if div.find('a')]
            return "; ".join(tags_list)
        except:
            return ""

    def get_date(self, soup_block: BeautifulSoup) -> str:
        try:
            return soup_block.find('time').get('datetime')
        except:
            return ""

    def get_lang_link(self, soup_block: BeautifulSoup) -> list:
        lang_link_list = []
        try:
            files_divs = soup_block.find_all('div', class_="file-item")
            link, lang = "", ""
            if not len(files_divs):
                p_url = self.get_doc_link(soup_page=soup_block)
                if any([p_url.lower().endswith("." + ext.lower()) for ext in CONFIG["general"]["file_types"]]):
                    link = p_url
                    lang = [lg for lg in CONFIG["general"]["un_languages"]["lang_dict"].values()]
                    lang = lang[0] if len(lang) else ""

                if link:
                    lang_link_list.append({
                        "lang": lang,
                        "link": link
                    })

            else:
                for seln in files_divs:
                    a_tag = seln.find('a')
                    if a_tag:
                        # Get the "href" attribute of the element
                        href_value = a_tag.get("href")

                        if href_value:
                            href_value = add_base_url_if_missing(base_url=self.download_base_url, url=href_value)
                            file_name_span = seln.find('span', class_="file-name")
                            # Get the "href" attribute of the element
                            file_name = file_name_span.text
                            lang = [lg for lg in set(CONFIG['general']['un_languages']['lang_dict'].values())
                                    if lg.lower() in file_name.lower()]
                            lang = lang[0] if lang else ""
                            lang_link_list.append({
                                "lang": lang,
                                "link": href_value
                            })
        except:
            return []

        return lang_link_list

    def get_doc_link(self, soup_page: BeautifulSoup) -> str:
        """
        Return the first link pointing to a document (supported type) in a web page
        :param soup_page:
        :return:
        """
        try:
            links = soup_page.find_all('a')
            link = [lk.get('href') for lk in links if lk.get('href') and lk.get('href').lower().endswith(".pdf")][0]
            return add_base_url_if_missing(base_url=self.download_base_url, url=link.strip())
        except:
            return ""

