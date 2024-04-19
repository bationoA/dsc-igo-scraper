"""This is the scraper class for the `UNICEF East Asia and Pacific`'s website. It collects all publications
and their details, and download the ones missing in the SQLite database """
import datetime
import re
from bs4 import BeautifulSoup
from src import CONFIG
from src.dir_fc import generate_organization_download_pdf_directory_path
from src.session import Session
from src.common import filter_list_publications_and_details, generate_document_id, add_base_url_if_missing, \
    get_page_from_url, format_language
from src.document import start_downloads, Document
from src.organizations import get_organization_by_condition
from src.time_fc import get_timestamp_from_date_and_time, timestamp_to_datetime_isoformat


class EastAsiaAndPacificScraper:
    _organization_acronym: str = "UNICEF"
    _organization_region: str = "East Asia and Pacific"
    _download_base_url = "https://www.unicef.org/eap/"

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
        # step 1: Get links of all publications and details
        self.get_publications_details()

        # step 2: Apply filter to the list of publications list
        filter_list_publications_and_details()

        # step 3: Download new publications
        results = start_downloads(pdf_files_directory=self.pdf_files_directory)
        self.number_of_pdfs_found_in_current_session = results['total_of_pdfs_found']
        self.number_of_downloaded_pdfs_in_current_session = results['total_of_pdfs_downloaded']

        return True

    def get_publication_details(self, publication_div: BeautifulSoup) -> list:
        """
        Return the details of a publication
        """
        results = []
        if not publication_div:
            return []

        # --- Get publication's title
        publication_title = self.get_title(p_div=publication_div)

        # --- Get Tags
        tags_list = self.get_tags(p_div=publication_div)

        # --- Publication's date
        publication_date = self.get_date(p_div=publication_div)

        # --- Get current publication's url
        publication_url = self.get_url(p_div=publication_div)

        # ---------- Get the languages anf links
        for lang_link in self.get_lang_link(p_div=publication_div):
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

    def get_publications_details(self):
        """
        This function stores the list of all publications and their corresponding details in the database
        """

        print("\r", f"Retrieving documents details (page - 1): 0", end="")

        total_retrieved_publication = 0
        last_page = False
        page = 0  # page number starts by 0 for this website

        while not last_page:
            # Get the url of the page
            page_url = self.get_api_url(page_number=page)

            # Request
            soup_page = get_page_from_url(url=page_url)

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

            # Gets publications
            publication_details_list = []
            for div in publication_divs:
                for doc in self.get_publication_details(publication_div=div):
                    publication_details_list.append(doc)

            for doc in publication_details_list:
                # Check if already in temporary documents table
                if not doc.exist_in_temporary_table():
                    doc.insert_in_temporary_table()
                    total_retrieved_publication += 1

            page += 1

            print(end=f"\r Retrieving documents details (page - {page+1}): {total_retrieved_publication} ")

    def get_api_url(self, page_number: int) -> str:
        """
        This function generate the url of a page for UN publications based the page number
        """

        return f"https://www.unicef.org/eap/research-reports?page=%2C%2C{page_number}#listAnchor"

    def get_title(self, p_div: BeautifulSoup) -> str:
        try:
            return p_div.find('h5').text.strip()
        except:
            return ""

    def get_tags(self, p_div: BeautifulSoup) -> str:
        try:
            p_tags = p_div.find('div', class_="field taxonomy_term name odd-t string")
            p_tags = p_tags.text.strip()
            return ", ".join(p_tags.split(","))
        except:
            return ""

    def get_date(self, p_div: BeautifulSoup) -> str:
        try:
            p_date = p_div.find('span', class_="list-date")
            d, m, y = p_date.text.strip().split(' ')  # day, month_name, year
            d, y = int(d), int(y)
            month_number = datetime.datetime.strptime(m, '%B').month  # get the integer representation of a month

            publication_timestamp = get_timestamp_from_date_and_time(year=y, month=month_number, day=d)
            return timestamp_to_datetime_isoformat(timestamp=publication_timestamp)
        except:
            return ""

    def get_url(self, p_div: BeautifulSoup) -> str:
        try:
            p_ulr = p_div.find('div', class_="list-content").find('a')
            return add_base_url_if_missing(base_url=self.download_base_url, url=p_ulr.get('href').strip())
        except:
            return ""

    def get_pdf_link_lang_from_url(self, url: str) -> tuple:
        """
        This function is called when a document's link was not found for a publication on the main page containing
        the list of the publications.
        It will navigate to the url of the publication and collect the first pdf link found on that page and its
        language

        :return:
        """
        link = ""
        lang = ""
        # Open the url of the publication and try to get the lang and link
        if url:
            soup_page = get_page_from_url(url=url)
            # Find all <a> elements with href attributes pointing to PDFs
            if soup_page:
                a_tag = soup_page.find_all('a', href=re.compile(r'\.pdf$', re.IGNORECASE))
                a_tag = [a for a in a_tag if "Download the report" in a.text]
                if len(a_tag):
                    a_tag = a_tag[0]
                    link = a_tag.get('href')
                    title_ = a_tag.get('title')
                    if len(title_):
                        lang = title_.split("|")[-1].strip()

        return link, format_language(lang=lang)

    def get_lang_link(self, p_div: BeautifulSoup) -> list:
        lang_link_list = []
        try:
            files_divs = p_div.find_all('div', class_="file-item")
            link, lang = "", ""
            if not len(files_divs):
                p_url = self.get_url(p_div=p_div)
                if any([p_url.lower().endswith("." + ext.lower()) for ext in CONFIG["general"]["file_types"]]):
                    link = p_url
                    lang = [lg for lg in CONFIG["general"]["un_languages"]["lang_dict"].values()]
                    lang = lang[0] if len(lang) else ""

                else:
                    link, lang = self.get_pdf_link_lang_from_url(url=p_url)

                if link:
                    lang_link_list.append({
                        "lang": lang,
                        "link": link
                    })
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
