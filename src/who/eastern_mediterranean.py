import datetime
import inspect
import re

from bs4 import BeautifulSoup

from src import Session, CONFIG
from src.common import filter_list_publications_and_details, generate_document_id, is_valid_url, get_page_from_url, \
    format_language
from src.db_handler import get_total_temp_publications_urls, get_chunk_temp_publications_urls
from src.dir_fc import generate_organization_download_pdf_directory_path
from src.document import start_downloads, Document
from src.files_fc import LogEvent, LogLevel
from src.organizations import get_organization_by_condition

from src.time_fc import get_timestamp_from_date_and_time, timestamp_to_datetime_isoformat


class WhoEasternMediterraneanScraper:
    _organization_acronym: str = "WHO"
    _organization_region: str = "Eastern Mediterranean"

    def __init__(self, session: Session):
        self.session = session
        self.organization = get_organization_by_condition(
            condition=f"acronym='{self.organization_acronym}' AND region='{self._organization_region}'")
        self.n_pub_page = 100  # Number of publications to get per page
        self.request_timeout = 15 * 60  # It's set it to 15 minutes because the sever for this regional website is slow
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

    def run(self):
        # step 1: Get links of all publications
        self.get_all_publications_links()

        # step 2: Get details togethers with the download links of each publication on the current page
        self.get_publications_details_from_urls()

        # # step 2: Apply filter to the list of publications list
        filter_list_publications_and_details()

        # step 3: Download new publications
        results = start_downloads(pdf_files_directory=self.pdf_files_directory)
        self.number_of_pdfs_found_in_current_session = results['total_of_pdfs_found']
        self.number_of_downloaded_pdfs_in_current_session = results['total_of_pdfs_downloaded']

        return True

    def get_page_url(self, page_number: int) -> str:
        """
        This function generate the url of a publications' page based the page number
        page_number: the page number
        """

        url = "https://vlibrary.emro.who.int/searchd/page/{}/?skeyword&journal_title&fauthor_title%5B0%5D&mesh_title" \
              "%5B0%5D&relation&index_option&format=summary&sort=PublicationDate&perpage={}&adv&database&year_from" \
              "&year_to&typepublication%5B0%5D=Annual+Reports&typepublication%5B1%5D=Publications&records#038" \
              ";journal_title&fauthor_title%5B0%5D&mesh_title%5B0%5D&relation&index_option&format=summary&sort" \
              "=PublicationDate&perpage=10&adv&database&year_from&year_to&typepublication%5B0%5D=Annual+Reports" \
              "&typepublication%5B1%5D=Publications&records "

        return url.format(page_number, self.n_pub_page)

    def get_all_publications_links(self):
        """
        This method retrieve the url of all the existing publications
        :return:
        """
        # For each page,
        print(" NOTE: WHO-Eastern-Mediterranean's server is quite slow. So, the request timeout is set to 7 minutes.")
        print(f" Retrieving publications: 0", end="")
        nbr_retrieved_publications = 0  # Number of retrieved publications

        last_page = False
        page = 0  # 0 is the first page
        nbr_none = 0  # Number of consecutive None values for `current_page_soup`
        max_nbr_none = 3  # Maximum number of consecutive None values for `current_page_soup` before exiting the loop
        while not last_page:
            if nbr_none >= max_nbr_none:
                break
            # Get the url of the page
            page_ulr = self.get_page_url(page_number=page)

            # Get the current page (as a BeautifulSoup object)
            current_page_soup = get_page_from_url(url=page_ulr, timeout=self.request_timeout)

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
                nbr_none = 0  # reset the counter of the Number of consecutive None values for `current_page_soup`
            # ---Check if the current page is the last one
            # Find all span elements with class "search_btn"
            search_btn_spans = current_page_soup.find_all('span', class_='search_btn')
            # Check if any of the spans have the text "Next"
            next_exists = any(span.text.strip().lower() == "next" for span in search_btn_spans)

            if not next_exists:
                last_page = True
            # ---

            # Get list of publications with their link on the current page\
            publ_links = self.get_list_of_publication_links_from_page(soup_page=current_page_soup)

            nbr_retrieved_publications += len(publ_links)

            for publ_link in publ_links:
                self.session.db_handler.insert_data_into_table(
                    table_name=CONFIG["general"]["temp_publications_urls_table"],
                    data={"url": publ_link})

            print(end=f"\r Retrieving publications: {nbr_retrieved_publications}")

            page += 1

        print("")

    def get_list_of_publication_links_from_page(self, soup_page: BeautifulSoup) -> list:
        """
        This function retrieve the links related to each publication of a specific page and returns them in a list
        """
        # Collect links
        list_publ_link = []
        for link in soup_page.find_all("a", class_="recordtitle"):
            if link is not None and link.get("href") is not None:
                list_publ_link.append(link.get("href"))

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
            # print(f"result_publications_urls: {result_publications_urls}")
            publications_urls = [purl['url'] for purl in result_publications_urls]
            last_url = result_publications_urls[-1]
            start_id = last_url['id'] + 1
            # print(f"start_id: {start_id}")

            for page_url in publications_urls:
                publication_details = self.get_publication_details(publication_url=page_url)  # Get the download link
                if not publication_details:
                    print("Warning. A document will be missing: Download link was not found for: ", page_url)
                else:
                    # if link found, then store it
                    for pub_d in publication_details:
                        # result.append(pub_d)
                        # Check if already in temporary documents table
                        if not pub_d.exist_in_temporary_table():
                            pub_d.insert_in_temporary_table()

                ind += 1

                print(end=f"\r Retrieving publication details: {round(100 * ind / length_publications_urls, 2)}% ")

    def get_publication_details(self, publication_url: str) -> list:
        """
        Return the details of a publication such as:
        Title
        publication_date
        download_link
        """
        results = []

        publication_page = get_page_from_url(url=publication_url)  # Get the page where the 'Download' Button is

        if publication_page is None:
            return results

        # ---- Get the title of the publication
        publication_title = publication_page.find('h6').text if publication_page.find('h6').text else ""

        # ----- Get publication date
        publication_iso_formatted_date = ""
        # Find all <div> elements with class="col-md-12"
        div_elements = publication_page.find_all('div', class_='col-md-12')

        # Loop through the <div> elements to find the date
        dates = None
        for div in div_elements:
            text = div.get_text()
            # Define a regular expression pattern to match the date format "Month, Year"
            date_pattern = r"[A-Z][a-z]+,\s*\d{4}"
            # Find all occurrences of the date pattern in the text
            dates = re.findall(date_pattern, text)

            if dates:  # If dates are found
                break

        publication_timestamp = None
        if dates and dates is not None:
            try:
                dates = dates[0].split(",")
                m, y = dates[0].strip(), int(dates[1].strip())  # month_name, year
                month_number = datetime.datetime.strptime(m, '%B').month  # get the integer representation of a
                # month
                # This website does not specify the days in the publication's dates, therefore we'll set it to 1
                d = 1
                publication_timestamp = get_timestamp_from_date_and_time(year=y, month=month_number, day=d)
            except:
                pass

        # Format date of publication
        if publication_timestamp is not None:
            publication_iso_formatted_date = timestamp_to_datetime_isoformat(timestamp=publication_timestamp)

        # ----- Extract publication tags/topics
        tags_list = ""
        try:
            # Find the element with the 'b' tag containing the text 'Broad Subjects :'
            broad_subjects_tag = publication_page.find('b', text='Broad Subjects :')

            # Get the next sibling element, which contains the text corresponding to tags
            text_after_broad_subjects = broad_subjects_tag.nextSibling.strip()
            tags_list = text_after_broad_subjects.split(",")
            tags_list = [tag for tag in tags_list if len(tag)]  # Remove empty tags
            tags_list = ", ".join(tags_list)
        except:
            pass

        # ----- Get download link
        # Get the list of all version of the pdfs for the current publication
        publication_pdfs_and_lang_list = self.get_list_of_documents_links_from_page(
            soup_page=publication_page)
        if len(publication_pdfs_and_lang_list):
            for i, publi in enumerate(publication_pdfs_and_lang_list):
                # Create and id for the current pdf
                document_id = generate_document_id(organization_acronym=self.organization_acronym,
                                                   org_region=self.organization_region,
                                                   publication_title=publication_title,
                                                   pdf_download_link=publi['download_link'])

                results.append(
                    Document(_id=document_id,
                             session_id=self.session.id,
                             organization_id=self.organization.id,
                             title=publication_title,
                             tags=tags_list,
                             publication_date=publication_iso_formatted_date,
                             publication_url=publication_url,
                             downloaded_at=datetime.datetime.utcnow().isoformat(),
                             pdf_link=publi['download_link'],
                             lang=publi['lang']
                             )
                )

        return results

    def get_list_of_documents_links_from_page(self, soup_page: BeautifulSoup):
        """
        This function returns PDFs links in a list together with their language
        """
        list_links = []
        a_tags_download_list = soup_page.find_all('a', class_='btn-info')

        if a_tags_download_list is None or not len(a_tags_download_list):
            return list_links

        try:
            for a_tag in a_tags_download_list:
                link = a_tag.get("href")
                lang = ""
                if link is not None:
                    # --- Get PDFs' language
                    # Get list of classes in the `a tag`
                    class_list = a_tag.get('class', [])
                    if class_list is not None and len(class_list):
                        # Get the last class
                        btn_lang = class_list[-1]
                        if "btn_" in btn_lang:
                            # Get language abbreviation
                            lang = btn_lang.removeprefix("btn_")

                    list_links.append({
                        'lang': format_language(lang=lang),
                        'download_link': link
                    })
        except BaseException as e:
            print(e.__str__())
            return list_links

        return list_links

    def extract_required_details_from_publication(self, publication: dict) -> Document:
        """
        This function take a publication that came directly from the API, and it extracts only the values that we're
        interested in and return as a Document.
        :param publication:
        :return:
        """

        publication_title = publication["TrimmedTitle"]
        download_link = publication["DownloadUrl"].strip()

        # Create and id for the current pdf
        document_id = generate_document_id(organization_acronym=self.organization_acronym,
                                           org_region=self.organization_region,
                                           publication_title=publication_title,
                                           pdf_download_link=download_link)
        # document_id = publication["Id"]
        p_link = publication["Links"]
        if p_link != "" and p_link is not None:

            # Parse the HTML with BeautifulSoup
            soup = BeautifulSoup(p_link, 'html.parser')

            try:
                # Find the first 'a' tag and get the 'href' attribute
                p_link = soup.find('a')['href']
            except:
                pass

        if not is_valid_url(p_link):
            p_link = ""

        publication_timestamp = ""
        try:
            d, m, y = publication["PublicationDateAndTime"].split(' ')  # month_name, day, year
            d = d.replace(',', '')  # remove the comma next to the day
            d, y = int(d), int(y)
            month_number = datetime.datetime.strptime(m, '%B').month  # get the integer representation of a
            # month
            publication_timestamp = get_timestamp_from_date_and_time(year=y, month=month_number, day=d)
        except:
            pass

        return Document(_id=document_id,
                        session_id=self.session.id,
                        organization_id=self.organization.id,
                        tags=publication["Tag"],
                        publication_date=publication_timestamp,
                        publication_url=p_link,
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
