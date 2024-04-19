import datetime
import inspect

from bs4 import BeautifulSoup

from src import CONFIG
from src.dir_fc import generate_organization_download_pdf_directory_path
from src.session import Session
from src.common import get_total_number_pages, \
    filter_list_publications_and_details, get_page_from_url, generate_document_id, add_base_url_if_missing, \
    format_language
from src.db_handler import get_total_temp_publications_urls, get_chunk_temp_publications_urls
from src.document import start_downloads, Document
from src.files_fc import LogEvent, LogLevel
from src.organizations import get_organization_by_condition

from src.time_fc import get_timestamp_from_date_and_time, timestamp_to_datetime_isoformat


class UndpGlobalScraper:
    _organization_acronym: str = "UNDP"
    _organization_region: str = "Global"
    _download_base_url = "https://www.undp.org"

    def __init__(self, session: Session):
        self.session = session
        self.organization = get_organization_by_condition(
            condition=f"acronym='{self.organization_acronym}' AND region='{self._organization_region}'")
        self.publications_page_url = self.organization.publication_urls  # self.config['TARGET_urls']['UNDP-Global']
        self.starting_page = 1
        self.total_publications_per_page = 6  # The default number of publications per page on UNDP website
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
        """
        This will execute all procedures in order to download all
        the pdf from UNDP publication website
        --------------
        IMPORTANT: By default, on UNDP publications website, the list of
                    publications is ordered from the most recent to the oldest

        --------------
        If update_mode is True, then the script will go through all pdf files
        that exist on the website, and download the ones that are missing in the relative local disk.

        If update_mode is False, then the script will start downloading the pdf files from the most recent file
        until it find some files that already exist. The condition of stopping further check is to find
        at least a certain number of consecutive files that already exist. That 'certain number' can be the
        maximum number of publication per page.
        """
        # Get total number of publications and pages from the website
        self.total_publications_online = self.get_total_number_publications()

        if self.total_publications_online is None:
            return False

        self.total_number_of_pages = get_total_number_pages(total_np=self.total_publications_online,
                                                            total_np_per_page=self.total_publications_per_page)

        if self.starting_page >= self.total_publications_per_page:
            print("Error: self.starting_page >= self.total_publications_per_page")
            print("Nothing will be returned")
            return False

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
        print(f"Retrieving publication links: 0%", end="")
        # list_publ_link = []
        for page in range(self.starting_page, self.total_number_of_pages + 1):
            # Get the url of the page
            page_ulr = self.get_page_url(page_number=page)

            # Get the current page (as a BeautifulSoup object)
            current_page_soup = get_page_from_url(url=page_ulr)

            if current_page_soup is None:
                msg = f"current_page_soup was None for the page ulr: {page_ulr}. \n " \
                      f"Check your internet connection and/or the page ulr.",
                print(msg)
                LogEvent(level=LogLevel.WARNING.value,
                         message=msg,
                         function_name=inspect.currentframe().f_code.co_name).save()
                continue

            # Get list of publications with their link on the current page
            # list_publ_link += self.get_list_of_publication_links_from_page(soup_page=current_page_soup, url=page_ulr)
            publ_links = self.get_list_of_publication_links_from_page(soup_page=current_page_soup, url=page_ulr)
            for publ_link in publ_links:
                self.session.db_handler.insert_data_into_table(
                    table_name=CONFIG["general"]["temp_publications_urls_table"],
                    data={"url": publ_link})

            print(end=f"\r Retrieving publication links: {round(100 * page / self.total_number_of_pages, 2)}% ")

        print("\r Retrieving publication links: 100%")

    def get_publication_tags_list(self, publication_page_soup: BeautifulSoup) -> str:
        publication_tag_links_list = publication_page_soup.find_all('a',
                                                                    class_=['tag-link'])

        # print(f"publication_tag_links_list: {publication_tag_links_list}")
        tags_list = ""
        if publication_tag_links_list:
            tags_list_ = [link.text for link in publication_tag_links_list]
            # print(f"tags_list: {tags_list_}")
            tags_list = "; ".join(tags_list_)

        return tags_list

    def get_publication_details(self, publication_url: str) -> list:
        """
        Return the details of a publication such as:
        Title
        publication_date
        download_link
        """
        #  UNIQUE constraint failed: documents.id. Create new id for each version
        #  Solution: Concatenate 'org acronym-region', '_', 'hashed title', '_', 'lang', '_',
        #  'hashed pdf link without .pdf'
        #  By adding .pdf at the end of document_id, we get the name of the file on the disk
        #  pdf files with same 'org acronym-region', '_', 'hashed title' part means the same publication
        results = []
        jump_to_get_any_pdf = False

        publication_title = publication_url.split("/")[-1].replace("-", " ")  # By default, the title is set to string
        # after the last '/' in the url. the '-' are replaced by single white spaces

        publication_iso_formatted_date = ""
        publication_timestamp = None

        publication_page = get_page_from_url(url=publication_url)  # Get the page where the 'Download' Button is

        if publication_page is None:
            return results

        # Get the title of the publication
        # publication_title_div = publication_page.find_all('div', class_=['coh-column', 'publication-content-wrapper'])
        publication_title_div = publication_page.find('div',
                                                      class_=['coh-inline-element column publication-card__title'])
        if publication_title_div is None:
            # If not found then collect all pdf file links
            jump_to_get_any_pdf = True

        else:
            # Get publication date
            publication_date = publication_title_div.find('h6', class_='coh-heading').text
            if publication_date is not None:
                publication_date = publication_date.strip()  # remove spaces from both sides
                try:
                    m, d, y = publication_date.split(' ')  # month_name, day, year
                    d = d.replace(',', '')  # remove the comma next to the day
                    d, y = int(d), int(y)
                    month_number = datetime.datetime.strptime(m, '%B').month  # get the integer representation of a
                    # month

                    publication_timestamp = get_timestamp_from_date_and_time(year=y, month=month_number, day=d)
                except:
                    pass
        # Format date of publication
        if publication_timestamp is not None:
            publication_iso_formatted_date = timestamp_to_datetime_isoformat(timestamp=publication_timestamp)

        if not jump_to_get_any_pdf:
            publication_title = publication_title_div.find('h2',
                                                           class_='coh-heading')
            if publication_title is not None:
                publication_title = publication_title.text.strip()  # strip to remove white
            else:
                print(f"publication_title is None,  publication_url: {publication_url}")
                jump_to_get_any_pdf = True

        # Extract publication tags/topics
        tags_list = self.get_publication_tags_list(publication_page_soup=publication_page)

        # Get download link
        pdf_download_link = self.get_pdf_download_link(download_page_soup=publication_page)

        # Check if download link does not target a pdf file
        if not jump_to_get_any_pdf and pdf_download_link.endswith(".pdf"):
            # print("if pdf_download_link[-4:]") Then the publication has only pne pdf file and its link is
            # directly available on the first page of the publication

            # Create and id for the current pdf
            document_id = generate_document_id(organization_acronym=self.organization_acronym,
                                               org_region=self.organization_region,
                                               publication_title=publication_title,
                                               pdf_download_link=pdf_download_link)

            return [Document(_id=document_id,
                             session_id=self.session.id,
                             organization_id=self.organization.id,
                             title=publication_title,
                             tags=tags_list,
                             publication_date=publication_iso_formatted_date,
                             publication_url=publication_url,
                             downloaded_at=datetime.datetime.utcnow().isoformat(),
                             pdf_link=pdf_download_link
                             )
                    ]

        # webpage targeted by the current link (pdf_download_link) Get the webpage with multiple pdfs
        # Get the list of all version of the pdfs for the current publication
        publication_pdfs_and_lang_list = self.get_list_of_publication_links_from_page_2nd_level(
            soup_page=publication_page)
        if not jump_to_get_any_pdf and len(publication_pdfs_and_lang_list):
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

        # if publication_url
        # If publication links was found using previous selections, then look for any links
        # that refers to a pdf file
        any_pdfs_list = self.get_list_of_publication_links_from_page_3rd_level(
            soup_page=publication_page)
        if len(any_pdfs_list):
            for i, pdf_ in enumerate(any_pdfs_list):
                # Create and id for the current pdf
                document_id = generate_document_id(organization_acronym=self.organization_acronym,
                                                   org_region=self.organization_region,
                                                   publication_title=publication_title,
                                                   pdf_download_link=pdf_['download_link'])

                results.append(
                    Document(_id=document_id,
                             session_id=self.session.id,
                             organization_id=self.organization.id,
                             title=f"{publication_title}",
                             tags=tags_list,
                             publication_date=publication_iso_formatted_date,
                             publication_url=publication_url,
                             downloaded_at=datetime.datetime.utcnow().isoformat(),
                             pdf_link=pdf_['download_link']
                             )
                )

        return results

    def get_list_of_publication_links_from_page_2nd_level(self, soup_page: BeautifulSoup):
        """
        This function is used when the link return by 'self.get_list_of_publication_links_from_page' does not target
        a pdf file. The pdfs are on the same page but at another location and hidden from user view in a Modal. The
        function will retrieve all download links related to only the current publication on UNDP' website and
        returns them in a list together with their respective extension referring to the language of the pdf (ENGLISH,
        FRENCH, ...)
        """
        list_links = []
        ul_list_download_list = soup_page.find('ul', class_='chapter-list download-list')

        if ul_list_download_list is None:
            return list_links

        try:
            for li in ul_list_download_list.find_all('li', class_='chapter-item download-row'):
                link = li.find('a', class_='text-link arrow-3 download-btn flex-container')
                if link is not None and link.get("href") is not None:
                    link_text = add_base_url_if_missing(base_url=self.download_base_url, url=link.get("href").strip())
                    if link_text.endswith(".pdf"):
                        lang = link.find('div').find('div').text.lower().strip().split(" ")[0]
                        list_links.append({
                            'lang': format_language(lang=lang),
                            'download_link': link_text
                        })
        except BaseException as e:
            print(e.__str__())
            return list_links

        return list_links

    def get_list_of_publication_links_from_page_3rd_level(self, soup_page: BeautifulSoup):
        """
        This method retrieve all links targeting a pdf file. It will be called when the other attempts of getting
        publication pdf links in a structured way failed. This function will just collect all links to a pdf no
        matter where they're located on a specific publication page.

        :param soup_page:
        :return:
        """
        list_links = []

        # Find all links on the page
        pdf_links = soup_page.find_all("a")

        if pdf_links is None:
            return list_links

        # Extract the link URLs from the "a" tags
        for ind, link in enumerate(pdf_links):
            if link is not None and link.get("href") is not None:
                link_text = add_base_url_if_missing(base_url=self.download_base_url, url=link.get("href").strip())
                if link_text.endswith(".pdf"):  # Only keep links ending with ".pdf"
                    list_links.append({
                        'ind': f"ind_{ind}",
                        'download_link': link_text
                    })

        return list_links

    def get_list_of_publication_links_from_page(self, soup_page: BeautifulSoup, url="") -> list:
        """
        This function retrieve the links related to each publication of
        a specific page on UNDP' website  and returns them in a list
        """
        # Get publication div container
        p_c_div = soup_page.find("div",
                                 class_="views-infinite-scroll-content-wrapper")  # the div container containing all
        if p_c_div is None:
            msg = f"No publication found on this page url: {url}"
            print(msg)
            LogEvent(level=LogLevel.ERROR.value,
                     message=msg,
                     function_name=inspect.currentframe().f_code.co_name).save()
            return []

        list_publ_link = []  # this will contain the links of each publication on the give page

        # Collect links
        for child_div in p_c_div.find_all("div", class_="content-card"):
            link = child_div.find("a")
            if link is not None and link.get("href") is not None:
                link_text = add_base_url_if_missing(base_url=self.download_base_url, url=link.get("href"))
                list_publ_link.append(link_text)

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
                    print("Warning. A pdf will be missing: Download link was not found for: ", page_url)
                else:
                    # if link found, then store it
                    for pub_d in publication_details:
                        # result.append(pub_d)
                        # Check if already in temporary documents table
                        if not pub_d.exist_in_temporary_table():
                            pub_d.insert_in_temporary_table()

                ind += 1

                print(end=f"\r Retrieving publication details: {round(100 * ind / length_publications_urls, 2)}% ")

        # return result

    def get_pdf_download_link(self, download_page_soup: BeautifulSoup) -> str:
        """
        This function retrieve the direct url to a specific pdf file on UNDP' server.
        It takes the download page as a BeautifulSoup object of only publication.
        The download url is returned as a string and will be used to download the file
        """
        a_tag = download_page_soup.find('a', class_='download', role='button')

        # Check if the <a> tag is found
        if a_tag and a_tag is not None:
            # If Download link found
            return a_tag.get("href").strip()
        else:
            return ""

    def get_total_number_publications(self):
        """
        This function makes a request to the publication page of UNDP website,
        retrieves and returns the total number of the available publications
        """
        try:
            page = get_page_from_url(url=self.publications_page_url)
        except BaseException as e:
            print(f"Error: {e.__str__()}")
            LogEvent(level=LogLevel.ERROR.value,
                     message=e.__str__(),
                     function_name=inspect.currentframe().f_code.co_name,
                     exception=e.__str__()).save()
            return None

        try:
            result = page.find('div', class_='advanced-content-results').text
        except BaseException as e:
            print(f"Error: {e.__str__()}")
            # Save the error in logs
            LogEvent(level=LogLevel.ERROR.value,
                     message=e.__str__(),
                     function_name=inspect.currentframe().f_code.co_name,
                     exception=e.__str__()).save()
            return None

        if not result:
            print(
                "The total number of publication returned an empty string: page.find('div', "
                "class_='advanced-content-results').text")
            return None

        try:
            total_publications = int(result.split(' ')[0])
        except BaseException as e:
            print(f"Error: {e.__str__()}")
            LogEvent(
                level=LogLevel.ERROR.value,
                message=e.__str__(),
                exception=e
            ).save()
            return None

        return total_publications

    def get_page_url(self, page_number: int) -> str:
        """
        This function generate the url of a page for UNDP publications based the page number
        page_number: the page number
        """
        return f"{self.publications_page_url}?combine=&sort_by=field_display_date_value&page={page_number}"
