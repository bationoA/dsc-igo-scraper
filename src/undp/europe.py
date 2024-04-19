import datetime
import time

from bs4 import BeautifulSoup

from src import CONFIG
from src.dir_fc import generate_organization_download_pdf_directory_path
from src.session import Session
from src.common import filter_list_publications_and_details, get_page_from_url, generate_document_id, \
    add_base_url_if_missing, format_language
from src.db_handler import get_total_temp_publications_urls, get_chunk_temp_publications_urls
from src.document import start_downloads, Document
from src.organizations import get_organization_by_condition

from selenium import webdriver  # for simulating user action such as a click on a button
from selenium.webdriver.chrome.options import Options  # Options while setting up the webdriver with chrome
from selenium.webdriver.common.by import By

from src.time_fc import get_timestamp_from_date_and_time, timestamp_to_datetime_isoformat


class UndpEuropeAndTheCommonwealthOfIndependentStatesScraper:
    _organization_acronym: str = "UNDP"
    _organization_region: str = "Europe and the Commonwealth of Independent States"
    _download_base_url = "https://www.undp.org"

    def __init__(self, session: Session):
        self.session = session
        self.organization = get_organization_by_condition(
            condition=f"acronym='{self.organization_acronym}' AND region='{self._organization_region}'")
        self.publications_page_url = self.organization.publication_urls
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
        the pdf from UNDP-Europe-and-the-Commonwealth-of-Independent-States publication website

        """
        # step 1: Load all publications
        # We use a while loop to make sure we loaded all publications. For that, the number publications found
        # in the current iteration should be equal to the one in the last iteration
        publications_block = None
        last_publications_n = 0
        while True:
            current_publications_block = self.load_all_publications()
            current_publications_n = len(self.get_publications_list(current_publications_block))
            if last_publications_n < current_publications_n:
                publications_block = current_publications_block
                last_publications_n = current_publications_n
            else:
                break

        # step 2: Get the list of publications that are present in publications_block_element as 'a' tags
        publications_list = self.get_publications_list(selenium_web_element=publications_block)

        # step 3: Get links of all publications and insert them into temp_publications_urls_table
        self.get_all_publications_links(publications_list=publications_list)

        # Free memory
        del publications_block
        del publications_list

        # step 4: Get details togethers with the download links of each publication on the current page
        self.get_publications_details_from_urls()

        # step 5: Apply filter to the list of publications list
        filter_list_publications_and_details()

        # step 6: Download new publications
        results = start_downloads(pdf_files_directory=self.pdf_files_directory)
        self.number_of_pdfs_found_in_current_session = results['total_of_pdfs_found']
        self.number_of_downloaded_pdfs_in_current_session = results['total_of_pdfs_downloaded']

        return True

    def load_all_publications(self):
        """
        This function load all available publications by clicking on 'View More' button until
        it is no longer visible on the webpage.
        It will then return the div block containing all
        """

        # Set up Chrome options
        chrome_options = Options()
        chrome_options.add_argument(
            "--headless")  # Run Chrome in headless mode: Without opening the Chrome browser in a visible window

        # Set up web driver with Chrome options
        # driver = webdriver.Chrome()  # show browser
        driver = webdriver.Chrome(options=chrome_options)  # Hide browser

        # Navigate to the website
        driver.get(self.publications_page_url)

        # Define the block element
        publications_block_locator = (By.ID, 'view-more-news-center')

        view_more_block_locator = (By.CLASS_NAME, 'cta-button')  # div containing the "View More" button
        load_more_button_selector = (By.CLASS_NAME, 'load-more-custom')

        print("\r", "Loaded page(s): 1", end="")
        i = 1
        nbr_times_block_size_remained_unchanged = 0  # number of times the number of publications in publications_block
        last_pubs_n = 0
        while True:
            publications_block = driver.find_element(*publications_block_locator)
            view_more_block_element = driver.find_element(*view_more_block_locator)

            # Check if all publications was loaded
            # For that we check if the div block containing the 'View More' button is still visible
            if 'hide' in view_more_block_element.get_attribute('class'):
                # If not visible then all publications are loaded. break the while loop
                is_view_more_button_visible = False
            else:
                # Before each new click
                button_view_more = driver.find_element(*load_more_button_selector)

                # Scroll to the load more button
                driver.execute_script("arguments[0].scrollIntoView();", button_view_more)

                # Click the load more button
                driver.execute_script("arguments[0].click();", button_view_more)

                # Wait for the new items to be loaded
                k = 0  # number of times the number of publications in publications_block
                # remains unchanged in the below while loop
                last_publications_n = 0
                is_view_more_button_visible = True
                while True:
                    current_publications_n = len(self.get_publications_list(publications_block))
                    # print(f"last_publications_n: {last_publications_n}, publications len: {get_publications_n(
                    # view_more_block_element)}.find, k: {k}")
                    if last_publications_n != current_publications_n:
                        publications_block = driver.find_element(*publications_block_locator)
                        current_publications_n = len(self.get_publications_list(publications_block))
                        last_publications_n = current_publications_n  # Update the last length
                    elif k < 3:
                        k += 1
                    else:
                        # The length of the block containing the publication is not changing
                        break
                i += 1

                print(end=f"\r Loaded page(s): {i}, publications n: {current_publications_n}")

                if last_pubs_n < current_publications_n:
                    last_pubs_n = current_publications_n
                    nbr_times_block_size_remained_unchanged = 0
                else:
                    nbr_times_block_size_remained_unchanged += 1

                # TODO: to be removed, was just for testing
                # --------------------- Start test
                # if i > 10:
                #     return driver.find_element(*publications_block_locator)
                # --------------------- End test

            # We test at least 10 times to be sure all publications were displayed on the page
            # (nbr_times_block_size_remained_unchanged > 10). If the total number of publications remains unchanged
            # during those consecutive 1 assessments, then we consider we have all available publications
            if not is_view_more_button_visible or nbr_times_block_size_remained_unchanged > 10:
                last_publications_n = 0
                k = 0
                while True:
                    time.sleep(1)
                    current_publications_n = len(self.get_publications_list(publications_block))
                    if last_publications_n != current_publications_n:
                        # time.sleep(2)
                        publications_block = driver.find_element(*publications_block_locator)
                        current_publications_n = len(self.get_publications_list(publications_block))
                        last_publications_n = current_publications_n  # Update the last length
                    elif k < 3:
                        k += 1
                    else:
                        # The length of the block containing the publication is not changing
                        break

                break  # Leave the main loop

        return driver.find_element(*publications_block_locator)

    def get_publications_list(self, selenium_web_element):
        """
        This function extract the number of publications from a Selenium web element.
        Each 'a' tag refers to a single publication
        :param selenium_web_element:
        :return:
        """
        return BeautifulSoup(selenium_web_element.get_attribute("outerHTML"), 'html.parser').find_all('a')

    def get_all_publications_links(self, publications_list):
        """
        This method retrieve the links of all the existing publications
        :return:
        """
        # Extract the links of all publications and insert them in temp_publications_urls_table
        for a_tag in publications_list:
            link = a_tag.attrs['href']
            publ_link = add_base_url_if_missing(base_url=self.download_base_url, url=link)
            self.session.db_handler.insert_data_into_table(
                table_name=CONFIG["general"]["temp_publications_urls_table"],
                data={"url": publ_link})

    def get_publication_tags_list(self, publication_page_soup: BeautifulSoup) -> str:
        publication_tag_links_list = publication_page_soup.find_all('a',
                                                                    class_=['tag-link'])
        tags_list = ""
        if publication_tag_links_list:
            tags_list_ = [link.text for link in publication_tag_links_list]
            tags_list = "; ".join(tags_list_)

        return tags_list

    def get_publication_details(self, publication_url: str) -> list:
        """
        Return the details of a publication such as:
        Title
        publication_date
        download_link
        """
        results = []
        jump_to_get_any_pdf = False

        publication_title = publication_url.split("/")[-1].replace("-", " ")  # By default, the title is set to string
        # after the last '/' in the url. the '-' are replaced by single white spaces
        publication_date = ""
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
                except BaseException as e:
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
            result_publications_urls = get_chunk_temp_publications_urls(from_id=start_id, limit=chunk_size)
            publications_urls = [purl['url'] for purl in result_publications_urls]
            last_url = result_publications_urls[-1]
            start_id = last_url['id'] + 1

            for page_url in publications_urls:
                publication_details = self.get_publication_details(publication_url=page_url)  # Get the download link

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
