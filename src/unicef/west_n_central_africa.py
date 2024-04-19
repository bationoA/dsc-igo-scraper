"""This is the scraper class for the `UNICEF West and Central Africa`'s website. It collects all publications
and their details, and download the ones missing in the SQLite database """
import datetime
import re
import time

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from src import CONFIG
from src.db_handler import get_total_temp_publications_urls, get_chunk_temp_publications_urls
from src.dir_fc import generate_organization_download_pdf_directory_path
from src.session import Session
from src.common import filter_list_publications_and_details, generate_document_id, add_base_url_if_missing, \
    get_page_from_url, format_language
from src.document import start_downloads, Document
from src.organizations import get_organization_by_condition


class WestAndCentralAfricaScraper:
    _organization_acronym: str = "UNICEF"
    _organization_region: str = "West and Central Africa"
    _download_base_url = "https://www.unicef.org/"

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
        # step 1: Load all publications
        # We use a while loop to make sure we loaded all publications. For that, the number publications found
        # in the current iteration should be equal to the one in the last iteration
        publications_links_list = self.load_all_publications()

        # step 2: Insert links of all publications into temp_publications_urls_table
        self.get_all_publications_links(publications_list=publications_links_list)

        # Free memory
        del publications_links_list

        # step 3: Get links of all publications and details
        self.get_publications_details_from_urls()

        # step 4: Apply filter to the list of publications list
        filter_list_publications_and_details()

        # step 5: Download new publications
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

        # view_more_block_locator = (By.CLASS_NAME, 'cta-button')  # div containing the "View More" button
        load_more_button_selector = (By.XPATH, "//a[contains(text(), 'Load more items')]")

        print("\r", "Loaded p(s): 1", end="")
        i = 1
        # Number of times the number of publications in publications_links_list
        nbr_times_block_size_remained_unchanged = 0
        last_pubs_n = 0
        current_publications_n = 0
        while True:
            try:
                # Before each new click
                button_load_more = driver.find_element(*load_more_button_selector)
            except:
                break

            # Scroll to the load more button
            driver.execute_script("arguments[0].scrollIntoView();", button_load_more)

            # Click the load more button
            driver.execute_script("arguments[0].click();", button_load_more)

            # Wait for the new items to be loaded
            k = 0  # number of times the number of publications in publications_links_list
            # remains unchanged in the below while loop
            last_publications_n = 0
            is_view_more_button_visible = True
            while True:
                current_publications_n = len(self.get_publications_list(driver))
                if last_publications_n != current_publications_n:
                    # publications_links_list = driver.find_element(*publications_links_list_locator)
                    current_publications_n = len(self.get_publications_list(driver))
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

            # We test at least 10 times to be sure all publications were displayed on the page
            # (nbr_times_block_size_remained_unchanged > 10). If the total number of publications remains unchanged
            # during those consecutive 10 assessments, then we consider we have all available publications
            if not is_view_more_button_visible or nbr_times_block_size_remained_unchanged > 10:
                last_publications_n = 0
                k = 0
                while True:
                    time.sleep(1)
                    current_publications_n = len(self.get_publications_list(driver))
                    if last_publications_n != current_publications_n:
                        # publications_links_list = driver.find_element(*publications_links_list_locator)
                        current_publications_n = len(self.get_publications_list(driver))
                        last_publications_n = current_publications_n  # Update the last length
                    elif k < 3:
                        k += 1
                    else:
                        # The length of the block containing the publication is not changing
                        break

                break  # Leave the main loop

        print(end=f"\r Loaded page(s): {i}, publications n: {current_publications_n}")
        return self.get_publications_list(driver)

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

    def get_publications_list(self, selenium_driver):
        """
        This function extract the number of publications from a Selenium web element.
        Each 'a' tag refers to a single publication
        :param selenium_driver:
        :return:
        """
        publications_divs_list = selenium_driver.find_elements(*(By.CLASS_NAME, 'card'))

        divs = [BeautifulSoup(selenium_item.get_attribute("outerHTML"), 'html.parser') for selenium_item in
                publications_divs_list]

        # Return `a` tags of each `div`
        return [div.find('a') for div in divs if div.find('a')]

    def get_publication_details(self, publication_url: str) -> list:
        """
        Return the details of a publication
        """
        results = []

        publication_page = get_page_from_url(url=publication_url)  # Get the page where the 'Download' Button is

        if publication_page is None:
            return results

        publication_div = publication_page.find('article', class_="ctype-publication")

        if not publication_div:
            return []

        # --- Get publication's title
        publication_title = self.get_title(p_div=publication_div)

        # --- Get Tags
        tags_list = self.get_tags(p_div=publication_div)

        # --- Publication's date
        publication_date = self.get_date(p_div=publication_div)

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

    def get_title(self, p_div: BeautifulSoup) -> str:
        try:
            return p_div.find('h3', class_="sub-title center").text.strip()
        except:
            return ""

    def get_tags(self, p_div: BeautifulSoup) -> str:
        try:
            p_tag_divs = p_div.find_all('div', class_="content-list-item grey-dark-bg grey-bg-focus")
            p_tags = [div.text.strip().replace("#", "") for div in p_tag_divs]
            return ", ".join(p_tags)
        except:
            return ""

    def get_date(self, p_div: BeautifulSoup) -> str:
        try:
            p_date_div = p_div.find('div', class_="field_publication_pub_date")
            return p_date_div.find('time').get("datetime").strip()
        except:
            return ""

    def get_lang_link(self, p_div: BeautifulSoup) -> list:
        lang_link_list = []
        try:
            files_divs = p_div.find_all('div', class_="file-item")
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
