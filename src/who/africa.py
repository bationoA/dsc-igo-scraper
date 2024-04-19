import datetime
import inspect
from bs4 import BeautifulSoup
from src import CONFIG
from src.dir_fc import generate_organization_download_pdf_directory_path
from src.files_fc import LogEvent, LogLevel
from src.session import Session
from src.common import filter_list_publications_and_details, get_page_from_url, generate_document_id, \
    add_base_url_if_missing
from src.db_handler import get_total_temp_publications_urls, get_chunk_temp_publications_urls
from src.document import start_downloads, Document
from src.organizations import get_organization_by_condition


class WhoAfricaScraper:
    _organization_acronym: str = "WHO"
    _organization_region: str = "Africa"
    _download_base_url = "https://www.afro.who.int"

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
        last_page = False
        page = 0
        while not last_page:
            # Get the url of the page
            page_ulr = self.get_page_url(page_number=page)

            # Get the current page (as a BeautifulSoup object)
            current_page_soup = get_page_from_url(url=page_ulr)

            # Check if the current page is the last one
            if current_page_soup is not None and current_page_soup.find("a", {"title": "Go to last page"}) is None:
                last_page = True
                # break

            if current_page_soup is None:
                msg = f"current_page_soup was None for the page ulr: {page_ulr}. \n " \
                      f"Check your internet connection and/or the page ulr.",
                print(msg)
                LogEvent(level=LogLevel.WARNING.value,
                         message=msg,
                         function_name=inspect.currentframe().f_code.co_name).save()
                continue

            # Get list of publications with their link on the current page\
            publ_links = self.get_list_of_publication_links_from_page(soup_page=current_page_soup, url=page_ulr)

            nbr_retrieved_publications += len(publ_links)

            for publ_link in publ_links:
                self.session.db_handler.insert_data_into_table(
                    table_name=CONFIG["general"]["temp_publications_urls_table"],
                    data={"url": publ_link})

            print(end=f"\r Retrieving publications: {nbr_retrieved_publications} ")

            page += 1
        print("")

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

        publication_title = publication_url.split("/")[-1].replace("-",
                                                                   " ")  # By default, the title is set to string
        # after the last '/' in the url. the '-' are replaced by single white spaces

        publication_page = get_page_from_url(url=publication_url)  # Get the page where the 'Download' Button is

        if publication_page is None:
            return []

        article = publication_page.find('article')
        if article is None:
            return []

        publication_content = publication_page.find('div', class_="publication-content")
        if publication_content is None:
            return []

        # Get publications details
        tmp_title = article.find('h3', class_="publication-title")
        publication_title = tmp_title.text if tmp_title is not None else publication_title
        publication_date = ""
        publication_language = ""
        tmp_details_p = publication_content.find('p')  # Other details
        if tmp_details_p is not None:
            if tmp_details_p.find('strong', text="Publication date"):
                # Find the <strong> tags containing the desired information
                strong_tags = tmp_details_p.find_all("strong")
                # Loop through the <strong> tags and extract the relevant information
                for tag in strong_tags:
                    if "Publication date" in tag.text:
                        publication_date = tag.next_sibling.strip().replace(':', '').strip()
                    elif "Languages" in tag.text:
                        publication_language = tag.next_sibling.strip().replace(':', '').strip()

        # Extract publication tags/topics
        tags_list = ""  # No tags for this website

        # Get download links
        pdf_download_links = self.get_pdf_download_links(download_page_soup=publication_page)

        # Check if download link does not target a pdf file
        if len(pdf_download_links) == 1:
            pdf_download_link = pdf_download_links[0]

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
                             publication_date=f"{publication_date}",  # Not formatted
                             publication_url=publication_url,
                             downloaded_at=datetime.datetime.utcnow().isoformat(),
                             pdf_link=pdf_download_link
                             )
                    ]

        # webpage targeted by the current link (pdf_download_link) Get the webpage with multiple pdfs
        # Get the list of all version of the pdfs for the current publication
        if len(pdf_download_links) > 0:
            for i, link in enumerate(pdf_download_links):
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
                             publication_date=f"{publication_date}",  # Not formatted,
                             publication_url=publication_url,
                             downloaded_at=datetime.datetime.utcnow().isoformat(),
                             pdf_link=link,
                             lang=publication_language
                             )
                )
            return results

        main_content = publication_page.find(id='main-content')
        if main_content is None:
            return []

        if not len(pdf_download_links):
            # If publication links was found using previous selections, then look for any links
            # that refers to a pdf file
            any_pdfs_list = self.get_any_pdf_links_from_page(
                soup_page=main_content)

            if not len(any_pdfs_list):
                # Attempts of getting pdf links failed then return an empty list
                return []

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
                             publication_date=f"{publication_date}",  # Not formatted,
                             publication_url=publication_url,
                             downloaded_at=datetime.datetime.utcnow().isoformat(),
                             pdf_link=pdf_['download_link']
                             )
                )

        return results

    def get_any_pdf_links_from_page(self, soup_page: BeautifulSoup):
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

                list_links.append({
                    'ind': f"ind_{ind}",
                    'download_link': link_text
                })

        return list_links

    def get_list_of_publication_links_from_page(self, soup_page: BeautifulSoup, url="") -> list:
        """
        This function retrieve the links related to each publication of
        a specific page and returns them in a list
        """
        # Get publication div container
        p_c_div = soup_page.find("div", class_="publication-view-page")  # the div container containing all
        if p_c_div is None:
            msg = f"No publication found on this page url: {url}"
            print(msg)
            LogEvent(level=LogLevel.ERROR.value,
                     message=msg,
                     function_name=inspect.currentframe().f_code.co_name).save()
            return []

        list_publ_link = []  # this will contain the links of each publication on the give page

        # Collect links
        for child_div in p_c_div.find_all("div", class_="col-md-3 views-row"):
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

    def get_pdf_download_links(self, download_page_soup: BeautifulSoup) -> list:
        """
        This function retrieve the direct url to a specific pdf file
        """
        # Extract PDF download link(s)
        span_pdf_links = download_page_soup.find_all("span", class_="file-link")

        if span_pdf_links is None:
            return []

        pdf_links = [span.find("a").get("href").strip() for span in span_pdf_links if span.find("a") is not None]

        # Some pdf links end as follows: ".pdf?..." Let's remove any text after .pdf if exist
        for ind, link in enumerate(pdf_links):
            if not link.endswith(".pdf"):
                real_link = link.split("?")[0]
                pdf_links[ind] = real_link

        return list(set(pdf_links))  # return distinct links

    def get_page_url(self, page_number: int) -> str:
        """
        This function generate the url of a page for publications based the page number
        page_number: the page number
        """
        return f"{self.publications_page_url}?page={page_number}"
