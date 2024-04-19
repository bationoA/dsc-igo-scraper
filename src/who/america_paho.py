"""This is the scraper class for `America-PAHO` website. It collects all publications and their details and download
the ones missing in the SQLite database """

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
from src.time_fc import get_timestamp_from_date_and_time, timestamp_to_datetime_isoformat


class WhoAmericaPahoScraper:
    _organization_acronym: str = "WHO"
    _organization_region: str = "America-PAHO"
    _download_base_url = "https://www.paho.org"

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
        This method retrieve the url of all the existing publications
        :return:
        """
        # For each page,
        print(f" Retrieving publications: 0", end="")
        nbr_retrieved_publications = 0  # Number of retrieved publications
        last_page = False
        page = 0  # 0 is the first page
        while not last_page:
            # Get the url of the page
            page_ulr = self.get_page_url(page_number=page)

            # Get the current page (as a BeautifulSoup object)
            current_page_soup = get_page_from_url(url=page_ulr)

            # Check if the current page is the last one
            if current_page_soup is not None and current_page_soup.find("a", {"title": "Go to last page"}) is None:
                last_page = True

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

        # Get both response and BeautifulSoup objects
        response, publication_page = get_page_from_url(url=publication_url, get_response=True)

        if publication_page is None:
            return []

        # -------------- Get publications details
        publication_title = ""
        publication_date = ""
        publication_language = ""
        tags_list = ""
        pdf_download_links = []

        # Get publication's details based on the based url
        final_url = response.url  # the final URL after following any redirects
        publication_detail = None

        if "https://www.paho.org/" in final_url:  # For `https://www.paho.org/` base url
            publication_detail = self.get_publication_details_from_paho(publication_url=final_url)

        if publication_detail is None and "iris.paho.org/" in final_url:  # For `iris.paho.org` base url
            publication_detail = self.get_publication_details_from_iris(publication_url=final_url)

        if "who.int/" in final_url:  # For `https://www.who.int/` base url
            publication_detail = self.get_publication_details_from_who(publication_url=final_url)

        if publication_detail is not None:
            publication_title = publication_detail["title"]
            publication_date = publication_detail["date"]
            publication_language = publication_detail["language"]
            tags_list = publication_detail["tags_list"]
            pdf_download_links = publication_detail["download_links"]

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
                             publication_date=publication_date,
                             publication_url=publication_url,
                             downloaded_at=datetime.datetime.utcnow().isoformat(),
                             pdf_link=pdf_download_link
                             )
                    ]

        # If webpage targeted by the current link (pdf_download_link) Get the webpage with multiple pdfs
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
                             publication_date=publication_date,  # Not formatted,
                             publication_url=publication_url,
                             downloaded_at=datetime.datetime.utcnow().isoformat(),
                             pdf_link=link,
                             lang=publication_language
                             )
                )
            return results

        if not len(pdf_download_links):
            # If publication links was found using previous selections, then look for any links
            # that refers to a pdf file
            any_pdfs_list = self.get_any_pdf_links_from_page(
                soup_page=publication_page)

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

    def get_publication_details_from_iris(self, publication_url: str):
        """
        Get publication's details if the publication's url has the base url `https://iris.paho.org/`
        :param publication_url:
        :return:
        """
        base_url = "https://iris.paho.org/"
        publication_title = publication_url.split("/")[-1].replace("-",
                                                                   " ")  # By default, the title is set to string
        # after the last '/' in the url. the '-' are replaced by single white spaces

        publication_page = get_page_from_url(url=publication_url)  # Get the page where the 'Download' Button is

        if publication_page is None:
            return None

        main_container = publication_page.find('div', class_="item-summary-view-metadata")

        if main_container is None:
            return None

        # -------------- Get publications details
        # ----- Title
        tmp_title = main_container.find('h2', class_="first-page-header")
        publication_title = tmp_title.text if tmp_title is not None else publication_title

        # ----- Date
        publication_date = ""
        date_div = main_container.find('div', "simple-item-view-date word-break item-page-field-wrapper table")
        try:
            publication_date = date_div.contents[1].text if date_div is not None else None
        except:
            pass
        publication_date = publication_date.strip() if publication_date is not None else ""

        if len(publication_date) > 4:  # For day, month, year
            try:
                y, m, d = publication_date.split(' ')  # day, month_name, year
                d = d.replace(',', '')  # remove the comma next to the day
                d, m, y = int(str(d)), int(str(m)), int(str(y))  # all are already integers

                publication_timestamp = get_timestamp_from_date_and_time(year=y, month=m, day=d)
                publication_date = timestamp_to_datetime_isoformat(timestamp=publication_timestamp)
            except:
                pass
        elif publication_date == 4:
            publication_date = int(str(publication_date))  # Only year

        # ----- Language
        publication_language = ""  # No language was observed for this website

        # ----- Tags/Topics
        tag_divs = main_container.find('div', class_="simple-item-view-description item-page-field-wrapper table",
                                       string="Subject")
        tags_list = ""
        if tag_divs is not None:
            tags_links = tag_divs.find_all('a')
            if len(tags_links):
                tags_list = [tg_lk['href'] for tg_lk in tags_links]
                tags_list = "; ".join(tags_list)

        # ----- Download
        download_links = []
        download_div = main_container.find('div', class_="item-page-field-wrapper table word-break")
        if download_div is not None:
            download_a_tags = download_div.find_all('a')
            download_links = [tag_a['href'] for tag_a in download_a_tags if tag_a['href'] is not None]
            # Add base url to links where missing
            download_links = [add_base_url_if_missing(base_url=base_url, url=lk) for lk in download_links]

        return dict(
            title=publication_title,
            date=publication_date,
            language=publication_language,
            tags_list=tags_list,
            download_links=download_links
        )

    def get_publication_details_from_who(self, publication_url: str):
        """
        Get publication's details if the publication's url has the base url `https://www.who.int/`
        :param publication_url:
        :return:
        """
        base_url = "https://www.who.int/"
        publication_title = publication_url.split("/")[-1].replace("-",
                                                                   " ")  # By default, the title is set to string
        # after the last '/' in the url. the '-' are replaced by single white spaces

        publication_page = get_page_from_url(url=publication_url)  # Get the page where the 'Download' Button is

        if publication_page is None:
            return None

        article = publication_page.find('article')

        if article is None:
            return None

        # -------------- Get publications details
        # ----- Title
        tmp_title = article.find('h1', class_="dynamic-content__heading")
        publication_title = tmp_title.text if tmp_title is not None else publication_title

        # ----- Date
        publication_date = article.find('div', class_="dynamic-content__date")
        publication_date = publication_date.text if publication_date is not None else None

        publication_date = publication_date.strip() if publication_date is not None else ""

        if len(publication_date) > 4:  # For day, month, year
            try:
                d, m, y = publication_date.split(' ')  # day, month_name, year
                d = d.replace(',', '')  # remove the comma next to the day
                d, m, y = int(str(d)), str(m).strip(), int(str(y))

                month_number = datetime.datetime.strptime(m, '%B').month  # get the integer representation of month
                publication_timestamp = get_timestamp_from_date_and_time(year=y, month=month_number, day=d)
                publication_date = timestamp_to_datetime_isoformat(timestamp=publication_timestamp)
            except:
                pass
        elif publication_date == 4:
            publication_date = int(str(publication_date))  # Only year

        # ----- Language
        publication_language = ""  # No language was observed for this website

        # ----- Tags/Topics
        tag_div = article.find('div', class_="dynamic-content__tag")
        tags_list = ""
        tags_list = tag_div.contents[1].text if tag_div is not None else ""
        tags_list = tags_list.replace(",", "; ")
        tags_list = tags_list.replace("/", "; ")

        # ----- Download links
        download_div = article.find('div', class_="button button-blue-background")

        if download_div is None:
            return None

        download_link = download_div.find('a')
        if download_link is None:
            return None

        download_link = download_link.get("href")
        if download_link is None:
            return None

        download_link = add_base_url_if_missing(base_url=base_url, url=download_link.strip())

        download_links = [download_link]
        return dict(
            title=publication_title,
            date=publication_date,
            language=publication_language,
            tags_list=tags_list,
            download_links=download_links
        )

    def get_publication_details_from_paho(self, publication_url: str):
        """
        Get publication's details if the publication's url has the base url `https://www.paho.org/`
        :param publication_url:
        :return:
        """
        base_url = "https://www.paho.org/"
        publication_title = publication_url.split("/")[-1].replace("-",
                                                                   " ")  # By default, the title is set to string
        # after the last '/' in the url. the '-' are replaced by single white spaces

        publication_page = get_page_from_url(url=publication_url)  # Get the page where the 'Download' Button is

        if publication_page is None:
            return None

        download_link = publication_page.find('div', class_="download-button")
        if download_link is None:
            return None

        download_link = download_link.find('a')
        if download_link is None:
            return None

        download_link = download_link["href"]

        if download_link is None or download_link == "":
            return None

        download_link = add_base_url_if_missing(base_url=base_url, url=download_link.strip())

        if "https://iris.paho.org/" in download_link:
            return self.get_publication_details_from_iris(publication_url=download_link)

        if "https://www.who.int/" in download_link:
            return self.get_publication_details_from_who(publication_url=download_link)

        # -------------- Get publications details
        # ----- Title
        tmp_title = publication_page.find('h1', class_="page-header")
        publication_title = tmp_title.text if tmp_title is not None else publication_title
        # ----- Date
        publication_date = publication_page.find('div', class_="author")
        publication_date = publication_date.text.strip() if publication_date is not None else ""

        if len(publication_date) > 4:  # For day, month, year
            try:
                d, m, y = publication_date.split(' ')  # day, month_name, year
                d = d.replace(',', '')  # remove the comma next to the day
                d, m, y = int(str(d)), str(m).strip(), int(str(y))

                # we'll use %b (for abbreviations like 'Jul') instead of %B (for values like 'July', full month's name)
                month_number = datetime.datetime.strptime(m, '%b').month  # get the integer representation of month
                publication_timestamp = get_timestamp_from_date_and_time(year=y, month=month_number, day=d)
                publication_date = timestamp_to_datetime_isoformat(timestamp=publication_timestamp)
            except:
                pass
        elif publication_date == 4:
            publication_date = int(str(publication_date))  # Only year
        # ----- Language
        publication_language = ""  # No language was observed for this website

        # ----- Tags/Topics
        tag_divs = publication_page.find_all('div', class_="field--type-entity-reference")
        tags_list = ""
        if len(tag_divs):
            tags_list = [tg_div.text for tg_div in tag_divs]
            tags_list = "; ".join(tags_list)

        download_links = [download_link]
        return dict(
            title=publication_title,
            date=publication_date,
            language=publication_language,
            tags_list=tags_list,
            download_links=download_links
        )

    def get_any_pdf_links_from_page(self, soup_page: BeautifulSoup):
        """
        This method retrieve all links targeting a pdf file without following a specific . It will be called when the
        other attempts of getting publication pdf links in a structured way failed. This function will just collect
        all links to a pdf no matter where they're located on a specific publication page.

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

                if ".pdf" in link_text or "download" in link_text or "retrieve" in link_text:
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
        # Get publications div container (the div container containing all publications of the current page)
        p_c_div = soup_page.find("div", class_="grid views-view-grid horizontal")
        if p_c_div is None:
            msg = f"No publication found on this page url: {url}"
            print(msg)
            LogEvent(level=LogLevel.ERROR.value,
                     message=msg,
                     function_name=inspect.currentframe().f_code.co_name).save()
            return []

        list_publ_link = []  # this will contain the links of each publication on the give page

        # Collect publication's urls
        for child_div in p_c_div.find_all("div", class_="views-field views-field-field-image"):
            links = child_div.find_all("a")

            if len(links):  # if publication's urls found

                # Remove all 'a-tags' those are without an 'href' attribute
                links = [lk.get("href") for lk in links if lk is not None and lk.get("href") is not None]
                # Filter out URLs that do not point to a file but only to webpages
                files_extensions = ['.pdf', '.doc', '.docx', '.xls', '.ppt', '.pptx', '.jpg', '.png']
                links = [lk for lk in links if not any([lk.endswith(ext) for ext in files_extensions])]

                if not len(links):  # if no a-tags left then go to the next iteration
                    continue
                # Add the base url to all links where missing
                links = [add_base_url_if_missing(base_url=self.download_base_url, url=link) for link in
                         links]
                # Get unique list of links
                links = list(set(links))
                # Get the one using the base url of this website
                links_w_base_url = [lk for lk in links if lk.startswith(self.download_base_url)]
                link_text = None
                if len(links_w_base_url):
                    link_text = links_w_base_url[0]  # Chose the first link containing the base url
                else:
                    link_text = links[0]  # Just choose an url from `links`

                list_publ_link.append(link_text)  # Add the extracted url to the list

        return list(set(list_publ_link))

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
                if not len(publication_details):
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
        This function retrieve the direct url to a specific file
        """
        # Extract PDF download link(s)
        span_pdf_links = download_page_soup.find_all("span", class_="file-link")

        if span_pdf_links is None:
            span_pdf_links = download_page_soup.find_all("span", class_="file - -application - pdf")

        if span_pdf_links is None:
            return []

        pdf_links = [span.find("a").get("href").strip() for span in span_pdf_links if span.find("a") is not None]

        pdf_links = [add_base_url_if_missing(base_url=self.download_base_url, url=l) for l in pdf_links]

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
