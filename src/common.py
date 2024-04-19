import os
import json
import inspect
import hashlib
import time
from functools import partial
from multiprocessing import Pool
from src.db_handler import get_total_temp_documents, get_chunk_temp_documents_as_dict, DatabaseHandler
from src.files_fc import CONFIG, LogEvent, LogLevel
from bs4 import BeautifulSoup  # for parsing HTML and XML documents
import requests  # for downloading pdf files and html files of targeted websites
from urllib.parse import urlparse  # for validating urls
from src.time_fc import timestamp_to_datetime_isoformat, get_now_utc_timestamp
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options  # Options while setting up the webdriver with chrome


def get_request_response_error(response):
    return {
        'status_code': response.status_code,
        'reason': response.reason
    }


def is_valid_url(url: str):
    """
    Return True if url is in the valid format, and False if not
    A valid url format is: http://www.abcd.efg or https://www.abcd.efg
    """
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except BaseException as e:
        print(e.__str__())
        return False


def get_page_from_url(url: str, timeout=CONFIG['general']['request_time_out_in_second'], get_response=False,
                      ssl_verify=True, max_attempt=0, max_waiting_time_sec=0, headers=None):
    """
    This function take a URL as argument and return a BeautifulSoup of the corresponding html file from the
    internet. If there is an error (client, server, ...), the function will return None while printing the error code
    and the http response object If url is not valid, then it returns False

    param: max_attempt
    param: max_waiting_time_sec in seconds
    """

    if not headers:
        headers = {}
        for item in CONFIG["general"]["request_default_headers"]:
            headers[list(item.keys())[0]] = list(item.values())[0]

    max_attempt = CONFIG['general']['max_request_attempt'] if not max_attempt else max_attempt
    max_waiting_time_sec = CONFIG['general']['max_waiting_time_sec'] if not max_waiting_time_sec \
        else max_waiting_time_sec

    current_waiting_time = 0  # in seconds
    waiting_time_step = int(max_waiting_time_sec / max_attempt)  # in seconds

    response = None
    # Validate the url
    if is_valid_url(url):
        try:
            for atp in range(max_attempt + 1):
                time.sleep(current_waiting_time)  # wait before next request attempt
                response = requests.get(url=url, timeout=timeout, verify=ssl_verify, headers=headers)

                if response.ok:
                    if get_response:  # Return a response and a BeautifulSoup object
                        return response, BeautifulSoup(response.content, 'html.parser')
                    else:
                        return BeautifulSoup(response.content, 'html.parser')
                if response.status_code == 429:  # If error is due to 'Too many requests'
                    if atp < max_attempt - 1:
                        current_waiting_time += waiting_time_step
                        print(f"\n  Too many requests: Will retry in {current_waiting_time} second(s)")

            if not response.ok:
                msg = f"\nAn error occurred while retrieving from: '{url}'"
                print(msg)
                print(f"response: {response}")
                print("Code: ", response.status_code)

                # Save the error in logs
                LogEvent(level=LogLevel.ERROR.value,
                         message=msg,
                         function_name=inspect.currentframe().f_code.co_name,
                         exception=get_request_response_error(response=response)).save()
                if get_response:
                    return None, None
                else:
                    return None
        except BaseException as e:
            print(e.__str__())
            # Save the error in logs
            LogEvent(level=LogLevel.ERROR.value,
                     message=e.__str__(),
                     function_name=inspect.currentframe().f_code.co_name,
                     exception=e.__str__()).save()
            if get_response:
                return None, None
            else:
                return None
    else:
        # if invalid url
        msg = f"Invalid URL, please check your url: {url}"
        print(msg)
        # Save the error in logs
        LogEvent(level=LogLevel.ERROR.value,
                 message=msg,
                 function_name=inspect.currentframe().f_code.co_name).save()
        if get_response:
            return None, None
        else:
            return None


def selenium_get_page_from_url(url: str, headers: list = None, wait_element_located_xpath: tuple = None,
                               max_wait_time_sec=60, headless=True, get_beautifulsoup=True):
    """
    Uses Selenium to get a webpage. If succeeded, returns the page as a BeautifulSoup object, else returns None
    :param url:
    :param headers:
    :param wait_element_located_xpath:
    :param max_wait_time_sec:
    :param headless:
    :param get_beautifulsoup:
    :return:
    """
    # Set up Chrome options
    chrome_options = Options()

    # Add User-Agent header to the Chrome options
    if not headers:
        headers = CONFIG["general"]["request_default_headers"]

    for item in headers:
        chrome_options.add_argument(f"{list(item.keys())[0]}={list(item.values())[0]}")

    # Set up web driver with Chrome options
    if headless:  # Hide browser
        # Run Chrome in headless mode: Without opening the Chrome browser in a visible window
        chrome_options.add_argument(
            "--headless")
        driver = webdriver.Chrome(options=chrome_options)
    else:  # show browser
        driver = webdriver.Chrome()

    # Navigate to the webpage
    driver.get(url)

    try:
        if wait_element_located_xpath:
            # Wait for a specific element or attribute within the iframe to appear
            WebDriverWait(driver, max_wait_time_sec).until(
                EC.presence_of_element_located(wait_element_located_xpath)
            )

        if get_beautifulsoup:
            # Get the HTML source of the page
            html_source = driver.page_source

            # Create a BeautifulSoup object
            soup = BeautifulSoup(html_source, "html.parser")

            return soup
        else:
            return driver

    except Exception as e:
        print("Error:", e)

    return None


def filter_list_publications_and_details():
    """
    :return:
    """
    list_existing_tmp_document_id_temps = []  # To be deleted from temporary documents table
    # Remove all duplicate documents those pdf_links are pointing at identical file online (pdf_link)
    # list_publications_and_details = get_unique_documents_based_on_pdf_link(documents=list_publications_and_details)

    if not CONFIG['general']['download_even_if_exist']:
        filtering_message = "Filtering publications' list"
        print(f"\n {filtering_message}: 0%", end="")
        db_handler = DatabaseHandler()
        # If not download_even_if_exist, then filter out all existing pdfs
        # Get list of new publications on the current page
        length_temp_documents = get_total_temp_documents()

        # Retrieve temporary documents by chunks
        chunk_size = CONFIG["general"]["max_document_links_chunk_size"]
        chunk_total = length_temp_documents / chunk_size
        chunk_total = int(chunk_total) + 1 if int(chunk_total) < chunk_total else int(chunk_total)

        start_id = 0
        number_checked_doc = 0  # number of documents that existence were checked in the database
        for i in range(chunk_total):
            result_temp_documents = get_chunk_temp_documents_as_dict(from_id_temp=start_id, limit=chunk_size)
            max_tmp_doc_id_tmp = max([tmp_doc['id_temp'] for tmp_doc in result_temp_documents])
            max_id_tmp_tmp_doc = [tmp_doc for tmp_doc in result_temp_documents if tmp_doc['id_temp'] ==
                                  max_tmp_doc_id_tmp]
            max_id_tmp_tmp_doc = max_id_tmp_tmp_doc[0]
            start_id = max_id_tmp_tmp_doc['id_temp'] + 1

            for tmp_doc in result_temp_documents:

                # Does it already with no download error?
                document = db_handler.select_columns(table_name=CONFIG["general"]["documents_table"],
                                                     columns=["*"],
                                                     condition=f"id=?",
                                                     condition_vals=(tmp_doc['id'],)
                                                     )
                if document is not None and len(document):
                    if not document[0]['error']:
                        # If document exists with no error, then consider that it "exists";
                        # meaning it won't be downloaded again
                        list_existing_tmp_document_id_temps.append(tmp_doc['id_temp'])
                    elif document[0]['error'] and not CONFIG["general"]["retry_download_in_next_session"]:
                        # If document exists but has 1 in the column `error`, and the config file says to not retry any
                        # download in next sessions , then new downloaded won't be attempted on this document
                        list_existing_tmp_document_id_temps.append(tmp_doc['id_temp'])

                number_checked_doc += 1
                print(end=f"\r {filtering_message}: "
                          f"{round(100 * number_checked_doc / length_temp_documents, 2)}% ")

            # Delete temporary documents that already exist in the main documents table
            # ## Create a comma-separated list of placeholders based on the length of the list
            placeholders = ', '.join(['?' for _ in list_existing_tmp_document_id_temps])
            db_handler.delete_from_table(table_name=CONFIG["general"]["temp_documents_table"],
                                         condition=f"id_temp IN ({placeholders})",
                                         condition_vals=tuple(list_existing_tmp_document_id_temps)
                                         )


def get_total_number_pages(total_np: int, total_np_per_page: int) -> int:
    """
    It takes the total number of available publications and the maximum number of publications per page, then returns
    the total number of pages.
    """
    # print("total_np, total_np_per_page: ", total_np, total_np_per_page)
    try:
        nbr_pg = total_np / total_np_per_page
    except BaseException as e:
        # Save the error in logs
        LogEvent(level=LogLevel.ERROR.value,
                 message=str(e),
                 function_name=inspect.currentframe().f_code.co_name,
                 exception=str(e)).save()
        return 0

    if nbr_pg > int(nbr_pg):
        return int(nbr_pg) + 1
    else:
        return int(nbr_pg)


def get_lan_from_text(text_: str) -> str:
    try:
        lang = [lg for lg in CONFIG["general"]["un_languages"]["lang_dict"].values() if
                lg.lower() in text_.lower()]
        return lang[0]
    except:
        return ""


def download_pdf(url: str, file_dir: str, file_name: str,
                 timeout=CONFIG['general']['request_time_out_in_second'],
                 max_attempt=1, max_waiting_time_sec=0) -> bool:
    """
    This function download a file and save on the disk at the path
    indicated in file_path
    """

    max_attempt = 1 if not max_attempt else max_attempt
    waiting_time_step = 0  # 0 second
    current_waiting_time = 0  # in seconds
    if max_attempt and max_waiting_time_sec:
        waiting_time_step = int(max_waiting_time_sec / max_attempt)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (HTML, like Gecko) "
                      "Chrome/58.0.3029.110 Safari/537.3 "
    }

    load_page = True
    try_nbr = 0
    response = None
    is_success = True
    while load_page:
        is_success = True
        try:
            for atp in range(max_attempt + 1):
                time.sleep(current_waiting_time)  # wait before next request attempt
                response = requests.get(url, timeout=timeout, headers=headers, verify=False)
                if response.status_code == 429:  # If error is due to 'Too many requests'
                    if atp < max_attempt - 1:
                        current_waiting_time += waiting_time_step
                        print(f"\n  Too many requests: Will retry in {current_waiting_time} second(s)")

        except BaseException as e:
            is_success = False
            print(e)
            # Save the error in logs
            LogEvent(level=LogLevel.ERROR.value,
                     message=e.__str__(),
                     function_name=inspect.currentframe().f_code.co_name,
                     exception=e.__str__()).save()
            break

        # If no error
        if response.status_code == 200:
            content_type = response.headers.get('Content-Type')
            # Check if the type of the downloaded is in the list of expected files types
            ext = [f_type for f_type in CONFIG["general"]["file_types"] if f_type.lower() in content_type.lower()]
            if ext:
                # Get file extension
                ext = ext[0]
                full_file_path = os.path.join(file_dir, file_name + "." + ext)
                try:
                    with open(full_file_path, 'wb') as file:
                        file.write(response.content)
                except BaseException as e:
                    is_success = False
                    # In case of error when trying to save the pdf...
                    print("  Error: ", e.__str__())
                    # Save the error in logs
                    LogEvent(level=LogLevel.DEBUG.value,
                             message=f"Error while saving at {full_file_path}, url: {url}",
                             function_name=inspect.currentframe().f_code.co_name,
                             exception=e.__str__()).save()
                break
            else:  # The downloaded file type is not in the list of file's type of interest
                # Try to load the page of the url and another pdf file if available
                if "html" in content_type:
                    try_nbr += 1
                    if try_nbr < 2:
                        resp, soup = get_page_from_url(url=url, timeout=timeout, get_response=True)
                        parsed_url = urlparse(resp.url)
                        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                        links = soup.find_all("a")

                        if len(links):
                            link = [lk for lk in links if
                                    lk is not None and "href" in lk.attrs and ".pdf" in lk["href"]]
                            if len(link):
                                url = add_base_url_if_missing(base_url=base_url, url=link[0]["href"])
                                load_page = True  # Go back and re-try with this new url
                        else:
                            is_success = False
                            break  # stop trying getting another link
                    else:
                        is_success = False
                        break  # stop trying getting another link
                else:
                    is_success = False
                    msg = f"Skip Warning: file type is not in the list of file's types of interest. expecting " \
                          f"{'/ '.join(CONFIG['general']['file_types'])} but got {content_type}"
                    print(f"  {msg}")
                    # Save the error in logs
                    LogEvent(level=LogLevel.WARNING.value,
                             message=f"{msg}, url: {url}",
                             function_name=inspect.currentframe().f_code.co_name
                             ).save()
                    break
        else:
            is_success = False
            break

    if response is not None and response.status_code != 200:
        is_success = False
        msg = f"Failed to download PDF file. Status code: {response.status_code}, url: {url}"
        print(f"  {msg}")
        # Save the error in logs
        LogEvent(level=LogLevel.ERROR.value,
                 message=msg,
                 function_name=inspect.currentframe().f_code.co_name,
                 exception=get_request_response_error(response=response)).save()

    return is_success


def download_and_save_pdfs(pdfs_file_dir: str, list_publications_details: list):
    """
    This function download a list of pdfs and store them in their corresponding organization folder
    :param pdfs_file_dir:
    :param list_publications_details: List of publications details
    """

    print(f"   Downloading documents: 0% - {list_publications_details[0].pdf_link}", end="")
    total_documents = len(list_publications_details)
    if not total_documents:
        msg = "  No new PDFs publications are available for download at the moment."
        print("\r", msg, end="")
        LogEvent(level=LogLevel.WARNING.value,
                 message=msg,
                 function_name=inspect.currentframe().f_code.co_name).save()
        return 0, 0

    nbr_error = 0
    for ind, document in enumerate(list_publications_details):
        file_name_on_disk = document.id  # The extension will be added later depending on the file's type
        # Download the file
        url = document.pdf_link
        file_dir = pdfs_file_dir
        file_name = file_name_on_disk
        timeout = CONFIG['general']['request_time_out_in_second']
        max_attempt = CONFIG["general"]["max_request_attempt"]
        max_waiting_time_sec = CONFIG["general"]["max_waiting_time_sec"]
        args = [url, file_dir, file_name, timeout, max_attempt, max_waiting_time_sec]

        status = download_pdf_args(args=args)
        # If an error occurred
        if not status:
            nbr_error += 1
            document.error = 1
            msg = f"Failed to download - publication: {json.dumps(document.to_dict())}"
            LogEvent(level=LogLevel.ERROR.value,
                     message=msg,
                     function_name=inspect.currentframe().f_code.co_name).save()

        if CONFIG['general']['retry_download_in_next_session'] and document.exist_in_database():
            # update document
            document.update()
        else:
            # Insert publication in documents' table
            document.downloaded_at = timestamp_to_datetime_isoformat(timestamp=get_now_utc_timestamp())
            document.insert()
        next_document_pdf_link = f"- {list_publications_details[ind].pdf_link}" if ind < (total_documents - 1) else ""
        print(end=f"\r   Downloading documents: {round(100 * (ind + 1) / total_documents, 2)}% "
                  f"{next_document_pdf_link}")

    if nbr_error > 0:
        # The script failed to download some pdfs. Number nbr_error
        msg = f"Failed to download {nbr_error} document(s)"
        warning_color = '\033[93m'
        reset_color = '\033[0m'
        print(f"\n  {warning_color}{msg}. See logs file{reset_color}")
        LogEvent(level=LogLevel.WARNING.value,
                 message=msg,
                 function_name=inspect.currentframe().f_code.co_name).save()
    # Return number of found pdfs, and total downloaded pdfs
    return len(list_publications_details), len(list_publications_details) - nbr_error


def download_and_save_pdfs_multiprocessing(pdfs_file_dir: str, list_publications_details: list,
                                           nbr_concurrent_downloads=CONFIG["general"]["max_concurrent_downloads"]):
    """
    Using `multiprocessing` package for parallel download,
    This function download a list of pdfs and store them in their corresponding organization folder
    :param pdfs_file_dir: path to write the downloaded file
    :param nbr_concurrent_downloads: Maximum number of document to be downloaded simultaneously
    :param list_publications_details: List of publications details
    """

    print(f"   Downloading documents: 0% ", end="")
    total_documents = len(list_publications_details)
    if not total_documents:
        msg = "  No new PDFs publications are available for download at the moment."
        print("\r", msg, end="")
        LogEvent(level=LogLevel.WARNING.value,
                 message=msg,
                 function_name=inspect.currentframe().f_code.co_name).save()
        return 0, 0

    nbr_error = 0

    concurrent_list_publications_details = get_concurrent_items_list(list_items=list_publications_details,
                                                                     nbr_simult=nbr_concurrent_downloads)

    nbr_assessed_docs = 0
    for sub_list_docs in concurrent_list_publications_details:
        pool = Pool(nbr_concurrent_downloads)  # Pool(cpu_count()) for using all available cpus
        download_func = partial(download_pdf_args)

        file_urls_list = [doc.pdf_link for doc in sub_list_docs]
        file_names_list = [doc.id for doc in sub_list_docs]

        # Create a list of tuples with all required arguments
        timeout = CONFIG['general']['request_time_out_in_second']
        max_attempt = CONFIG["general"]["max_request_attempt"]
        max_waiting_time_sec = CONFIG["general"]["max_waiting_time_sec"]

        file_urls_names_list = [
            (url, pdfs_file_dir, file_name, timeout, max_attempt, max_waiting_time_sec)
            for url, file_name in
            zip(file_urls_list, file_names_list)]

        results = pool.map(download_func, file_urls_names_list)
        pool.close()
        pool.join()

        nbr_assessed_docs += len(sub_list_docs)

        for success, document in zip(results, sub_list_docs):
            # If an error occurred
            if not success:
                nbr_error += 1
                document.error = 1
                msg = f"Failed to download - publication: {json.dumps(document.to_dict())}"
                LogEvent(level=LogLevel.ERROR.value,
                         message=msg,
                         function_name=inspect.currentframe().f_code.co_name).save()

            if CONFIG['general']['retry_download_in_next_session'] and document.exist_in_database():
                # update document
                document.update()
            else:
                # Insert publication in documents' table
                document.downloaded_at = timestamp_to_datetime_isoformat(timestamp=get_now_utc_timestamp())
                document.insert()

            print(end=f"\r   Downloading documents: {round(100 * nbr_assessed_docs / total_documents, 2)}% ")

    if nbr_error > 0:
        # The script failed to download some pdfs. Number nbr_error
        msg = f"Failed to download {nbr_error} document(s)"
        warning_color = '\033[93m'
        reset_color = '\033[0m'
        print(f"\n  {warning_color}{msg}. See logs file{reset_color}")
        LogEvent(level=LogLevel.WARNING.value,
                 message=msg,
                 function_name=inspect.currentframe().f_code.co_name).save()
    # Return number of found pdfs, and total downloaded pdfs
    return len(list_publications_details), len(list_publications_details) - nbr_error


def get_concurrent_items_list(list_items: list, nbr_simult: int) -> list:
    """
    Take a list returns a list of mutually exclusive sublist of length less or equal to nbr_simult

    :param: list_items: List of documents to be downloaded
    :param: nbr_simult: Maximum number of documents to be downloaded simultaneously
    """

    nbr_simult = 1 if nbr_simult < 1 else nbr_simult

    nbr_iterations = len(list_items) / nbr_simult
    nbr_iterations = int(nbr_iterations) + 1 if nbr_iterations > int(nbr_iterations) else int(nbr_iterations)
    result = []
    for sub_index in range(nbr_iterations):
        start_ind = sub_index * nbr_simult if sub_index > 0 else 0
        sub_list = list_items[start_ind:start_ind + nbr_simult]
        result.append(sub_list)

    return result


def download_pdf_args(args) -> bool:
    """
    This function download a file and save on the disk at the path
    indicated in file_path.

    url, file_dir, file_name, timeout, max_attempt, max_waiting_time_sec = args
    """

    url, file_dir, file_name, timeout, max_attempt, max_waiting_time_sec = args
    max_attempt = 1 if not max_attempt else max_attempt
    waiting_time_step = 0  # 0 second
    current_waiting_time = 0  # in seconds
    if max_attempt and max_waiting_time_sec:
        waiting_time_step = int(max_waiting_time_sec / max_attempt)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/58.0.3029.110 Safari/537.3 "
    }

    load_page = True
    try_nbr = 0
    response = None
    is_success = True
    while load_page:
        is_success = True
        try:
            for atp in range(max_attempt + 1):
                time.sleep(current_waiting_time)  # wait before next request attempt
                response = requests.get(url, timeout=timeout, headers=headers, verify=False)
                if response.status_code == 429:  # If error is due to 'Too many requests'
                    if atp < max_attempt - 1:
                        current_waiting_time += waiting_time_step
                        print(f"\n  Too many requests: Will retry in {current_waiting_time} second(s)")

        except BaseException as e:
            is_success = False
            print(e)
            # Save the error in logs
            LogEvent(level=LogLevel.ERROR.value,
                     message=e.__str__(),
                     function_name=inspect.currentframe().f_code.co_name,
                     exception=e.__str__()).save()
            break

        # If no error
        if response.status_code == 200:
            content_type = response.headers.get('Content-Type')
            # Check if the type of the downloaded is in the list of expected files types
            ext = [f_type for f_type in CONFIG["general"]["file_types"] if f_type.lower() in content_type.lower()]
            if ext:
                # Get file extension
                ext = ext[0]
                full_file_path = os.path.join(file_dir, file_name + "." + ext)
                try:
                    with open(full_file_path, 'wb') as file:
                        file.write(response.content)
                except BaseException as e:
                    is_success = False
                    # In case of error when trying to save the pdf...
                    print("  Error: ", e.__str__())
                    # Save the error in logs
                    LogEvent(level=LogLevel.DEBUG.value,
                             message=f"Error while saving at {full_file_path}, url: {url}",
                             function_name=inspect.currentframe().f_code.co_name,
                             exception=e.__str__()).save()
                break
            else:  # The downloaded file type is not in the list of file's type of interest
                # Try to load the page of the url and another pdf file if available
                if "html" in content_type:
                    try_nbr += 1
                    if try_nbr < 2:
                        resp, soup = get_page_from_url(url=url, timeout=timeout, get_response=True)
                        if resp is None or soup is None:
                            is_success = False
                            break
                        parsed_url = urlparse(resp.url)
                        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                        links = soup.find_all("a")

                        if len(links):
                            link = [lk for lk in links if
                                    lk is not None and "href" in lk.attrs and ".pdf" in lk["href"]]
                            if len(link):
                                url = add_base_url_if_missing(base_url=base_url, url=link[0]["href"])
                                load_page = True  # Go back and re-try with this new url
                        else:
                            is_success = False
                            break  # stop trying getting another link
                    else:
                        is_success = False
                        break  # stop trying getting another link
                else:
                    is_success = False
                    msg = f"Skip Warning: file type is not in the list of file's types of interest. expecting " \
                          f"{'/ '.join(CONFIG['general']['file_types'])} but got {content_type}"
                    print(f"  {msg}")
                    # Save the error in logs
                    LogEvent(level=LogLevel.WARNING.value,
                             message=f"{msg}, url: {url}",
                             function_name=inspect.currentframe().f_code.co_name
                             ).save()
                    break
        else:
            is_success = False
            break

    if response is not None and response.status_code != 200:
        is_success = False
        msg = f"Failed to download PDF file. Status code: {response.status_code}, url: {url}"
        print(f"  {msg}")
        # Save the error in logs
        LogEvent(level=LogLevel.ERROR.value,
                 message=msg,
                 function_name=inspect.currentframe().f_code.co_name,
                 exception=get_request_response_error(response=response)).save()

    return is_success


def hash_md5(data: str) -> str:
    """
    Hash string `data` into a 32-character string using MD5 algorithm
    :param data: The input string to be hashed
    :return: The hashed string
    """
    # Create an MD5 hash object
    md5_hash = hashlib.md5()

    # Convert the string to bytes and update the hash object
    md5_hash.update(data.encode('utf-8'))

    # Get the hexadecimal representation of the hash
    hashed_string = md5_hash.hexdigest()

    return hashed_string


def format_file_name(org_acronym: str, org_region: str, publication_title: str, lang='') -> str:
    """
    This function format the name of a pdf file by creating a new name
    that complies with the Naming convention

    org_acronym: The acronym of the organization that published the document publication_title: The original name of the 
    pdf as it was on the organization website lang: The ISO 639-1 Code of the language (eg. 'EN' for English, 
    'FR' for French), in case the same document was published in different languages. 
    """

    org_acronym_region = f"{org_acronym}-{org_region}".replace(" ", "-")

    # Hash publication_title
    hashed_name = hash_md5(data=publication_title)
    # print(edited_publication_title)

    # Returning the new formatted name that complies with the naming convention
    lang = lang if len(lang) <= 32 else hash_md5(data=lang)  # lang argument should not be too long (we set the
    lang = lang.replace("_", "-").replace(" ", "-")
    # maximum to 50. If too long then hash it
    new_name = f"{org_acronym_region.upper()}_{hashed_name}_{lang.upper()}" if lang \
        else f"{org_acronym_region.upper()}_{hashed_name}"

    return new_name.strip()


def format_language(lang: str) -> str:
    lang = lang.lower()
    un_languages_lang_dict = CONFIG['general']['un_languages']['lang_dict']
    un_languages_name = [un_languages_lang_dict[lg_key].lower() for lg_key in un_languages_lang_dict.keys()]
    un_languages_code2 = [lg.lower() for lg in CONFIG['general']['un_languages']['code2']]
    un_languages_code3 = [lg.lower() for lg in CONFIG['general']['un_languages']['code3']]
    if lang in un_languages_name:
        return un_languages_lang_dict[lang]

    # For cases where lang = "2020 ASDR - EXECUTIVE SUMMARY (ENGLISH)"
    # Check if a UN language exist in the string lang
    lang_in_string = [lg for lg in un_languages_name if lg in lang]
    # Check if there is only one language in lang_in_string
    if len(lang_in_string) == 1:
        lang_in_string = lang_in_string[0]
        # Check if the language is surrounded by brackets OR if the lang has the word "version" OR if lang ends
        # with lang_in_string
        if f"({lang_in_string})" in lang or f"{lang_in_string} version" in lang or f"version {lang_in_string}" in \
                lang or lang.endswith(lang_in_string):
            return un_languages_lang_dict[lang_in_string]

    # For languages code3 ('ENG', 'FRA', ...)
    # Check if a UN language exist in the string lang
    lang_in_string = [lg for lg in un_languages_code3 if f"({lg})" in lang]
    # Check if there is only one language in lang_in_string
    if len(lang_in_string) == 1:
        lang_in_string = lang_in_string[0]
        return un_languages_lang_dict[lang_in_string]

    # For languages code3 ('EN', 'FR', ...)
    # Check if a UN language exist in the string lang
    lang_in_string = [lg for lg in un_languages_code2 if f"({lg})" in lang or lg == lang]
    # Check if there is only one language in lang_in_string
    if len(lang_in_string) == 1:
        lang_in_string = lang_in_string[0]
        return un_languages_lang_dict[lang_in_string]

    return ""


def add_base_url_if_missing(base_url: str, url: str) -> str:
    """
    This function will add the base url (e.g https://www.undp.org) to any invalid url if missing
    :param base_url:
    :param url:
    :return:
    """
    if not is_valid_url(url=url) and not url.startswith(base_url):
        # Remove forward slashes / from the beginning of url and the end of download_base_url before building a
        # valid url
        url = base_url.rstrip("/") + "/" + url.lstrip("/")
    return fix_url(url=url)


# create and id for the current publication
def generate_document_id(organization_acronym: str, org_region: str, publication_title: str,
                         pdf_download_link: str):
    # create a formatted using based on the defined convention
    formatted_name = format_file_name(org_acronym=organization_acronym,
                                      org_region=org_region,
                                      publication_title=publication_title)

    hashed_pdf_download_link = hash_md5(data=pdf_download_link.replace('.pdf', ''))

    return formatted_name + "_" + hashed_pdf_download_link


def remove_keys_from_list_of_dicts(data: list, keys_list: list, to_remove: bool = True) -> list:
    if to_remove:
        # keys_list to remove
        keys_list_to_remove = keys_list.copy()
        for item in data:
            for key in keys_list_to_remove:
                item.pop(key, None)
    else:
        # keys_list to keep
        keys_list_to_keep = keys_list.copy()
        for item in data:
            keys_list_to_remove = [k for k in item.keys() if k not in keys_list_to_keep]
            for key in keys_list_to_remove:
                item.pop(key, None)
    return data


def clean_text(text: str) -> str:
    text = text.replace('\t', ' ')  # Remove tabulations
    text = text.replace('\n', ' ')  # Remove new line characters
    text = text.strip()  # Remove white spaces from left and right

    return text


def fix_url(url: str) -> str:
    """
    This function urls with 2 `http`s
    :param url:
    :return:
    """
    if "http" not in url:
        return url

    if "https://" not in url and "https:/" in url:
        url = url.replace("https:/", "https://")

    if "http://" not in url and "http:/" in url:
        url = url.replace("http:/", "http://")

    parts = url.split("http")
    if len(parts) <= 2:
        return url

    last_part = parts[-1]
    return "http" + last_part
