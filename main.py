import inspect
from src import App, SESSION, scraper_instances
from src.db_handler import reset_temp_publications_urls_table, reset_temp_documents_table
from src.files_fc import LogEvent, LogLevel, CONFIG, SESSION_ERRORS

bar_length = len(App['name']) + 6
print('=' * bar_length)
print(f"* {App['name']}  *")
print(f"* Version: {App['version']}  *")
print(f"* Release: {App['release']}  *")
print(f"* Session ID: {SESSION.id}  *")
print(f"* Started at: {SESSION.started_at}  *")
print(f"* Publications chunk size: {CONFIG['general']['max_publication_urls_chunk_size']}  *")
print(f"* PDFs chunk size: {CONFIG['general']['max_document_links_chunk_size']}  *")
print(f"* Allow parallel downloads: {CONFIG['general']['allow_parallel_downloads']}  *")
if CONFIG["general"]["allow_parallel_downloads"]:
    print(f"* Concurrent downloads: {CONFIG['general']['max_concurrent_downloads']}  *")
print('=' * bar_length)
print("\n")

total_organization = len(scraper_instances)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    nbr_pdfs_found = 0  # number of pdfs found in current session
    nbr_down_pdfs = 0  # number of downloaded pdfs in current session
    msg = ""
    run_result = True

    print(f"Website assessed: 0")
    for i, p in enumerate(scraper_instances):
        # Reset temporary tables
        reset_temp_publications_urls_table()
        reset_temp_documents_table()

        scraper = p['scraper']

        # Save event in logs
        msg = f"Working on {p['name']}'s publications..."
        print(msg)
        LogEvent(level=LogLevel.INFO.value,
                 message=msg,
                 function_name=inspect.currentframe().f_code.co_name).save()

        # Start scraping pdfs from the current organization
        run_result = scraper.run()

        print(f"\nWebsite(s) assessed: {i + 1}/{total_organization}")

        # If run_result is False then an error might have happened
        if not run_result:
            msg = "The process was interrupted. Check your internet connection and/or the website's link. See logs.\n"
            print(msg)
            break

        nbr_pdfs_found = scraper.number_of_pdfs_found_in_current_session  # Increment number of pdfs found
        nbr_down_pdfs += scraper.number_of_downloaded_pdfs_in_current_session  # Increment number of downloaded pdfs

        if i < len(scraper_instances) - 1:
            print('-' * bar_length, "\n")
    # ---- Complete Scrapping
    SESSION.errors_number = SESSION_ERRORS["session"]["errors_number"]
    SESSION.interrupt()  # End session

    # -- Report
    str_0 = "Web scrapping complete.\n" if run_result else msg
    msg = str_0
    str_1 = f" -> A total explored website(s): {total_organization}"
    msg += f"{str_1}\n"
    downloaded_docs_percent = round(100 * nbr_down_pdfs / nbr_pdfs_found, 2) if nbr_pdfs_found > 0 else 0
    msg += f" -> Total pdfs downloaded: {nbr_down_pdfs} / {nbr_pdfs_found} " \
           f"-- {downloaded_docs_percent}%\n"

    msg += f"* End time: {SESSION.ended_at}"

    LogEvent(level=LogLevel.INFO.value,
             message=msg).save()

    print("\n")
    bar_length = max(len(str_0), len(str_1)) + 1
    print('*' * bar_length)
    print(msg)
    print('*' * bar_length)
