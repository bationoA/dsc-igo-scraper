"""
This file contains functions related to directories management
"""

import os
from .files_fc import CONFIG


def init_folders(config: dict, list_org_acronym_region: list):
    """
    This function create all the required directories related to data for this project:
    Folders for downloads and sql lite
    """
    # Download directories
    for organization_acronym_region in list_org_acronym_region:
        initialize_download_pdf_directory_for(organization_acronym_region=organization_acronym_region.lower(),
                                              config=config)

    # Make sure that the folders for SQL Lite exists. If not, create them
    initialize_sql_lite_database_folder()


def initialize_download_pdf_directory_for(organization_acronym_region: str, config: dict):
    """
    It creates a directory for a specific organization if the
    directory doesn't exist already
    """
    # print("Checking directories")
    # Generate the path to the download directory of pdfs for the current organization
    download_dir = generate_organization_download_pdf_directory_path(
        organization_acronym_region=organization_acronym_region, config=config)

    parent_folder = ""
    for folder in download_dir.split(os.sep):
        directory = os.path.join(parent_folder, folder)
        # Create the directory only if it doesn't exist
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Directory '{directory}' created successfully.")

        parent_folder = directory


def initialize_sql_lite_database_folder():
    parent_folder = ""
    for folder in CONFIG['general']['database']['sql_lite']['path']:
        directory = os.path.join(parent_folder, folder)

        # Create the directory only if it doesn't exist
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Directory '{directory}' created successfully.")

        parent_folder = directory


def is_pdf_already_exist(pdf_formatted_name: str, config: dict) -> bool:
    # Get the name of the organization from the name of the file
    org_name = pdf_formatted_name.split('_')[0]

    # Generate the path to the download directory of pdfs for the current organization
    download_dir = generate_organization_download_pdf_directory_path(organization_acronym_region=org_name,
                                                                     config=config)

    # Create path for the pdf file
    filepath = os.path.join(download_dir, pdf_formatted_name)
    return os.path.exists(filepath)


def generate_organization_download_pdf_directory_path(organization_acronym_region: str, config: dict):
    """
    It generates the path of the directory where the downloaded pdf will be stored. The format of the path is meant to
    match the all Operating System' path format.
    """
    acronym = organization_acronym_region.split("-")[0]  # Get only acronym
    region = "-".join(organization_acronym_region.split("-")[1:])  # Get only the region
    acronym = acronym.replace(" ", "")
    region = region.replace(" ", "-")

    if CONFIG["on_azure_jupyter_cloud"]:
        # Use Azure jupyter cloud folder's path
        return os.path.join(*config['general']['azure_downloaded_pdfs_relative_path'], acronym, region).lower()
    else:
        # Use local machine folder's path
        return os.path.join(*config['general']['local_downloaded_pdfs_relative_path'], acronym, region).lower()
