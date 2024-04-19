# __init__.py

# Import specific modules or classes to make them directly accessible when importing the package
import os

from .db_handler import init_database
from .dir_fc import init_folders
from .files_fc import update_lst_err, CONFIG, SESSION_ERRORS, init_logs, load_yaml
from .ilo import IloGlobalScraper, IloBeirutScraper, IloCentralAmericaScraper, IloLatinAmericaCaribbeanScraper, \
    IloPhilippinesScraper
from .session import Session
from .un import UnGlobalScraper
from .unaids import UnaidsGlobalScraper
from .uncdf import UncdfGlobalScraper
from .undp import UndpGlobalScraper, UndpAfricaScraper, UndpArabStatesScraper, UndpAsiaAndThePacificScraper, \
    UndpEuropeAndTheCommonwealthOfIndependentStatesScraper, UndpLatinAmericaAndTheCaribbeanScraper
from .unep import UnepGlobalScraper, UnepWedocsScraper
from .unhabitat import UnHabitatGlobalScraper
from .unicef import UnicefGlobalScraper, EastAsiaAndPacificScraper, EasternAndSouthernAfricaScraper, \
    MiddleEastAndNorthAfricaScraper, SouthAsiaScraper, WestAndCentralAfricaScraper
from .unwto import UnwtoGlobalScraper
from .wfp import WfpGlobalScraper
from .who import WhoGlobalScraper, WhoAfricaScraper, WhoAmericaPahoScraper, WhoSouthEastAsiaScraper, WhoEuropeScraper, \
    WhoWesternPacificScraper, WhoEasternMediterraneanScraper
from .wipo import WipoGlobalScraper
from .worldbank import OpenKnowledgeRepoScraper, DocumentsReportsScraper

# Perform any initialization or setup tasks for the package
# Define package-level variables or constants
__name__ = "UN Data Futures Platform Pipeline"
__version__ = '1.0.0'
__author__ = 'Bationo A., Praet S., Skrynnyk M.'
__maintainer__ = "maintainer"
__description__ = "Web-scraping pipelines to collect publications from websites of intergovernmental organisations. "
__summary__ = "Web-scraping pipelines to collect publications from websites of intergovernmental organisations. "
__license__ = "GPL-3.0 license "
__release__ = "v0.12.0"

print(f"Initializing {__name__} package...")

# Get registered scrapers
registered_scrapers = load_yaml(filepath=os.path.join('src', 'scrapers_register.yaml'))
registered_scraper_names = list(registered_scrapers.keys())
# Get registered active scrapers
active_scrapers = [{sp: registered_scrapers[sp]} for sp in registered_scraper_names if
                   registered_scrapers[sp]['status']['active']]
active_scraper_names = [list(sp.keys())[0] for sp in active_scrapers]

# Create all required directories
init_folders(config=CONFIG, list_org_acronym_region=active_scraper_names)
# Initialize database and create tables if they don't exist
init_database()
# Set the session id
SESSION = Session()

# Update the session in config file so all modules have access to the current session's id
SESSION_ERRORS['session']['id'] = SESSION.id
update_lst_err()  # Update the session id of the object `session errors`
# Get App info
App = {
    "version": __version__,
    "author": __author__,
    "name": __name__,
    "license": __license__,
    "release": __release__
}

# Optionally, define a list of modules to be imported when using the wildcard import *
# __all__ = ['UndpWebScraper', 'Common']

# Initializing scrapers
scraper_instances = [
    {
        "name": "UNDP-Global",
        "scraper": UndpGlobalScraper(session=SESSION)
    },
    {
        "name": "UNDP-Africa",
        "scraper": UndpAfricaScraper(session=SESSION)
    },
    {
        "name": "UNDP-Arab-States",
        "scraper": UndpArabStatesScraper(session=SESSION)
    },
    {
        "name": "UNDP-Asia-and-the-Pacific",
        "scraper": UndpAsiaAndThePacificScraper(session=SESSION)
    },
    {
        "name": "UNDP-Europe-and-the-Commonwealth-of-Independent-States",
        "scraper": UndpEuropeAndTheCommonwealthOfIndependentStatesScraper(session=SESSION)
    },
    {
        "name": "UNDP-Latin-America-and-the-Caribbean",
        "scraper": UndpLatinAmericaAndTheCaribbeanScraper(session=SESSION)
    },
    {
        "name": "WHO-Global",
        "scraper": WhoGlobalScraper(session=SESSION)
    },
    {
        "name": "WHO-Africa",
        "scraper": WhoAfricaScraper(session=SESSION)
    },
    {
        "name": "WHO-America-PAHO",
        "scraper": WhoAmericaPahoScraper(session=SESSION)
    },
    {
        "name": "WHO-South-East-Asia",
        "scraper": WhoSouthEastAsiaScraper(session=SESSION)
    },
    {
        "name": "WHO-Europe",
        "scraper": WhoEuropeScraper(session=SESSION)
    },
    {
        "name": "WHO-Western-Pacific",
        "scraper": WhoWesternPacificScraper(session=SESSION)
    },
    {
        "name": "WHO-Eastern-Mediterranean",
        "scraper": WhoEasternMediterraneanScraper(session=SESSION)
    },
    {
        "name": "UN-Global",
        "scraper": UnGlobalScraper(session=SESSION)
    },
    {
        "name": "WORLDBANK-Openknowledge",
        "scraper": OpenKnowledgeRepoScraper(session=SESSION)
    },
    {
        "name": "WORLDBANK-Documents-and-Reports",
        "scraper": DocumentsReportsScraper(session=SESSION)
    },
    {
        "name": "UNICEF-Global",
        "scraper": UnicefGlobalScraper(session=SESSION)
    },
    {
        "name": "UNICEF-East-Asia-and-Pacific",
        "scraper": EastAsiaAndPacificScraper(session=SESSION)
    },
    {
        "name": "UNICEF-Eastern-and-Southern-Africa",
        "scraper": EasternAndSouthernAfricaScraper(session=SESSION)
    },
    {
        "name": "UNICEF-Middle-East-and-North-Africa",
        "scraper": MiddleEastAndNorthAfricaScraper(session=SESSION)
    },
    {
        "name": "UNICEF-South-Asia",
        "scraper": SouthAsiaScraper(session=SESSION)
    },
    {
        "name": "UNICEF-West-and-Central-Africa",
        "scraper": WestAndCentralAfricaScraper(session=SESSION)
    },
    {
        "name": "UNAIDS-Global",
        "scraper": UnaidsGlobalScraper(session=SESSION)
    },
    {
        "name": "UNEP-Global",
        "scraper": UnepGlobalScraper(session=SESSION)
    },
    {
        "name": "UNEP-Wedocs",
        "scraper": UnepWedocsScraper(session=SESSION)
    },
    {
        "name": "WFP-Global",
        "scraper": WfpGlobalScraper(session=SESSION)
    },
    {
        "name": "UNHabitat-Global",
        "scraper": UnHabitatGlobalScraper(session=SESSION)
    },
    {
        "name": "UNWTO-Global",
        "scraper": UnwtoGlobalScraper(session=SESSION)
    },
    {
        "name": "ILO-Global",
        "scraper": IloGlobalScraper(session=SESSION)
    },
    {
        "name": "ILO-Beirut",
        "scraper": IloBeirutScraper(session=SESSION)
    },
    {
        "name": "ILO-Central-America",
        "scraper": IloCentralAmericaScraper(session=SESSION)
    },
    {
        "name": "ILO-Latin-America-And-The-Caribbean",
        "scraper": IloLatinAmericaCaribbeanScraper(session=SESSION)
    },
    {
        "name": "ILO-Philippines",
        "scraper": IloPhilippinesScraper(session=SESSION)
    },
    {
        "name": "UNCDF-Global",
        "scraper": UncdfGlobalScraper(session=SESSION)
    },
    {
        "name": "WIPO-Global",
        "scraper": WipoGlobalScraper(session=SESSION)
    },
]

# Filter scrapers' classes by removing the ones that are not active
scraper_instances = [sp for sp in scraper_instances if sp['name'].lower() in active_scraper_names]
