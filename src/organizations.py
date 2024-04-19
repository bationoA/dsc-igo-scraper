from .common import *


class Organization:

    def __init__(self, acronym: str, name: str, region: str, home_page_url: str, publication_urls: str,
                 _id: int = None):
        self.id = _id
        self.acronym = acronym
        self.name = name
        self.region = region
        self.home_page_url = home_page_url
        self.publication_urls = publication_urls

    def insert(self) -> bool:
        db_handler = DatabaseHandler()
        table_name = CONFIG["general"]["organizations_table"]
        data = remove_keys_from_list_of_dicts(data=[self.to_dict()], keys_list=["id"], to_remove=True)
        return db_handler.insert_data_into_table(
            table_name=table_name,
            data=data[0]
        )

    def to_dict(self) -> dict:
        return {
            'id': self.id,
            'acronym': self.acronym,
            'name': self.name,
            'region': self.region,
            'home_page_url': self.home_page_url,
            'publication_urls': self.publication_urls
        }


def dict_to_organization_object(_dict) -> Organization:
    return Organization(
        _id=_dict['id'],
        acronym=_dict['acronym'],
        name=_dict['name'],
        region=_dict['region'],
        home_page_url=_dict['home_page_url'],
        publication_urls=_dict['publication_urls']
    )


def get_organization_by_id(_id: int) -> Organization:
    """

    :param _id:
    :return:
    """
    db_handler = DatabaseHandler()
    result = db_handler.select_columns(CONFIG["general"]["organizations_table"],
                                       columns=["*"],
                                       condition="id = ?",
                                       condition_vals=(str(_id),)
                                       )

    return dict_to_organization_object(_dict=result[0])


def get_organization_by_condition(condition: str) -> Organization:
    """

    :param condition: Specify the condition for the select query
    :return:
    """
    db_handler = DatabaseHandler()
    columns = ["*"]
    result = db_handler.select_columns(CONFIG["general"]["organizations_table"],
                                       columns=columns, condition=condition)

    return dict_to_organization_object(_dict=result[0])
