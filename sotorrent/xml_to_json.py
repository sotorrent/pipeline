import logging

import xmltodict

logger = logging.getLogger(__name__)
_dict_elements = []


def _handle_row(path, _):
    _dict_elements.append((path[1][1]))  # this accesses the attributes of a row element in the XML dump files
    return True


def parse_xml_into_dict(file):
    """
    Parse XML file into a dict
    :param file: path to XML file
    :return: list of dicts with content of XML file
    """

    logger.info(f"Parsing XML file '{file}'...")
    with open(file, 'r', encoding='utf-8') as fp:
        xmltodict.parse(fp.read(), item_depth=2, item_callback=_handle_row)
    logger.info(f"Parsed '{len(_dict_elements)}' elements.")

    return _dict_elements
