"""
Shared query building utilities used by both bulk loading and OGM.
"""
from typing import List, Union


def get_label_string_from_list_of_labels(labels: Union[List[str], None]) -> str:
    """
    Get a label string from a list of labels. If the list is empty, return an empty string.
    :param labels: List of labels
    :return: Label string
    """
    if labels:
        return f":{':'.join(labels)}"
    else:
        return ""


