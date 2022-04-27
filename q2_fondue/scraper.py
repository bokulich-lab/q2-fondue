# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
import re
import pandas as pd
from pyzotero import zotero, zotero_errors
from q2_fondue.entrezpy_clients._utils import set_up_logger

logger = set_up_logger('INFO', logger_name=__name__)


class NoAccessionIDs(Exception):
    pass


def _get_collection_id(zot: zotero.Zotero, col_name: str) -> str:
    """
    Returns collection ID given the name of a Zotero collection

    Args:
        zot (zotero.Zotero): Zotero instance
        col_name (str): Name of collection
    Returns:
        str: Collection ID.
    """

    # get all collections in this zot instance
    # note w/o zot.everything only max. 100 items are retrieved
    all_col = zot.everything(zot.collections())

    # retrieve name and key of all collections
    name_key = {x['data']['name']: x['key'] for x in all_col}

    # return col_name's key
    try:
        col_id = name_key[col_name]
    except KeyError:
        raise KeyError(
            f'Provided collection name {col_name} does not '
            f'exist in this library')

    return col_id


def _get_attachment_keys(items) -> list:
    """Retrieves attachment keys of attachments in provided list of items.

    Args:
        items (list): List of Zotero items.

    Returns:
        list: List of attachment keys.
    """
    attach = [x for x in items if x['data']['itemType'] == 'attachment']
    if len(attach) == 0:
        raise KeyError(
            'No attachments exist in this collection')
    else:
        attach_keys = sorted(list(set([x['key'] for x in attach])))
        return attach_keys


def _find_special_id(txt, pattern, split_str) -> list:
    """Creates an accession ID from starting characters in `pattern` and
    digits following `split_str` in `txt`.

    Args:
        txt (str): Text to search for ID
        pattern (str): Pattern containing at the start the character prefix and
                       at the end the remaining digits of the accession ID
        split_str (str): String separating the digit part of the ID

    Returns:
        list: List with accession ID.
    """
    match = re.findall(f'({pattern})', txt)
    ids = []
    if len(match) != 0:
        for match in match:
            split_match = match.split(split_str)
            prefix = re.findall("[A-Z]+", split_match[0])[0]
            number = split_match[-1].strip()
            ids += [prefix + number]
    return ids


def _find_accession_ids(txt, ID_type) -> list:
    """Returns list of run or BioProject IDs found in `txt`.

    Searching for these patterns of accession IDs as they are
    currently supported by q2fondue:
    ProjectID: PRJ(E|D|N)[A-Z][0-9]+
    runID: (E|D|S)RR[0-9]{6,}

    Args:
        txt (str): Some text to search
        ID_type (str): Type of ID to search for ('run' or 'bioproject')

    Returns:
        list: List of run or BioProject IDs found.
    """
    # DEFAULT: Find plain accession ID: PREFIX12345 or PREFIX 12345
    if ID_type == 'run':
        pattern = r'[EDS]RR\s?\d+'
    elif ID_type == 'bioproject':
        pattern = r'PRJ[EDN][A-Z]\s?\d+'
    ids = re.findall(f'({pattern})', txt)
    # remove potential whitespace
    ids = [x.replace(' ', '') for x in ids]

    # SPECIAL case 1: get IDs after comma:
    # "PREFIX12345, 56789" yields "PREFIX56789"
    for nb_comma in range(1, 11):
        pattern_comma = pattern + nb_comma * r',\s\d+'
        ids_match = _find_special_id(txt, pattern_comma, ',')
        if len(ids_match) == 0:
            pattern_comma = pattern + (nb_comma - 1) * r',\s\d*'
            break
        else:
            ids += ids_match

    # SPECIAL case 2: get IDs after and:
    # "PREFIX12345, 56789 and 67899" yields "PREFIX67899"
    pattern_and = pattern_comma + r'\sand\s\d+'
    ids += _find_special_id(txt, pattern_and, 'and')

    return list(set(ids))


def scrape_collection(
    library_type: str, user_id: str, api_key: str, collection_name: str,
    log_level: str = 'INFO'
) -> (pd.Series, pd.Series):
    """
    Scrapes Zotero collection for run and BioProject IDs.

    Args:
        library_type (str): Zotero API library type
        user_id (str): Valid Zotero API userID (for library_type 'user'
            extract from https://www.zotero.org/settings/keys, for 'group'
            extract by hovering over group name in
            https://www.zotero.org/groups/).
        api_key (str): Valid Zotero API user key (retrieve from
            https://www.zotero.org/settings/keys/new checking
            'Allow library access').
        collection_name (str): Name of the collection to be scraped.
        log_level (str, default='INFO'): Logging level.

    Returns:
        pd.Series: Series with run and BioProject IDs scraped from collection.
    """
    logger.setLevel(log_level.upper())

    logger.info(
        f'Scraping accession IDs for collection "{collection_name}" in '
        f'{library_type} library with user ID {user_id}...'
    )

    # initialise Zotero instance
    zot = zotero.Zotero(
        user_id,
        library_type,
        api_key)

    # get collection id
    coll_id = _get_collection_id(zot, collection_name)

    # get all items of this collection (required for DOI extraction)
    items = zot.everything(zot.collection_items(coll_id))

    # todo: input - items / output - parentitem_key_doi
    # extract item_id and doi/isbn for all items in this collection with
    # this key (items of type "attachment" don't have this key within)
    parentitem_key_doi = {}
    for key_token in ['DOI', 'ISBN']:
        parentitem_key_doi.update({x['key']: x['data'][key_token]
                                   for x in items
                                   if key_token in x['data'].keys()})

    # get all attachment items keys of this collection (where pdf/html
    # snapshots are stored)
    attach_keys = _get_attachment_keys(items)
    logger.info(
        f'Found {len(attach_keys)} attachments to scrape through...'
    )

    # extract IDs from text of attachment items
    run_ids, bioproject_ids = [], []
    for attach_key in attach_keys:
        # todo: create function _find_doi() & test it
        # # get doi/isbn linked with this attachment
        # attach_item = [x for x in items if x['key'] == attach_key]
        # # note: assumption 1 attach has only 1 parent item
        # parent_item = attach_item[0]['data']['parentItem']
        # doi_isbn = parentitem_key_doi[parent_item]

        # get text
        try:
            str_text = zot.fulltext_item(attach_key)['content']
        except zotero_errors.ResourceNotFound:
            str_text = ''
            logger.warning(f'Item {attach_key} doesn\'t contain any '
                           f'full-text content')

        # find run IDs
        # todo: create 2 dict with key being doi & update if it already exists
        run_ids += _find_accession_ids(str_text, 'run')
        # find bioproject IDs
        bioproject_ids += _find_accession_ids(str_text, 'bioproject')

    # remove duplicate entries in both lists
    run_ids = list(set(run_ids))
    bioproject_ids = list(set(bioproject_ids))

    if (len(run_ids) == 0) & (len(bioproject_ids) == 0):
        raise NoAccessionIDs(f'The provided collection {collection_name} '
                             f'does not contain any run or BioProject IDs')
    elif len(run_ids) == 0:
        logger.warning(f'The provided collection {collection_name} '
                       f'does not contain any run IDs')
    elif len(bioproject_ids) == 0:
        logger.warning(f'The provided collection {collection_name} '
                       f'does not contain any BioProject IDs')
    return (pd.Series(run_ids, name='ID'),
            pd.Series(bioproject_ids, name='ID'))
