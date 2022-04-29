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


def _get_parent_and_doi(items: list) -> dict:
    """
    Extract parent keys and DOI/ISBN for all `items` containing
    this information (note: items of type \"attachment\" don't have this key)

    Args:
        items (list): List of Zotero items.

    Returns:
        dict: Dictionary with parent keys and DOI/ISBN as corresponding values.
    """
    parent_doi = {}
    for key_token in ['DOI', 'ISBN']:
        parent_doi.update({x['key']: x['data'][key_token]
                           for x in items
                           if key_token in x['data'].keys()})
    if len(parent_doi) == 0:
        raise KeyError(
            'This collection has no items with associated DOI/ISBN names.')
    return parent_doi


def _get_attachment_keys(items: list) -> list:
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


def _link_attach_and_doi(
        items: list, attach_key: str, parent_doi: dict) -> str:
    """
    Matches given `attach_key` in `items` to corresponding DOI/ISBN name
    linked via parent ID in `parent_doi` dictionary.

    Args:
        items (list): List of Zotero items.
        attach_key (str): Key of attachment to be matched.
        parent_doi (dict): Known parent ID and DOI/ISBN matches.

    Returns:
        str: Matching DOI/ISBN name
    """
    attach_item = [x for x in items if x['key'] == attach_key]
    parent_key = attach_item[0]['data']['parentItem']
    if parent_key not in parent_doi:
        raise KeyError(
            f'Attachment {attach_key} does not contain a matching DOI/ISBN '
            f'parent in this collection')
    else:
        return parent_doi[parent_key]


def _merge2dict(id_dict: dict, keys: list, value2link: str) -> dict:
    """
    Creates new entries with key from `keys` and associated
    `value2link` in existing dictionary `id_dict`.

    Args:
        id_dict (dict): Existing dictionary with some keys and values.
        keys (list): List of keys to be added individually to `id_dict`.
        value2link (str): Value to assign to each of the `keys`.

    Returns:
        dict: Dictionary extended with new keys and associated value.
    """
    for key in keys:
        if key in id_dict:
            # attach to already scraped accession IDs
            id_dict[key].append(value2link)
            # remove duplicate values & sort
            id_dict[key] = sorted(list(set(id_dict[key])))
        else:
            id_dict[key] = [value2link]
    return id_dict


def _transform_dict_to_df(orig_dict: dict) -> pd.DataFrame:
    """Transforms provided `orig_dict` to a dataframe with indices being
    keys of `orig_dict`` and corresponding column entries being its values.

    Args:
        orig_dict (dict): Dictionary with accession IDs as keys and
        DOI/ISBN as values.

    Returns:
        pd.DataFrame: dataframe with indices being keys of `orig_dict`
        and corresponding column entries being its values.
    """
    new_df = pd.DataFrame.from_dict(
        orig_dict, orient='index', columns=['DOI'])
    new_df.index.name = 'ID'
    return new_df


def _find_special_id(txt: str, pattern: str, split_str: str) -> list:
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


def _find_accession_ids(txt: str, ID_type: str) -> list:
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
) -> (pd.DataFrame, pd.DataFrame):
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

    # get parent keys and corresponding DOI/ISBN
    parent_doi = _get_parent_and_doi(items)

    # get all attachment items keys of this collection (where pdf/html
    # snapshots are stored)
    attach_keys = _get_attachment_keys(items)
    logger.info(
        f'Found {len(attach_keys)} attachments to scrape through...'
    )

    # extract IDs from text of attachment items
    run_doi, bioproject_doi = {}, {}

    for attach_key in attach_keys:
        # get doi/isbn linked with this attachment key
        doi_isbn = _link_attach_and_doi(items, attach_key, parent_doi)

        # get text
        try:
            str_text = zot.fulltext_item(attach_key)['content']
        except zotero_errors.ResourceNotFound:
            str_text = ''
            logger.warning(f'Item {attach_key} doesn\'t contain any '
                           f'full-text content')

        # find accession (run and bioproject) IDs
        run_ids = _find_accession_ids(str_text, 'run')
        bioproject_ids = _find_accession_ids(str_text, 'bioproject')

        # match found accession IDs with DOI
        run_doi = _merge2dict(run_doi, run_ids, doi_isbn)
        bioproject_doi = _merge2dict(
            bioproject_doi, bioproject_ids, doi_isbn)

    if (len(run_doi) == 0) & (len(bioproject_doi) == 0):
        raise NoAccessionIDs(f'The provided collection {collection_name} '
                             f'does not contain any run or BioProject IDs')
    elif len(run_doi) == 0:
        logger.warning(f'The provided collection {collection_name} '
                       f'does not contain any run IDs')
    elif len(bioproject_doi) == 0:
        logger.warning(f'The provided collection {collection_name} '
                       f'does not contain any BioProject IDs')

    # transform dict to dataframe & return
    return (_transform_dict_to_df(run_doi),
            _transform_dict_to_df(bioproject_doi))
