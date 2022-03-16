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


class NoAccessionIDs(Exception):
    pass


def _get_collection_id(
        zot: zotero.Zotero, col_name: str):
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
    ls_all_col = zot.everything(zot.collections())

    # retrieve name and key of all collections
    dic_name_key = {x['data']['name']: x['key'] for x in ls_all_col}

    # return col_name's key
    try:
        col_id = dic_name_key[col_name]
    except KeyError:
        raise KeyError(
            f'Provided collection name {col_name} does not '
            f'exist in this library')

    return col_id


def _get_attachment_keys(zot, coll_id):
    """Retrieves attachment keys of attachments in provided collection.

    Args:
        zot (zotero.Zotero): Zotero instance
        coll_id (_type_): Collection ID.

    Returns:
        list: List of attachment keys.
    """
    ls_attach = zot.everything(
        zot.collection_items(coll_id, itemType='attachment'))
    if len(ls_attach) == 0:
        raise KeyError(
            'No attachments exist in this collection')
    else:
        ls_attach_keys = list(set([x['key'] for x in ls_attach]))
        return ls_attach_keys


def _find_accessionIDs(txt):
    """Returns list of run and BioProject IDs found in `txt`.

        Searching for these patterns of accession IDs as they are
        currently supported by q2fondue:
        ProjectID: PRJ(E|D|N)[A-Z][0-9]+
        runID: (E|D|S)RR[0-9]{6,}

        Args:
            txt (str): Some text to search

        Returns:
            list: List of run and BioProject IDs found.
        """
    str_runid = r'[EDS]RR\d*'
    str_projectid = r'PRJ[EDN][A-Z]\d*'
    ls_ids = re.findall(f'({str_runid}|{str_projectid})', txt)

    # todo: make sure they scraped IDs are valid:
    # todo: removing blank space, removing links, ...
    return list(set(ls_ids))


def scrape_collection(
    library_type: str, library_id: str, api_key: str, collection_name: str
) -> pd.Series:
    """
    Scrapes Zotero collection for run and BioProject IDs.

    Args:
        library_type (str): Zotero API library type
        library_id (str): Valid Zotero API userID (for library_type 'user'
            extract from https://www.zotero.org/settings/keys, for 'group'
            extract by hovering over group name in
            https://www.zotero.org/groups/)
        api_key (str): Valid Zotero API user key (retrieve from
            https://www.zotero.org/settings/keys/new checking
            'Allow library access').
        collection_name (str): Name of the collection to be scraped.

    Returns:
        pd.Series: Series with run and Bioproject IDs scraped from collection.
    """
    logger = set_up_logger('INFO', logger_name=__name__)

    logger.info(
        f'Scraping accession IDs for collection "{collection_name}" in '
        f'{library_type} library with library ID {library_id}...'
    )

    # initialise Zotero instance
    zot = zotero.Zotero(
        library_id,
        library_type,
        api_key)

    # get collection id
    coll_id = _get_collection_id(zot, collection_name)

    # get all attachment items keys of this collection (where pdf/html
    # snapshots are stored)
    ls_attach_keys = _get_attachment_keys(zot, coll_id)
    logger.info(
        f'Found {len(ls_attach_keys)} attachments to scrape through...'
    )

    # extract IDs from text of attachment items
    ls_ids = []
    for attach_key in ls_attach_keys:
        # get text
        try:
            str_text = zot.fulltext_item(attach_key)['content']
        except zotero_errors.ResourceNotFound:
            str_text = ''
            logger.warning(f'Item {attach_key} doesn\'t contain any '
                           f'full-text content')

        # find accession IDs
        ls_ids += _find_accessionIDs(str_text)

    if len(ls_ids) == 0:
        raise NoAccessionIDs(f'The provided collection {collection_name} '
                             f'does not contain any run or Bioproject IDs')
    else:
        return pd.Series({'id': ls_ids})
