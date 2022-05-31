# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
import os
import dotenv
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


def _find_doi_in_extra(item: dict) -> str:
    """Finds DOI in 'extra' field of `item` or returns an empty string.

    Args:
        item (dict): Zotero item.

    Returns:
        str: DOI
    """
    doi_regex = r'10\.\d+/[-;()\w.]+'
    if 'extra' in item['data'].keys():
        doi_id = re.findall(doi_regex, item['data']['extra'])
        if len(doi_id) > 0:
            return doi_id[0]
        else:
            return ''
    else:
        return ''


def _find_doi_in_arxiv_url(item: dict) -> str:
    """Finds arXiv DOI in 'url' field of `item` or returns an empty string.

    Args:
        item (dict): Zotero item.

    Returns:
        str: DOI
    """
    reg_arxiv_id = r'https*://arxiv.org/abs/(.*)'
    if 'url' in item['data'].keys():
        arxiv_id = re.findall(reg_arxiv_id, item['data']['url'])

        if len(arxiv_id) > 0:
            doi_prefix = '10.48550/arXiv.'
            return [doi_prefix+x for x in arxiv_id][0]
        else:
            return ''
    else:
        return ''


def _get_parent_and_doi(items: list, on_no_dois: str = 'ignore') -> dict:
    """
    Extract parent keys and DOI for all `items` containing
    this information.

    Args:
        items (list): List of Zotero items.

    Returns:
        dict: Dictionary with parent keys and DOI as corresponding values.
    """
    parent_doi = {}
    for item in items:
        item_key = item['key']

        # fetch DOI for items with field DOI (e.g. JournalArticles)
        doi = item['data'].get('DOI', '')
        parent_doi.update({item_key: doi}) if doi else False

        # fetch DOI with "Extra" field and a DOI within (e.g. Reports from
        # bioRxiv and medRxiv, Books)
        doi = _find_doi_in_extra(item)
        parent_doi.update({item_key: doi}) if doi else False

        # if arXiv ID present - create DOI from it as described in
        # https://blog.arxiv.org/2022/02/17/new-arxiv-articles-are-
        # now-automatically-assigned-dois/
        doi = _find_doi_in_arxiv_url(item)
        parent_doi.update({item_key: doi}) if doi else False

    if len(parent_doi) == 0 and on_no_dois == 'error':
        raise KeyError(
            'This collection has no items with associated DOI names.')
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
        items: list, attach_key: str, parent_doi: dict,
        on_no_dois: str = 'ignore') -> str:
    """
    Matches given `attach_key` in `items` to corresponding DOI name
    linked via parent ID in `parent_doi` dictionary.

    Args:
        items (list): List of Zotero items.
        attach_key (str): Key of attachment to be matched.
        parent_doi (dict): Known parent ID and DOI matches.

    Returns:
        str: Matching DOI name
    """
    attach_item = [x for x in items if x['key'] == attach_key]
    parent_key = attach_item[0]['data']['parentItem']
    if parent_key not in parent_doi and on_no_dois == 'error':
        raise KeyError(
            f'Attachment {attach_key} does not contain a matching DOI '
            f'parent in this collection')
    elif parent_key not in parent_doi and on_no_dois == 'ignore':
        return ''
    else:
        return parent_doi[parent_key]


def _expand_dict(id_dict: dict, keys: list, value2link: str) -> dict:
    """
    Creates new entries with key from `keys` and associated
    `value2link` in existing dictionary `id_dict`.

    Args:
        id_dict (dict): Existing dictionary with some keys and values.
        keys (list): List of keys to be added individually to `id_dict`.
        value2link (str): Value to assign to each of the `keys`.

    Returns:
        dict: Dictionary expanded with new keys and associated value.
    """
    for key in keys:
        if key in id_dict and value2link not in id_dict[key]:
            # attach to already scraped accession IDs
            id_dict[key].append(value2link)
        elif key not in id_dict:
            id_dict[key] = [value2link]
    return id_dict


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


def _find_hyphen_sequence(
        txt: str, pattern: str, after_hyphen: str) -> list:
    """Return all accession IDs from a hyphenated accession ID sequence, both
    'SRX100006-7' (below `after_hyphen` option '\\d+') and
    'SRX100006-SRX100007' (below `after_hyphen` option 'pattern')
    yield 'SRX100006, SRX100007'.

    Args:
        txt (str): Text to scrape through.
        pattern (str): Accession ID pattern to search for.
        after_hyphen (str): Pattern given after the hyphen (supported
        options include '\\d+' and pattern)

    Returns:
        list: List of accession IDs with hyphenated IDs included.
    """
    # source of hyphens: https://stackoverflow.com/a/48923796 with \u00ad added
    hyphens = r'[\u002D\u058A\u05BE\u1400\u1806\u2010-\u2015\u2E17\u2E1A\
    \u2E3A\u2E3B\u2E40\u301C\u3030\u30A0\uFE31\uFE32\uFE58\uFE63\uFF0D\
    \u00AD]'
    pattern_hyphen = pattern + r'\s*' + hyphens + r'\s*' + after_hyphen
    ids = []
    matches = re.findall(f'({pattern_hyphen})', txt)
    if len(matches) > 0:
        for match in matches:
            split_match = re.split(hyphens, match)

            if after_hyphen == r'\d+':
                # 3.a) "SRX100006-7" > "SRX100006, SRX100007"
                nb_digits = len(split_match[-1])
                base = split_match[0][:-nb_digits]
                start = split_match[0][-nb_digits:]
                end = split_match[-1][-nb_digits:]
            elif after_hyphen == pattern:
                # 3.b) "SRX100006-SRX100007" > "SRX100006, SRX100007"
                prefix_digit_split = re.split(r'(\d+)', split_match[0])
                base = prefix_digit_split[0]
                start = prefix_digit_split[1]
                end = re.split(r'(\d+)', split_match[-1])[1]
            for i in range(int(start), int(end) + 1):
                ids += [base + str(i)]
    return ids


def _find_accession_ids(txt: str, id_type: str) -> list:
    """Returns list of run, study, BioProject, experiment and
    sample IDs found in `txt`.

    Searching for these patterns of accession IDs that are all also
    supported by other q2fondue actions:
    BioProject ID: PRJ(E|D|N)[A-Z][0-9]+
    Study ID: (E|D|S)RP[0-9]{6,}
    Run ID: (E|D|S)RR[0-9]{6,}
    Experiment ID: (E|D|S)RX[0-9]{6,}
    Sample ID: (E|D|S)RS[0-9]{6,}


    Args:
        txt (str): Some text to search
        id_type (str): Type of ID to search for 'run', 'study', 'bioproject',
        'experiment' or 'sample'.

    Returns:
        list: List of run, study, BioProject, experiment or sample IDs found.
    """
    # DEFAULT: Find plain accession ID: PREFIX12345 or PREFIX 12345
    patterns = {
        'run': r'[EDS]RR\s?\d+', 'study': r'[EDS]RP\s?\d+',
        'bioproject': r'PRJ[EDN][A-Z]\s?\d+',
        'experiment': r'[EDS]RX\s?\d+',
        'sample': r'[EDS]RS\s?\d+',
    }
    pattern = patterns[id_type]

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

    # SPECIAL case 3: hyphenated sequence of IDs
    # "SRX100006-7" and "SRX100006-SRX100007" both yield "SRX100006, SRX100007"
    ids += _find_hyphen_sequence(txt, pattern, r'\d+')
    ids += _find_hyphen_sequence(txt, pattern, pattern)

    return list(set(ids))


def scrape_collection(
    collection_name: str, on_no_dois: str = 'ignore', log_level: str = 'INFO'
) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame):
    """
    Scrapes Zotero collection for accession IDs (run, study, BioProject,
    experiment and sample) and associated DOI names.

    Args:
        collection_name (str): Name of the collection to be scraped.
        on_no_dois (str): Behavior if no DOIs were found.
        log_level (str, default='INFO'): Logging level.

    Returns:
        (pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame):
        Dataframes with run, study, BioProject, experiment and sample IDs and
        associated DOI names scraped from Zotero collection.
    """
    logger.setLevel(log_level.upper())

    dotenv.load_dotenv()

    logger.info(
        f'Scraping accession IDs for collection "{collection_name}"...'
    )

    # initialise Zotero instance
    zot = zotero.Zotero(
        os.getenv('ZOTERO_USERID'),
        os.getenv('ZOTERO_TYPE'),
        os.getenv('ZOTERO_APIKEY'))

    # get collection id
    coll_id = _get_collection_id(zot, collection_name)

    # get all items of this collection (required for DOI extraction)
    items = zot.everything(zot.collection_items(coll_id))

    # get parent keys and corresponding DOI
    parent_doi = _get_parent_and_doi(items, on_no_dois)

    # get all attachment items keys of this collection (where pdf/html
    # snapshots are stored)
    attach_keys = _get_attachment_keys(items)
    logger.info(
        f'Found {len(attach_keys)} attachments to scrape through...'
    )

    # extract IDs from text of attachment items
    doi_dicts = {'run': {}, 'study': {}, 'bioproject': {}, 'experiment': {},
                 'sample': {}}

    for attach_key in attach_keys:
        # get doi linked with this attachment key
        doi = _link_attach_and_doi(items, attach_key, parent_doi, on_no_dois)

        # get text
        try:
            str_text = zot.fulltext_item(attach_key)['content']
            # remove character frequently placed before soft hyphen, see
            # https://stackoverflow.com/a/51976543
            str_text = str_text.replace('\xad', '')
        except zotero_errors.ResourceNotFound:
            str_text = ''
            logger.warning(f'Item {attach_key} doesn\'t contain any '
                           f'full-text content')

        # find accession IDs
        for id_type in doi_dicts.keys():
            ids = _find_accession_ids(str_text, id_type)
            # match found accession IDs with DOI
            doi_dicts[id_type] = _expand_dict(doi_dicts[id_type], ids, doi)

    if sum([len(v) for _, v in doi_dicts.items()]) == 0:
        raise NoAccessionIDs(f'The provided collection {collection_name} does '
                             f'not contain any accession IDs.')
    for id_type in doi_dicts.keys():
        if len(doi_dicts[id_type]) == 0:
            logger.warning(f'The provided collection {collection_name} '
                           f'does not contain any {id_type} IDs')

    dfs = []
    for doi_dict in doi_dicts.values():
        df = pd.DataFrame.from_dict([doi_dict]).transpose()
        df.columns = ['DOI']
        df.index.name = 'ID'
        dfs.append(df)

    return tuple(dfs)
