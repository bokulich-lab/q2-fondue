# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
import json
import pandas as pd
import logging
from qiime2.plugin.testing import TestPluginBase
from pandas._testing import assert_frame_equal
from unittest.mock import patch
from pyzotero import zotero, zotero_errors
from q2_fondue.scraper import (
    _find_special_id,
    _get_collection_id, _find_accession_ids,
    _get_parent_and_doi, _merge2dict,
    _transform_dict_to_df,
    _link_attach_and_doi,
    _get_attachment_keys, scrape_collection,
    NoAccessionIDs
)


class TestUtils4CollectionScraping(TestPluginBase):
    package = 'q2_fondue.tests'

    def setUp(self):
        super().setUp()
        self.zot = zotero.Zotero(12345, "user", "myuserkey")

    def _open_json_file(self, filename):
        path2col = self.get_data_path(filename)
        file = open(path2col)
        return json.load(file)

    def _open_txt_file(self, filename):
        path2file = self.get_data_path(filename)
        txt_file = open(path2file, "r")
        return json.loads(txt_file.read())

    def _create_doi_id_dataframe(self, id_ls, doi_ls):
        df = pd.DataFrame(columns=['DOI'], data=doi_ls,
                          index=id_ls)
        df.index.name = 'ID'
        return df

    @patch.object(zotero.Zotero, 'everything')
    @patch.object(zotero.Zotero, 'collections')
    def test_get_correct_collectionID(self, patch_col, patch_ever):
        patch_ever.return_value = self._open_json_file(
            'scraper_collections.json')

        exp_id = '9MK5KS97'
        returned_id = _get_collection_id(self.zot, 'Humanities')
        self.assertEqual(exp_id, returned_id)

    @patch.object(zotero.Zotero, 'everything')
    @patch.object(zotero.Zotero, 'collections')
    def test_get_collectionID_raiseError(self, patch_col, patch_ever):
        patch_ever.return_value = self._open_json_file(
            'scraper_collections.json')

        col_name = 'BlaBla'
        with self.assertRaisesRegex(
            KeyError, f'name {col_name} does not exist'
        ):
            _get_collection_id(self.zot, col_name)

    def test_get_parent_and_doi(self):
        items = self._open_txt_file('scraper_items.txt')
        exp_out = {'CP4ED2CY': '10.1038/s41467-021-26215-w'}
        obs_out = _get_parent_and_doi(items)
        self.assertEqual(exp_out, obs_out)

    def test_get_parent_and_doi_no_doi(self):
        items = self._open_txt_file('scraper_items_no_doi.txt')
        with self.assertRaisesRegex(KeyError, 'no items with associated DOI'):
            _get_parent_and_doi(items)

    def test_get_attachment_keys(self):
        items = self._open_txt_file('scraper_items.txt')
        exp_keys = ['DMJ4AQ48', 'WZV4HG8X']
        returned_keys = _get_attachment_keys(items)
        self.assertEqual(sorted(exp_keys), sorted(returned_keys))

    def test_get_attachment_keys_raiseError(self):
        items = self._open_txt_file('scraper_items_no_attach.txt')
        with self.assertRaisesRegex(KeyError, 'No attachments exist'):
            _get_attachment_keys(items)

    def test_link_attach_and_doi(self):
        items = self._open_txt_file('scraper_items.txt')
        parent_doi = {'CP4ED2CY': '10.1038/s41467-021-26215-w'}
        exp_doi = '10.1038/s41467-021-26215-w'
        obs_doi = _link_attach_and_doi(items, 'DMJ4AQ48', parent_doi)
        self.assertEqual(obs_doi, exp_doi)

    def test_link_attach_and_doi_no_parent(self):
        items = self._open_txt_file('scraper_items.txt')
        parent_doi = {'other_parentID': '10.1038/s41467-021-26215-w'}
        with self.assertRaisesRegex(KeyError, 'DMJ4AQ48 does not contain'):
            _link_attach_and_doi(items, 'DMJ4AQ48', parent_doi)

    def test_find_special_id_one_match(self):
        txt = 'PRJDB1234: 2345 and 4567. How about another study?'
        pattern = r'PRJ[EDN][A-Z]\s?\d+:\s\d+'

        exp_ids = ['PRJDB2345']
        obs_ids = _find_special_id(txt, pattern, ':')
        self.assertListEqual(sorted(obs_ids), sorted(exp_ids))

    def test_find_special_id_two_matches(self):
        txt = 'PRJDB1234: 2345 and 4567. How about another study? ' \
              'PRJEA9876: 8765.'
        pattern = r'PRJ[EDN][A-Z]\s?\d+:\s\d+'

        exp_ids = ['PRJDB2345', 'PRJEA8765']
        obs_ids = _find_special_id(txt, pattern, ':')
        self.assertListEqual(sorted(obs_ids), sorted(exp_ids))

    def test_find_special_id_no_match(self):
        txt = 'How about another study?'
        pattern = r'PRJ[EDN][A-Z]\s?\d+:\s\d+'

        exp_ids = []
        obs_ids = _find_special_id(txt, pattern, ':')
        self.assertListEqual(sorted(obs_ids), sorted(exp_ids))

    def test_find_runIDs(self):
        txt_w_2ids = 'this data available in PRJEB4519 and ERR2765209'
        exp_id = ['ERR2765209']
        obs_id = _find_accession_ids(txt_w_2ids, 'run')
        self.assertListEqual(sorted(obs_id), sorted(exp_id))

    def test_find_bioproject_ids(self):
        txt_w_2ids = 'this data available in PRJEB4519 and ERR2765209'
        exp_id = ['PRJEB4519']
        obs_id = _find_accession_ids(txt_w_2ids, 'bioproject')
        self.assertListEqual(sorted(obs_id), sorted(exp_id))

    def test_find_accession_ids_no_double(self):
        txt_w_2ids = 'this data available in PRJEB4519 and PRJEB4519. Also '\
                     'in ERR2765209 and ERR2765209.'
        exp_proj = ['PRJEB4519']
        obs_proj = _find_accession_ids(txt_w_2ids, 'bioproject')
        self.assertListEqual(exp_proj, obs_proj)

        exp_run = ['ERR2765209']
        obs_run = _find_accession_ids(txt_w_2ids, 'run')
        self.assertListEqual(exp_run, obs_run)

    def test_find_accession_ids_special_cases_one_comma(self):
        # example inspired by this publication:
        # https://doi.org/10.1038/s41467-021-26215-w
        txt_diff = 'under accession numbers PRJEB11895, 12577 and '\
                   '41427'
        exp_proj = ['PRJEB11895', 'PRJEB12577', 'PRJEB41427']
        obs_proj = _find_accession_ids(txt_diff, 'bioproject')
        self.assertListEqual(sorted(exp_proj), sorted(obs_proj))

    def test_find_accession_ids_special_cases_three_commas(self):
        # example inspired by this publication:
        # https://doi.org/10.1038/s41467-021-26215-w
        txt_diff = 'under accession numbers PRJEB11895, 12577, 34555, '\
                   '89765 and 41427'
        exp_proj = ['PRJEB11895', 'PRJEB12577', 'PRJEB34555',
                    'PRJEB89765', 'PRJEB41427']
        obs_proj = _find_accession_ids(txt_diff, 'bioproject')
        self.assertListEqual(sorted(exp_proj), sorted(obs_proj))

    def test_find_accession_ids_no_ids(self):
        txt = 'this text has no run ids and no bioproject ids.'
        exp_ls = []
        obs_run = _find_accession_ids(txt, 'run')
        obs_proj = _find_accession_ids(txt, 'bioproject')
        self.assertListEqual(exp_ls, obs_run)
        self.assertListEqual(exp_ls, obs_proj)

    def test_merge2dict_new_items(self):
        ext_dict = {'accID1': ['doi1']}

        exp_out = {'accID1': ['doi1'],
                   'accID2': ['new_doi'],
                   'accID3': ['new_doi']}
        obs_out = _merge2dict(ext_dict, ['accID2', 'accID3'], 'new_doi')
        self.assertDictEqual(exp_out, obs_out)

    def test_merge2dict_existing_item_extend(self):
        ext_dict = {'accID1': ['doi1']}

        exp_out = {'accID1': ['doi1', 'new_doi'],
                   'accID3': ['new_doi']}
        obs_out = _merge2dict(ext_dict, ['accID1', 'accID3'], 'new_doi')
        self.assertDictEqual(exp_out, obs_out)

    def test_merge2dict_no_duplicate_dois(self):
        ext_dict = {'accID1': ['doi1']}

        exp_out = {'accID1': ['doi1'],
                   'accID3': ['doi1']}
        obs_out = _merge2dict(ext_dict, ['accID1', 'accID3'], 'doi1')
        self.assertDictEqual(exp_out, obs_out)

    def test_transform_dict_to_df(self):
        ext_dict = {'accID1': ['doi1'], 'accID2': ['doi2']}
        exp_out = self._create_doi_id_dataframe(
            ['accID1', 'accID2'], ['doi1', 'doi2'])

        obs_out = _transform_dict_to_df(ext_dict)
        assert_frame_equal(exp_out, obs_out)

    def test_transform_dict_to_df_empty_dict(self):
        ext_dict = {}
        exp_out = self._create_doi_id_dataframe(
            [], [])

        obs_out = _transform_dict_to_df(ext_dict)
        assert_frame_equal(exp_out, obs_out)


class TestCollectionScraping(TestUtils4CollectionScraping):
    package = 'q2_fondue.tests'

    @classmethod
    def setUpClass(cls) -> None:
        cls.fake_logger = logging.getLogger('test_log')

    @patch('q2_fondue.scraper._get_collection_id')
    @patch.object(zotero.Zotero, 'everything')
    @patch.object(zotero.Zotero, 'collection_items')
    @patch.object(zotero.Zotero, 'fulltext_item')
    def test_collection_scraper_bothIDs(
            self, patch_zot_txt,
            patch_col, patch_items, patch_get_col_id):
        # define patched outputs
        patch_items.return_value = self._open_txt_file(
            'scraper_items.txt')
        patch_zot_txt.return_value = {
            "content": "This is full-text with PRJEB4519 and ERR2765209.",
            "indexedPages": 50,
            "totalPages": 50
        }
        # check
        exp_out_run = self._create_doi_id_dataframe(
            ['ERR2765209'], ['10.1038/s41467-021-26215-w'])
        exp_out_proj = self._create_doi_id_dataframe(
            ['PRJEB4519'], ['10.1038/s41467-021-26215-w'])
        obs_out_run, obs_out_proj = scrape_collection(
            "user", "12345", "myuserkey", "test_collection")
        assert_frame_equal(exp_out_proj, obs_out_proj)
        assert_frame_equal(exp_out_run, obs_out_run)

    @patch('q2_fondue.scraper._get_collection_id')
    @patch.object(zotero.Zotero, 'everything')
    @patch.object(zotero.Zotero, 'collection_items')
    @patch.object(zotero.Zotero, 'fulltext_item')
    def test_collection_scraper_only_run_ids(
            self, patch_zot_txt,
            patch_col, patch_items, patch_get_col_id):
        # define patched outputs
        patch_items.return_value = self._open_txt_file(
            'scraper_items.txt')
        patch_zot_txt.return_value = {
            "content": "This is full-text with ERR2765209.",
            "indexedPages": 50,
            "totalPages": 50
        }
        # check
        exp_out_run = self._create_doi_id_dataframe(
            ['ERR2765209'], ['10.1038/s41467-021-26215-w'])
        obs_out_proj = self._create_doi_id_dataframe(
            [], [])
        with self.assertLogs('q2_fondue.scraper', level='WARNING') as cm:
            obs_out_run, obs_out_proj = scrape_collection(
                "user", "12345", "myuserkey", "test_collection")
            self.assertIn(
                "WARNING:q2_fondue.scraper:The provided collection "
                "test_collection does not contain any BioProject IDs",
                cm.output
            )
            assert_frame_equal(obs_out_run, exp_out_run)
            assert_frame_equal(obs_out_proj, obs_out_proj)

    @patch('q2_fondue.scraper._get_collection_id')
    @patch.object(zotero.Zotero, 'everything')
    @patch.object(zotero.Zotero, 'collection_items')
    @patch.object(zotero.Zotero, 'fulltext_item')
    def test_collection_scraper_onlyProjectIDs(
            self, patch_zot_txt,
            patch_col, patch_items, patch_get_col_id):
        # define patched outputs
        patch_items.return_value = self._open_txt_file(
            'scraper_items.txt')
        patch_zot_txt.return_value = {
            "content": "This is full-text with PRJEB4519.",
            "indexedPages": 50,
            "totalPages": 50
        }
        # check
        exp_out = self._create_doi_id_dataframe(
            ['PRJEB4519'], ['10.1038/s41467-021-26215-w'])

        with self.assertLogs('q2_fondue.scraper', level='WARNING') as cm:
            _, obs_out = scrape_collection("user", "12345",
                                           "myuserkey", "test_collection")
            self.assertIn(
                "WARNING:q2_fondue.scraper:The provided collection "
                "test_collection does not contain any run IDs",
                cm.output
            )
            assert_frame_equal(obs_out, exp_out)

    @patch('q2_fondue.scraper._get_collection_id')
    @patch.object(zotero.Zotero, 'everything')
    @patch.object(zotero.Zotero, 'collection_items')
    @patch.object(zotero.Zotero, 'fulltext_item')
    def test_collection_scraper_noIDs(
            self, patch_zot_txt,
            patch_col, patch_items, patch_get_col_id):
        # define patched outputs
        patch_items.return_value = self._open_txt_file(
            'scraper_items.txt')
        patch_zot_txt.return_value = {
            "content": "This is full-text without any IDs.",
            "indexedPages": 50,
            "totalPages": 50
        }
        col_name = "test_collection"
        with self.assertRaisesRegex(
            NoAccessionIDs, f'collection {col_name} does not'
        ):
            scrape_collection("user", "12345",
                              "myuserkey", col_name)

    @patch('q2_fondue.scraper._get_collection_id')
    @patch.object(zotero.Zotero, 'everything')
    @patch.object(zotero.Zotero, 'collection_items')
    @patch.object(zotero.Zotero, 'fulltext_item')
    def test_collection_scraper_nofulltext(
            self, patch_zot_txt,
            patch_col, patch_items, patch_get_col_id):
        # define patched outputs
        patch_items.return_value = self._open_txt_file(
            'scraper_items.txt')
        patch_zot_txt.side_effect = [zotero_errors.ResourceNotFound,
                                     {
                                         "content":
                                         "This is full-text with PRJEB4519.",
                                         "indexedPages": 50,
                                         "totalPages": 50
                                     }]
        exp_out = self._create_doi_id_dataframe(
            ['PRJEB4519'], ['10.1038/s41467-021-26215-w'])
        with self.assertLogs('q2_fondue.scraper', level='WARNING') as cm:
            _, obs_out = scrape_collection("user", "12345",
                                           "myuserkey", "test_collection")
            self.assertIn(
                "WARNING:q2_fondue.scraper:Item DMJ4AQ48 doesn't contain "
                "any full-text content",
                cm.output
            )
            assert_frame_equal(obs_out, exp_out)
