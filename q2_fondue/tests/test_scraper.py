# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
import json
import pandas as pd
from qiime2.plugin.testing import TestPluginBase
from pandas._testing import assert_series_equal
from unittest.mock import patch
from pyzotero import zotero, zotero_errors
from q2_fondue.scraper import (
    _get_collection_id, _find_accessionIDs,
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

    @patch.object(zotero.Zotero, 'everything')
    @patch.object(zotero.Zotero, 'collection_items')
    def test_get_attachment_keys(self, patch_col, patch_ever):
        patch_ever.return_value = self._open_json_file(
            'scraper_collection_items.json')
        exp_keys = ['XSE8Y2GR', 'PAWLP4NQ']
        returned_keys = _get_attachment_keys(self.zot, 'testID')
        self.assertEqual(sorted(exp_keys), sorted(returned_keys))

    @patch.object(zotero.Zotero, 'everything')
    @patch.object(zotero.Zotero, 'collection_items')
    def test_get_attachment_keys_raiseError(self, patch_col, patch_ever):
        patch_ever.return_value = []
        with self.assertRaisesRegex(
                KeyError, 'No attachments exist'):
            _get_attachment_keys(self.zot, 'testID')

    def test_find_accessionIDs(self):
        txt_w_2ids = 'this data available in PRJEB4519 and ERR2765209'
        exp_ls = ['PRJEB4519', 'ERR2765209']
        obs_ls = _find_accessionIDs(txt_w_2ids)
        self.assertListEqual(sorted(exp_ls), sorted(obs_ls))

    def test_find_accessionIDs_no_double(self):
        txt_w_2ids = 'this data available in PRJEB4519 and PRJEB4519'
        exp_ls = ['PRJEB4519']
        obs_ls = _find_accessionIDs(txt_w_2ids)
        self.assertListEqual(exp_ls, obs_ls)

    def test_find_accessionIDs_no_ids(self):
        txt = 'this text has no accession ids.'
        exp_ls = []
        obs_ls = _find_accessionIDs(txt)
        self.assertListEqual(exp_ls, obs_ls)


class TestCollectionScraping(TestPluginBase):
    package = 'q2_fondue.tests'

    @patch('q2_fondue.scraper._get_collection_id')
    @patch('q2_fondue.scraper._get_attachment_keys')
    @patch.object(zotero.Zotero, 'fulltext_item')
    def test_collection_scraper(
            self, patch_zot_txt,
            patch_get_attach, patch_get_col_id):
        # define patched outputs
        patch_get_attach.return_value = ['attach_key']
        patch_zot_txt.return_value = {
            "content": "This is full-text with PRJEB4519.",
            "indexedPages": 50,
            "totalPages": 50
        }
        # check
        exp_out = pd.Series({'id': ['PRJEB4519']})
        obs_out = scrape_collection("user", "12345",
                                    "myuserkey", "test_collection")
        assert_series_equal(exp_out, obs_out)

    @patch('q2_fondue.scraper._get_collection_id')
    @patch('q2_fondue.scraper._get_attachment_keys')
    @patch.object(zotero.Zotero, 'fulltext_item')
    def test_collection_scraper_noIDs(
            self, patch_zot_txt,
            patch_get_attach, patch_get_col_id):
        # define patched outputs
        patch_get_attach.return_value = ['attach_key']
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
    @patch('q2_fondue.scraper._get_attachment_keys')
    @patch.object(zotero.Zotero, 'fulltext_item')
    def test_collection_scraper_nofulltext(
            self, patch_zot_txt,
            patch_get_attach, patch_get_col_id):
        # define patched outputs
        patch_get_attach.return_value = ['attach_key1', 'attach_key2']
        patch_zot_txt.side_effect = [zotero_errors.ResourceNotFound,
                                     {
                                         "content":
                                         "This is full-text with PRJEB4519.",
                                         "indexedPages": 50,
                                         "totalPages": 50
                                     }]
        exp_out = pd.Series({'id': ['PRJEB4519']})
        with self.assertLogs('q2_fondue.scraper', level='WARNING') as cm:
            obs_out = scrape_collection("user", "12345",
                                        "myuserkey", "test_collection")
            self.assertIn(
                "WARNING:q2_fondue.scraper:Item attach_key1 doesn't contain "
                "any full-text content",
                cm.output
            )
            assert_series_equal(obs_out, exp_out)
