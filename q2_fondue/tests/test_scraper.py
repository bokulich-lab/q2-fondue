# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
# import logging
import json
from qiime2.plugin.testing import TestPluginBase
from unittest.mock import patch
from pyzotero import zotero
from q2_fondue.scraper import (
    _get_collection_id  # , scrape_collection
)


# class ScraperTests(TestPluginBase):
#     package = 'q2_fondue.tests'

#     @classmethod
#     def setUpClass(cls) -> None:
#         # todo add proper logger in test and method
#         # fake_logger = logging.getLogger('test_log')


class TestUtilsCollectionScraping(TestPluginBase):
    package = 'q2_fondue.tests'

    def setUp(self):
        super().setUp()

    def _open_json_file(self, filename):
        path2col = self.get_data_path(filename)
        file = open(path2col)
        return json.load(file)

    @patch.object(zotero.Zotero, 'everything')
    @patch.object(zotero.Zotero, 'collections')
    def test_get_correct_collectionID(self, patch_col, patch_ever):
        patch_ever.return_value = self._open_json_file(
            'scraper_collections.json')

        zot = zotero.Zotero("myuserID", "user", "myuserkey")
        exp_id = '9MK5KS97'
        returned_id = _get_collection_id(zot, 'Humanities')
        self.assertEqual(exp_id, returned_id)

    @patch.object(zotero.Zotero, 'everything')
    @patch.object(zotero.Zotero, 'collections')
    def test_get_collectionID_raiseError(self, patch_col, patch_ever):
        patch_ever.return_value = self._open_json_file(
            'scraper_collections.json')

        zot = zotero.Zotero("myuserID", "user", "myuserkey")
        col_name = 'BlaBla'
        with self.assertRaisesRegex(
            KeyError, f'name {col_name} does not exist'
        ):
            _get_collection_id(zot, col_name)

    # class TestCollectionScraping(ScraperTests):
    #     def test_collection_scraper(self):
    #         collection_name = '12_infant_dvmpt'
    #         ser_ids = scrape_collection(self.library_type, self.library_id,
    #                                     self.api_key, collection_name)
