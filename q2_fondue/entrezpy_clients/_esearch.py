# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import json
from typing import List

import pandas as pd
from entrezpy.base.analyzer import EutilsAnalyzer
from entrezpy.base.result import EutilsResult
from q2_fondue.entrezpy_clients._utils import set_up_logger


class ESearchResult(EutilsResult):
    """Entrezpy client for ESearch utility used to search for or validate
        provided accession IDs.
    """
    def __init__(self, response, request, log_level):
        super().__init__(request.eutil, request.query_id, request.db)
        self.result_raw = None
        self.result = None
        self.query_key = None
        self.webenv = None
        self.logger = set_up_logger(log_level, self)

    def size(self):
        return self.result.shape[0]

    def isEmpty(self):
        return True if self.size() == 0 else False

    def dump(self):
        return {self: {'dump': {'result': self.result,
                                'query_id': self.query_id,
                                'db': self.db,
                                'eutil': self.function}}}

    def get_link_parameter(self, reqnum=0):
        """Generates params required for an ELink query"""
        return {
            'db': self.db, 'queryid': self.query_id, 'WebEnv': self.webenv,
            'query_key': self.query_key, 'cmd': 'neighbor_history'
        }

    def validate_result(self) -> dict:
        """Validates hit counts obtained for all the provided UIDs.

        As the expected hit count for a valid SRA accession ID is 1, all the
        IDs with that value will be considered valid. UIDs with count higher
        than 1 will be considered 'ambiguous' as they could not be resolved
        to a single result. Likewise, UIDs with a count of 0 will be considered
        'invalid' as no result could be found for those.

        Raises:
            InvalidIDs: An exception is raised when either ambiguous or invalid
                IDs were encountered.

        """
        # correct id should have count == 1
        leftover_ids = self.result[self.result != 1]
        if leftover_ids.shape[0] == 0:
            return {}
        ambigous_ids = leftover_ids[leftover_ids > 0]
        invalid_ids = leftover_ids[leftover_ids == 0]

        error_msg = 'Some of the IDs are invalid or ambiugous:'
        if ambigous_ids.shape[0] > 0:
            error_msg += f'\n Ambiguous IDs: {", ".join(ambigous_ids.index)}'
        if invalid_ids.shape[0] > 0:
            error_msg += f'\n Invalid IDs: {", ".join(invalid_ids.index)}'
        self.logger.warning(error_msg)
        return {
            **{_id: 'ID is ambiguous.' for _id in ambigous_ids.index},
            **{_id: 'ID is invalid.' for _id in invalid_ids.index}
        }

    def parse_search_results(self, response, uids: List[str]):
        """Parses response received from Esearch as a pandas Series object.

        Hit counts obtained in the response will be extracted and assigned to
        their respective query IDs. IDs not found in the results but present
        in the UIDs list will get a count of 0.

        Args:
            response (): Response received from Esearch.
            uids (List[str]): List of original UIDs that were submitted
                as a query.

        """
        self.result_raw = response
        self.webenv = self.result_raw['esearchresult'].get('webenv')
        self.query_key = self.result_raw['esearchresult'].get('querykey')

        translation_stack = self.result_raw[
            'esearchresult'].get('translationstack')
        if not translation_stack:
            self.result = pd.Series({x: 0 for x in uids}, name='count')
            return

        # filter out only positive hits
        found_terms = [x for x in translation_stack if isinstance(x, dict)]
        found_terms = {
            x['term'].replace('[All Fields]', ''): int(x['count'])
            for x in found_terms
        }

        # find ids that are missing
        missing_ids = [x for x in uids if x not in found_terms.keys()]
        missing_ids = {x: 0 for x in missing_ids}
        found_terms.update(missing_ids)

        self.result = pd.Series(found_terms, name='count')


class ESearchAnalyzer(EutilsAnalyzer):
    def __init__(self, uids, log_level):
        super().__init__()
        self.uids = uids
        self.log_level = log_level

    def init_result(self, response, request):
        if not self.result:
            self.result = ESearchResult(response, request, self.log_level)

    def analyze_error(self, response, request):
        print(json.dumps({
            __name__: {
                'Response': {
                    'dump': request.dump(),
                    'error': response.getvalue()}}}))

    def analyze_result(self, response, request):
        self.init_result(response, request)
        self.result.parse_search_results(response, self.uids)
