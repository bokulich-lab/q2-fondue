# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import json

import pandas as pd
from entrezpy.base.analyzer import EutilsAnalyzer
from entrezpy.base.result import EutilsResult

from q2_fondue.entrezpy_clients._efetch import InvalidIDs


class ESearchResult(EutilsResult):
    def __init__(self, response, request):
        super().__init__(request.eutil, request.query_id, request.db)
        self.result_raw = None
        self.result = None

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
        return {}

    def validate_result(self):
        # correct id should have count == 1
        leftover_ids = self.result[self.result != 1]
        if leftover_ids.shape[0] == 0:
            return True
        ambigous_ids = leftover_ids[leftover_ids > 0]
        invalid_ids = leftover_ids[leftover_ids == 0]

        error_msg = 'Some of the IDs are invalid or ambiugous:'
        if ambigous_ids.shape[0] > 0:
            error_msg += f'\n Ambiguous IDs: {", ".join(ambigous_ids.index)}'
        if invalid_ids.shape[0] > 0:
            error_msg += f'\n Invalid IDs: {", ".join(invalid_ids.index)}'
        error_msg += '\nPlease check your accession IDs and try again.'
        raise InvalidIDs(error_msg)

    def parse_search_results(self, response, uids):
        self.result_raw = response

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
    def __init__(self, uids):
        super().__init__()
        self.uids = uids

    def init_result(self, response, request):
        if not self.result:
            self.result = ESearchResult(response, request)

    def analyze_error(self, response, request):
        print(json.dumps({
            __name__: {
                'Response': {
                    'dump': request.dump(),
                    'error': response.getvalue()}}}))

    def analyze_result(self, response, request):
        self.init_result(response, request)
        self.result.parse_search_results(response, self.uids)
