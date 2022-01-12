# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import json

from entrezpy.base.analyzer import EutilsAnalyzer
from entrezpy.base.result import EutilsResult


class ELinkResult(EutilsResult):
    """Entrezpy client for ELink utility used to look up IDs related to
        another ID set received in an earlier, linked request.
    """
    def __init__(self, response, request):
        super().__init__(request.eutil, request.query_id, request.db)
        self.result_raw = None
        self.result = None
        self.query_key = None
        self.webenv = None

    def size(self):
        return len(self.result)

    def isEmpty(self):
        # TODO: this needs to be adjusted
        # TODO: why is this never called ?!
        return True if self.size() == 0 else False

    def dump(self):
        return {self: {'dump': {'result': self.result,
                                'query_id': self.query_id,
                                'db': self.db,
                                'eutil': self.function}}}

    def get_link_parameter(self, reqnum=0):
        """Generates params required for an ELink query"""
        return {
            'db': self.db, 'WebEnv': self.webenv,
            'query_key': self.query_key, 'cmd': 'neighbor_history',
            # TODO: that's cheating due to a bug in the EFetchParameter init:
            'retmax': 10000
        }

    def parse_link_results(self, response):
        """Parses response received from Elink.

        It will extract details of the history server for the
        follow-up request.

        Args:
            response: Response received from Elink.
        """
        self.result_raw = response
        self.result = response['linksets'][0]
        # TODO: what happens when linksets or linksetdbhistories > 1?
        self.webenv = self.result.get('webenv')
        self.query_key = self.result['linksetdbhistories'][0].get('querykey')


class ELinkAnalyzer(EutilsAnalyzer):
    def __init__(self):
        super().__init__()

    def init_result(self, response, request):
        if not self.result:
            self.result = ELinkResult(response, request)

    def analyze_error(self, response, request):
        print(json.dumps({
            __name__: {
                'Response': {
                    'dump': request.dump(),
                    'error': response.getvalue()}}}))

    def analyze_result(self, response, request):
        self.init_result(response, request)
        self.result.parse_link_results(response)
