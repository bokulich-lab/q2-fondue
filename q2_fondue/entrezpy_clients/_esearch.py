# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

from typing import List, Union

import pandas as pd
from entrezpy.esearch.esearch_analyzer import EsearchAnalyzer
from entrezpy.esearch.esearch_result import EsearchResult


class ESearchResult(EsearchResult):
    """Entrezpy client for ESearch utility used to search for or validate
        provided accession IDs.
    """
    def __init__(self, response, request):
        super().__init__(response, request)
        self.result = None

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
        ambiguous_ids = leftover_ids[leftover_ids > 0]
        invalid_ids = leftover_ids[leftover_ids == 0]

        error_msg = 'Some of the IDs are invalid or ambiguous:'
        if ambiguous_ids.shape[0] > 0:
            error_msg += f'\n Ambiguous IDs: {", ".join(ambiguous_ids.index)}'
        if invalid_ids.shape[0] > 0:
            error_msg += f'\n Invalid IDs: {", ".join(invalid_ids.index)}'
        self.logger.warning(error_msg)
        return {
            **{_id: 'ID is ambiguous.' for _id in ambiguous_ids.index},
            **{_id: 'ID is invalid.' for _id in invalid_ids.index}
        }

    def parse_search_results(self, response, uids: Union[List[str], None]):
        """Parses response received from Esearch as a pandas Series object.

        Hit counts obtained in the response will be extracted and assigned to
        their respective query IDs. IDs not found in the results but present
        in the UIDs list will get a count of 0.

        Args:
            response (): Response received from Esearch.
            uids (List[str]): List of original UIDs that were submitted
                as a query.

        """
        translation_stack = response['esearchresult'].get('translationstack')
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
        if uids:
            missing_ids = [x for x in uids if x not in found_terms.keys()]
            missing_ids = {x: 0 for x in missing_ids}
            found_terms.update(missing_ids)

        self.result = pd.Series(found_terms, name='count')


class ESearchAnalyzer(EsearchAnalyzer):
    def __init__(self, uids):
        super().__init__()
        self.uids = uids

    # override the base method to use our own ESResult
    def init_result(self, response, request):
        if not self.result:
            self.result = ESearchResult(response, request)
            return True
        return False

    # override the base method to additionally parse the result
    def analyze_result(self, response, request):
        super().analyze_result(response, request)
        self.result.parse_search_results(response, self.uids)
