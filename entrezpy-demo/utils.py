import json

import pandas as pd
import pprint
from entrezpy.base.analyzer import EutilsAnalyzer
from entrezpy.base.result import EutilsResult
from xmltodict import parse as parsexml


class DuplicateKeyError:
    pass


class EFetchResult(EutilsResult):
    def __init__(self, response, request):
        super().__init__(request.eutil, request.query_id, request.db)
        self.metadata_raw = None
        self.metadata = {}

    def size(self):
        return len(self.metadata)

    def isEmpty(self):
        return True if self.size() == 0 else False

    def dump(self):
        return {self: {'dump': {'metadata': self.metadata,
                                'query_id': self.query_id,
                                'db': self.db,
                                'eutil': self.function}}}

    def get_link_parameter(self, reqnum=0):
        return {}

    def to_df(self):
        return pd.DataFrame.from_dict(self.metadata, orient='index')

    @staticmethod
    def _process_single_run(attributes_dict):
        processed_meta = {}

        keys_to_keep = {'SAMPLE', 'STUDY'}
        allowed_duplicates = {'ENA-FIRST-PUBLIC', 'ENA-LAST-UPDATE'}

        for k1, v1 in attributes_dict.items():
            if k1 in keys_to_keep and f'{k1}_ATTRIBUTES' in v1.keys():
                try:
                    for attr in v1[f'{k1}_ATTRIBUTES'][f'{k1}_ATTRIBUTE']:
                        if attr['TAG'] in processed_meta.keys() and attr['TAG'] not in allowed_duplicates:
                            raise DuplicateKeyError
                        processed_meta[attr['TAG']] = attr.get('VALUE')
                except Exception as e:
                    print(f'Exception has occurred: {e}. Contents of the metadata was:')
                    pprinter = pprint.PrettyPrinter(indent=4, compact=True)
                    pprinter.pprint(attributes_dict)

        return processed_meta

    def add_metadata(self, metadata, uids):
        # use json to quickly get rid of OrderedDicts
        self.metadata_raw = json.loads(json.dumps(parsexml(metadata.read())))
        parsed_results = self.metadata_raw['EXPERIMENT_PACKAGE_SET']['EXPERIMENT_PACKAGE']

        if isinstance(parsed_results, list):
            for i, uid in enumerate(uids):
                self.metadata[uid] = self._process_single_run(parsed_results[i])
        else:
            self.metadata[uids[0]] = self._process_single_run(parsed_results)


class EFetchAnalyzer(EutilsAnalyzer):
    def __init__(self):
        super().__init__()

    def init_result(self, response, request):
        if not self.result:
            self.result = EFetchResult(response, request)

    def analyze_error(self, response, request):
        print(json.dumps({__name__: {'Response': {'dump': request.dump(),
                                                  'error': response.getvalue()}}}))

    def analyze_result(self, response, request):
        self.init_result(response, request)
        self.result.add_metadata(response, request.uids)

    # cheating a bit here
    def parse(self, raw_response, request):
        response = self.convert_response(raw_response.read().decode('utf-8'), request)
        self.analyze_result(response, request)
