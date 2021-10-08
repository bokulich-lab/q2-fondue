# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

from dataclasses import dataclass, field
from typing import List

import pandas as pd


# SRA EXPERIMENT is a unique sequencing result for a specific sample.
def get_attrs(obj, excluded=()):
    print([k for k, v in vars(obj).items()])
    return [k for k, v in vars(obj).items()
            if k not in excluded and not k.startswith('__')]


PREFIX = {
    'run': ('SRR', 'ERR', 'DRR'),
    'experiment': ('SRX', 'ERX', 'DRX'),
    'sample': ('SRS', 'ERS', 'DRS'),
    'study': ('SRP', 'ERP', 'DRP'),
    'bioproject': ('PRJN', 'PRJE', 'PRJD')
}


@dataclass
class LibraryMetadata:
    name: str
    layout: str
    selection: str
    source: str

    def generate_meta(self):
        index = get_attrs(self)
        return pd.Series(data=[getattr(self, k) for k in index],
                         index=[str(x) for x in index])


@dataclass
class SRARun:
    id: str
    public: bool  # should this be here?
    size: int
    bases: int
    spots: int
    avg_spot_len: int = None
    experiment_id: str = None

    def __post_init__(self):
        self.avg_spot_len = int(int(self.bases)/int(self.spots))

    def generate_meta(self):
        index = get_attrs(self)
        return pd.Series(data=[getattr(self, k) for k in index],
                         index=[str(x) for x in index])


@dataclass
class SRAExperiment:
    id: str
    instrument: str
    platform: str
    library: LibraryMetadata = None
    runs: List[SRARun] = field(default_factory=list)
    sample_id: str = None

    def generate_meta(self):
        index = get_attrs(self, excluded=('runs', 'library'))
        return pd.Series(data=[getattr(self, k) for k in index],
                         index=[str(x) for x in index])


@dataclass
class SRASample:
    id: str
    name: str
    title: str
    biosample_id: str
    organism: str
    tax_id: str
    study_id: str = None
    experiments: List[SRAExperiment] = field(default_factory=list)

    def generate_meta(self):
        index = get_attrs(self, excluded=('experiments',))
        return pd.Series(data=[getattr(self, k) for k in index],
                         index=[str(x) for x in index])


@dataclass
class SRAStudy:
    id: str
    bioproject_id: str
    center_name: str
    samples: List[SRASample] = field(default_factory=list)

    def generate_meta(self):
        index = get_attrs(self, excluded=('samples',))
        return pd.Series(data=[getattr(self, k) for k in index],
                         index=[str(x) for x in index])
