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
class SRABaseMeta:
    id: str
    child: str

    def get_base_metadata(self, excluded):
        index = get_attrs(self, excluded=('child',) + excluded)
        base_meta = pd.DataFrame(
            data=[getattr(self, k) for k in index], index=index).T
        base_meta.index = [self.id]
        return base_meta

    def get_child_metadata(self):
        child_meta = pd.concat(
            [x.generate_meta() for x in
             self.__getattribute__(f'{self.child}s')]
        )
        child_meta.index.name = f'{self.child}_id'
        return child_meta


@dataclass
class SRARun(SRABaseMeta):
    public: bool = True
    size: int = None
    bases: int = None
    spots: int = None
    avg_spot_len: int = None
    experiment_id: str = None
    child: str = None

    def __post_init__(self):
        self.avg_spot_len = int(int(self.bases)/int(self.spots))

    def generate_meta(self):
        return self.get_base_metadata(excluded=('id',))


@dataclass
class SRAExperiment(SRABaseMeta):
    instrument: str = None
    platform: str = None
    library: LibraryMetadata = None
    runs: List[SRARun] = field(default_factory=list)
    sample_id: str = None
    child: str = 'run'

    def generate_meta(self):
        exp_meta = self.get_base_metadata(excluded=('id', 'runs', 'library'))
        runs_meta = self.get_child_metadata()
        return runs_meta.merge(
            exp_meta, left_on='experiment_id', right_index=True)


@dataclass
class SRASample(SRABaseMeta):
    name: str = None
    title: str = None
    biosample_id: str = None
    organism: str = None
    tax_id: str = None
    study_id: str = None
    experiments: List[SRAExperiment] = field(default_factory=list)
    child: str = 'experiment'

    def generate_meta(self):
        sample_meta = self.get_base_metadata(excluded=('id', 'experiments'))
        exps_meta = self.get_child_metadata()
        return exps_meta.merge(
            sample_meta, left_on='sample_id', right_index=True)


@dataclass
class SRAStudy(SRABaseMeta):
    bioproject_id: str = None
    center_name: str = None
    samples: List[SRASample] = field(default_factory=list)
    child: str = 'sample'

    def generate_meta(self):
        study_meta = self.get_base_metadata(excluded=('id', 'samples'))
        samples_meta = self.get_child_metadata()
        return samples_meta.merge(
            study_meta, left_on='study_id', right_index=True)
