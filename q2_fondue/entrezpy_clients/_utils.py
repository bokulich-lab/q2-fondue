# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

from dataclasses import dataclass, field
from typing import List, Union

import pandas as pd


PREFIX = {
    'run': ('SRR', 'ERR', 'DRR'),
    'experiment': ('SRX', 'ERX', 'DRX'),
    'sample': ('SRS', 'ERS', 'DRS'),
    'study': ('SRP', 'ERP', 'DRP'),
    'bioproject': ('PRJN', 'PRJE', 'PRJD')
}


def get_attrs(obj, excluded=()):
    print([k for k, v in vars(obj).items()])
    return [k for k, v in vars(obj).items()
            if k not in excluded and not k.startswith('__')]


def rename_columns(df: pd.DataFrame):
    # clean up ID columns
    col_map = {}
    id_cols = [col for col in df.columns if col.endswith('_id')]
    for col in id_cols:
        col_split = col.split('_')
        col_map[col] = f'{col_split[0].capitalize()} {col_split[1].upper()}'

    # clean up other multi-word columns
    wordy_cols = [col for col in df.columns
                  if '_' in col and col not in id_cols]
    for col in wordy_cols:
        col_map[col] = ' '.join([x.capitalize() for x in col.split('_')])

    # capitalize the rest
    remainder_cols = [col for col in df.columns
                      if col not in id_cols and col not in wordy_cols]
    for col in remainder_cols:
        col_map[col] = col.capitalize()

    return df.rename(columns=col_map, inplace=False)


@dataclass
class LibraryMetadata:
    name: str
    layout: str
    selection: str
    source: str

    def generate_meta(self):
        index = get_attrs(self)
        return pd.DataFrame(
            data=[getattr(self, k) for k in index],
            index=[f"library_{x}" for x in index]).T


@dataclass
class SRABaseMeta:
    id: str
    custom_meta: Union[dict, None]
    child: str = None

    def __post_init__(self):
        if self.custom_meta:
            self.custom_meta_df = pd.DataFrame(
                self.custom_meta, index=[self.id])
        else:
            self.custom_meta_df = None

    def get_base_metadata(self, excluded):
        index = get_attrs(
            self,
            excluded=('child', 'custom_meta', 'custom_meta_df') + excluded)
        base_meta = pd.DataFrame(
            data=[getattr(self, k) for k in index], index=index).T
        base_meta.index = [self.id]

        if self.custom_meta:
            base_meta = pd.concat(
                [base_meta, self.custom_meta_df], axis=1,
            )

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
    bytes: int = None
    bases: int = None
    spots: int = None
    avg_spot_len: int = None
    experiment_id: str = None
    child: str = None

    def __post_init__(self):
        super().__post_init__()
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
        lib_meta = self.library.generate_meta()
        lib_meta.index = exp_meta.index

        exp_meta = pd.concat([exp_meta, lib_meta], axis=1)
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
