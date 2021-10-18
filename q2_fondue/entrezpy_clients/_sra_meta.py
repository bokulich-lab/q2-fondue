# ----------------------------------------------------------------------------
# Copyright (c) 2021, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

from abc import abstractmethod, ABCMeta
from dataclasses import dataclass, field
from typing import Union, List

import pandas as pd

from q2_fondue.entrezpy_clients._utils import get_attrs


@dataclass
class LibraryMetadata:
    """A class for storing sequencing library metadata."""
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
class SRABaseMeta(metaclass=ABCMeta):
    """A base class for generation of SRA metadata objects.

    Attributes:
        id (str): Unique ID of the metadata object.
        custom_meta (Union[dict, None]): Custom metadata belonging
            to the object, if any.
        child (str): a one-word description of the child type for
            the given object (e.g., a 'sample' is a child of a 'study').
    """
    id: str
    custom_meta: Union[dict, None]
    child: str = None

    def __post_init__(self):
        """Initializes custom metadata DataFrame."""
        if self.custom_meta:
            self.custom_meta_df = pd.DataFrame(
                self.custom_meta, index=[self.id])
        else:
            self.custom_meta_df = None

    def get_base_metadata(self, excluded: tuple) -> pd.DataFrame:
        """Generates a DataFrame containing basic metadata of the SRA object.

        The metadata generated by this method do not contain any of the
        metadata belonging the any of the object's children.

        Args:
            excluded (tuple): attributes to be excluded during metadata
                DataFrame generation
        Returns:
            base_meta (pd.DataFrame): Requested base metadata.
        """
        index = get_attrs(
            self,
            excluded=('child', 'custom_meta', 'custom_meta_df') + excluded)
        base_meta = pd.DataFrame(
            data={k: getattr(self, k) for k in index}, index=[self.id])

        if self.custom_meta:
            base_meta = pd.concat(
                [base_meta, self.custom_meta_df], axis=1,
            )

        return base_meta

    def get_child_metadata(self) -> pd.DataFrame:
        """Generates a DataFrame containing metadata of all the
            children SRA objects.

        Returns:
             child_meta (pd.DataFrame): Requested children objects' metadata.
        """
        child_meta_dfs = [x.generate_meta() for x in
                          self.__getattribute__(f'{self.child}s')]
        if child_meta_dfs:
            child_meta = pd.concat(child_meta_dfs)
        else:
            child_meta = pd.DataFrame()
        child_meta.index.name = f'{self.child}_id'
        return child_meta

    @abstractmethod
    def generate_meta(self) -> pd.DataFrame:
        """Generates a DataFrame with all metadata.

        Metadata from current object will be collected and merged together
        with metadata gathered from all of its children.

        Returns:
            pd.DataFrame: DataFrame containing all metadata.
        """
        pass


@dataclass
class SRARun(SRABaseMeta):
    """A class containing all the SRA run metadata.

    Attributes:
        public (bool): True if the dataset was public.
        bytes (int): Size of the run dataset.
        bases (int): Nucleotide count of the run dataset.
        spots (int): Spot count of the run dataset.
        avg_spot_len (int): Average spot length.
        experiment_id (str): ID of the experiment which the run belongs to.
        child (str): Run's child type (None, as runs have no children objects).
    """
    public: bool = True
    bytes: int = None
    bases: int = None
    spots: int = None
    avg_spot_len: int = None
    experiment_id: str = None
    child: str = None

    def __post_init__(self):
        """Calculates an average spot length."""
        super().__post_init__()
        self.avg_spot_len = int(int(self.bases)/int(self.spots))

    def generate_meta(self) -> pd.DataFrame:
        """Generates run's metadata.

        Returns:
            pd.DataFrame: Run's metadata.
        """
        return self.get_base_metadata(excluded=('id',))


@dataclass
class SRAExperiment(SRABaseMeta):
    """A class containing all the SRA experiment metadata.

    Attributes:
        instrument (str): Sequencing instrument name.
        platform (str): Sequencing platform name.
        library (LibraryMetadata): Metadata of the sequencing library.
        runs (List[SRARun]): All SRA runs belonging to this experiment.
        child (str): Runs are children of experiment objects.

    """
    instrument: str = None
    platform: str = None
    library: LibraryMetadata = None
    runs: List[SRARun] = field(default_factory=list)
    sample_id: str = None
    child: str = 'run'

    def generate_meta(self) -> pd.DataFrame:
        """Generates experiment's metadata.

        Generated metadata will include all metadata of the linked runs.

        Returns:
            pd.DataFrame: Experiment's metadata with all of its children.
        """
        exp_meta = self.get_base_metadata(excluded=('id', 'runs', 'library'))
        lib_meta = self.library.generate_meta()
        lib_meta.index = exp_meta.index

        exp_meta = pd.concat([exp_meta, lib_meta], axis=1)
        runs_meta = self.get_child_metadata()
        if len(runs_meta) > 0:
            runs_merged = runs_meta.merge(
                exp_meta, left_on='experiment_id', right_index=True)
            runs_merged.index.name = 'run_id'
            return runs_merged
        else:
            return exp_meta


@dataclass
class SRASample(SRABaseMeta):
    """A class containing all the SRA sample metadata.

    Attributes:
        name (str): Name of the sample.
        title (str): Title of the sample.
        biosample_id (str): BioSample ID linked to the sample.
        organism (str): Organism name.
        tax_id (str): Organism taxonomic ID.
        study_id (str): = ID of the study which the sample belongs to.
        experiments (List[SRAExperiment]): All SRA experiments
            belonging to the sample.
        child (str): = Experiments are children of sample objects.
    """
    name: str = None
    title: str = None
    biosample_id: str = None
    organism: str = None
    tax_id: str = None
    study_id: str = None
    experiments: List[SRAExperiment] = field(default_factory=list)
    child: str = 'experiment'

    def generate_meta(self) -> pd.DataFrame:
        """Generates SRA sample's metadata.

        Generated metadata will include all metadata of the linked experiments.

        Returns:
            pd.DataFrame: Sample's metadata with all of its children.
        """
        sample_meta = self.get_base_metadata(excluded=('id', 'experiments'))
        exps_meta = self.get_child_metadata()
        if len(exps_meta) > 0:
            exps_merged = exps_meta.merge(
                sample_meta, left_on='sample_id', right_index=True)
            exps_merged.index.name = 'run_id'
            return exps_merged
        else:
            return sample_meta


@dataclass
class SRAStudy(SRABaseMeta):
    """Generates SRA study's metadata.

    Generated metadata will include all metadata of the linked samples.

    Attributes:
        bioproject_id (str): ID of the linked BioProject.
        center_name (str): Name of the center where the study was performed.
        samples (List[SRASample]): All SRA samples belonging to the study.
        child (str): Samples are children of study objects.
    """
    bioproject_id: str = None
    center_name: str = None
    samples: List[SRASample] = field(default_factory=list)
    child: str = 'sample'

    def generate_meta(self) -> pd.DataFrame:
        """Generates SRA study's metadata.

        Generated metadata will include all metadata of the linked samples.

        Returns:
            pd.DataFrame: Study's metadata with all of its children.
        """
        study_meta = self.get_base_metadata(excluded=('id', 'samples'))
        samples_meta = self.get_child_metadata()
        if len(samples_meta) > 0:
            samples_merged = samples_meta.merge(
                study_meta, left_on='study_id', right_index=True)
            samples_merged.index.name = 'run_id'
            return samples_merged
        else:
            return study_meta
