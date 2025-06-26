# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import unittest

import numpy as np
import pandas as pd
from pandas._testing import assert_frame_equal
from qiime2.plugin.testing import TestPluginBase

from q2_fondue.entrezpy_clients._sra_meta import (
    LibraryMetadata,
    SRARun,
    SRAExperiment,
    SRASample,
    SRAStudy,
)


class TestSraMetadata(TestPluginBase):
    package = "q2_fondue.tests"

    def setUp(self):
        super().setUp()
        self.library_meta = {
            "library_name": "fake_lib",
            "library_layout": "check",
            "library_selection": "random",
            "library_source": "lake",
        }
        self.run_meta = {
            "public": True,
            "bytes": 123,
            "bases": 1236,
            "spots": 12,
            "avg_spot_len": 103,
        }
        self.experiment_meta = {"instrument": "violin", "platform": "illumina"}
        self.sample_meta = {"organism": "Homo sapiens", "tax_id": "tax123"}
        self.study_meta = {"bioproject_id": "biop123", "center_name": "somewhere"}
        self.library = LibraryMetadata(
            **{k.split("_")[1]: v for k, v in self.library_meta.items()}
        )

    @staticmethod
    def assertFrameEqual(exp_df, obs_df):
        assert_frame_equal(exp_df.sort_index(axis=1), obs_df.sort_index(axis=1))

    @staticmethod
    def _generate_custom_meta(custom_type):
        mapping = {"run": 1, "exp": 3, "smp": 5, "std": 7}
        i = mapping[custom_type]
        return {f"custom {i + 1}": "val1", f"custom {i + 2}": "val2"}

    def _generate_sample_meta(self, ids):
        return [
            (
                _id,
                {
                    "name": f"sample_{i + 1}",
                    "title": f"Awesome Sample {i + 1}",
                    "biosample_id": f"bios{123 + i}",
                    **self.sample_meta,
                },
            )
            for i, _id in enumerate(ids)
        ]

    def _generate_runs(self, ids, exp_id):
        return [
            SRARun(
                id=_id,
                custom_meta=self._generate_custom_meta("run"),
                experiment_id=exp_id,
                **self.run_meta,
            )
            for _id in ids
        ]

    def _generate_exps(self, ids, smp_id, run_count=0):
        if run_count > 0:
            run_ids = [f"run{i}" for i in range(1, (run_count) * len(ids) + 1)]
            runs = [
                self._generate_runs(
                    run_ids[run_count * i : run_count * i + run_count], _id
                )
                for i, _id in enumerate(ids)
            ]
            return [
                SRAExperiment(
                    id=_id,
                    custom_meta=self._generate_custom_meta("exp"),
                    library=self.library,
                    sample_id=smp_id,
                    runs=runs[i],
                    **self.experiment_meta,
                )
                for i, _id in enumerate(ids)
            ]
        else:
            return [
                SRAExperiment(
                    id=_id,
                    custom_meta=self._generate_custom_meta("exp"),
                    library=self.library,
                    sample_id=smp_id,
                    **self.experiment_meta,
                )
                for i, _id in enumerate(ids)
            ]

    def _generate_smps(self, ids, std_id, exp_count=0):
        # will create 2 runs per experiment
        if exp_count > 0:
            exp_ids = [f"exp{i}" for i in range(1, (exp_count) * len(ids) + 1)]
            exps = [
                self._generate_exps(
                    exp_ids[exp_count * i : exp_count * i + exp_count], _id, run_count=2
                )
                for i, _id in enumerate(ids)
            ]
            return [
                SRASample(
                    id=_id,
                    **smp_meta,
                    study_id=std_id,
                    experiments=exps[i],
                    custom_meta=self._generate_custom_meta("smp"),
                )
                for i, (_id, smp_meta) in enumerate(self._generate_sample_meta(ids))
            ]
        else:
            return [
                SRASample(
                    id=_id,
                    **smp_meta,
                    study_id=std_id,
                    custom_meta=self._generate_custom_meta("smp"),
                )
                for _id, smp_meta in self._generate_sample_meta(ids)
            ]

    def test_library_meta_df(self):
        obs_df = self.library.generate_meta()
        exp_df = pd.DataFrame(self.library_meta, index=[0])
        self.assertFrameEqual(exp_df, obs_df)

    def test_sra_run_df(self):
        run = self._generate_runs(["run123"], "exp123")[0]

        self.assertEqual(103, run.avg_spot_len)

        obs_df = run.generate_meta()
        exp_df = pd.DataFrame(
            {
                "experiment_id": "exp123",
                **self.run_meta,
                **self._generate_custom_meta("run"),
            },
            index=pd.Index(["run123"]),
        )
        self.assertFrameEqual(exp_df, obs_df)

    def test_sra_experiment_df_no_run(self):
        exp = self._generate_exps(["exp123"], "smp123")[0]

        obs_df = exp.generate_meta()
        exp_df = pd.DataFrame(
            {
                "sample_id": "smp123",
                **self.experiment_meta,
                **self._generate_custom_meta("exp"),
                **self.library_meta,
            },
            index=pd.Index(["exp123"]),
        )
        self.assertFrameEqual(exp_df, obs_df)

    def test_sra_experiment_with_runs(self):
        exp = self._generate_exps(["exp123"], "smp123", run_count=2)[0]

        obs_df = exp.generate_meta()
        exp_run_ids = ["run1", "run2"]
        exp_df = pd.DataFrame(
            [
                {
                    "sample_id": "smp123",
                    "experiment_id": "exp123",
                    **self.experiment_meta,
                    **self._generate_custom_meta("exp"),
                    **self._generate_custom_meta("run"),
                    **self.run_meta,
                    **self.library_meta,
                }
            ]
            * len(exp_run_ids),
            index=pd.Index(exp_run_ids, name="run_id"),
        )
        self.assertFrameEqual(exp_df, obs_df)

    def test_sra_sample_no_exp(self):
        smp = self._generate_smps(["smp123"], "std123")[0]

        obs_df = smp.generate_meta()
        exp_df = pd.DataFrame(
            {
                **self._generate_sample_meta(["smp123"])[0][1],
                **self._generate_custom_meta("smp"),
                "study_id": "std123",
            },
            index=pd.Index(["smp123"]),
        )
        self.assertFrameEqual(exp_df, obs_df)

    def test_sra_sample_with_exps(self):
        smp = self._generate_smps(["smp123"], "std123", exp_count=2)[0]

        obs_df = smp.generate_meta()
        exp_run_ids = ["run1", "run2", "run3", "run4"]
        exp_df = pd.DataFrame(
            [
                {
                    "sample_id": "smp123",
                    "experiment_id": exp_id,
                    "study_id": "std123",
                    **self.experiment_meta,
                    **self.run_meta,
                    **self.library_meta,
                    **self.sample_meta,
                    **self._generate_sample_meta(["smp123"])[0][1],
                    **self._generate_custom_meta("smp"),
                    **self._generate_custom_meta("exp"),
                    **self._generate_custom_meta("run"),
                }
                for exp_id in ["exp1", "exp1", "exp2", "exp2"]
            ],
            index=pd.Index(exp_run_ids, name="run_id"),
        )
        self.assertFrameEqual(exp_df, obs_df)

    def test_sra_study_no_smp(self):
        std = SRAStudy(
            id="std123",
            custom_meta=self._generate_custom_meta("std"),
            **self.study_meta,
        )

        obs_df = std.generate_meta()
        exp_df = pd.DataFrame(
            {**self.study_meta, **self._generate_custom_meta("std")},
            index=pd.Index(["std123"]),
        )
        self.assertFrameEqual(exp_df, obs_df)

    def test_sra_study_with_smps(self):
        sample_ids = ["smp123", "smp234"]
        smps = self._generate_smps(sample_ids, "std123", exp_count=2)
        std = SRAStudy(
            id="std123",
            custom_meta=self._generate_custom_meta("std"),
            samples=smps,
            **self.study_meta,
        )

        obs_df = std.generate_meta()
        exp_run_ids = ["run1", "run2", "run3", "run4"] * 2
        exp_exp_smp_ids = list(
            zip(
                np.repeat([f"exp{i}" for i in range(1, 5)], 2),
                np.repeat(
                    [_id for _id, _ in self._generate_sample_meta(sample_ids)], 4
                ),
                np.repeat(
                    [smeta for _, smeta in self._generate_sample_meta(sample_ids)], 4
                ),
            )
        )
        exp_df = pd.DataFrame(
            [
                {
                    "sample_id": samp_id,
                    "experiment_id": exp_id,
                    "study_id": "std123",
                    **self.experiment_meta,
                    **samp_meta,
                    **self.run_meta,
                    **self.library_meta,
                    **self.sample_meta,
                    **self.study_meta,
                    **self._generate_custom_meta("std"),
                    **self._generate_custom_meta("smp"),
                    **self._generate_custom_meta("exp"),
                    **self._generate_custom_meta("run"),
                }
                for exp_id, samp_id, samp_meta in exp_exp_smp_ids
            ],
            index=pd.Index(exp_run_ids, name="run_id"),
        )
        self.assertFrameEqual(exp_df, obs_df)


if __name__ == "__main__":
    unittest.main()
