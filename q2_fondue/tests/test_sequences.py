# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------
import filecmp
import glob
import gzip
import itertools
import logging
import os
import shutil
import tempfile
from unittest.mock import patch, call, MagicMock

import pandas as pd
from parameterized import parameterized
from q2_types.per_sample_sequences import (
    FastqGzFormat,
    CasavaOneEightSingleLanePerSampleDirFmt,
)
from qiime2 import Artifact
from qiime2.metadata import Metadata
from qiime2.plugin.testing import TestPluginBase

from q2_fondue.sequences import (
    get_sequences,
    _run_fasterq_dump,
    _process_downloaded_sequences,
    _write_empty_casava,
    combine_seqs,
    _write_to_casava,
    _get_sequences,
)
from q2_fondue.utils import DownloadError


class MockTempDir(tempfile.TemporaryDirectory):
    pass


class SequenceTests(TestPluginBase):
    # class is inspired by class SubsampleTest in
    # q2_demux.tests.test_subsample
    package = "q2_fondue.tests"

    @classmethod
    def setUpClass(cls) -> None:
        cls.fake_logger = logging.getLogger("test_log")

    def move_files_to_tmp_dir(self, files):
        test_temp_dir = MockTempDir()

        for file in files:
            path_seq_single = self.get_data_path(file)

            shutil.copy(path_seq_single, os.path.join(test_temp_dir.name, file))

        return test_temp_dir

    def _validate_sequences_in_samples(self, read_output):
        nb_obs_samples = 0
        ls_seq_length = []
        samples = read_output.sequences.iter_views(FastqGzFormat)

        # iterate over each sample
        for _, file_loc in samples:
            # assemble sequences
            nb_obs_samples += 1
            file_fh = gzip.open(str(file_loc), "rt")

            # Assemble expected sequences, per-sample
            file_seqs = [r for r in itertools.zip_longest(*[file_fh] * 4)]

            ls_seq_length.append(len(file_seqs))

        return nb_obs_samples, ls_seq_length

    def validate_counts(
        self, single_output, paired_output, ls_exp_lengths_single, ls_exp_lengths_paired
    ):
        nb_samples_single, ls_seq_length_single = self._validate_sequences_in_samples(
            single_output
        )
        self.assertTrue(nb_samples_single == 1)
        self.assertTrue(ls_seq_length_single == ls_exp_lengths_single)

        # test paired sequences
        nb_samples_paired, ls_seq_length_paired = self._validate_sequences_in_samples(
            paired_output
        )
        self.assertTrue(nb_samples_paired == 2)
        self.assertTrue(ls_seq_length_paired == ls_exp_lengths_paired)

    def prepare_metadata(self, acc_id, to_artifact=False):
        acc_id_tsv = f"{acc_id}_md.tsv"
        self.move_files_to_tmp_dir([acc_id_tsv])
        fp = self.get_data_path(acc_id_tsv)
        if to_artifact:
            return Artifact.import_data("NCBIAccessionIDs", fp)
        else:
            return Metadata.load(fp)


class TestUtils4SequenceFetching(SequenceTests):

    @patch("os.remove")
    @patch("subprocess.run", return_value=MagicMock(returncode=0))
    @patch("q2_fondue.sequences._has_enough_space", return_value=True)
    def test_run_cmd_fasterq_sra_file(self, mock_space_check, mock_subprocess, mock_rm):
        test_temp_dir = self.move_files_to_tmp_dir(["testaccA.fastq", "testaccA.sra"])

        exp_prefetch = ["prefetch", "-X", "u", "-O", "testaccA", "testaccA"]
        exp_fasterq = [
            "fasterq-dump",
            "-e",
            "6",
            "--size-check",
            "on",
            "-x",
            "testaccA",
        ]

        _run_fasterq_dump(
            "testaccA", test_temp_dir.name, threads=6, key_file="", retries=0
        )
        mock_subprocess.assert_has_calls(
            [
                call(
                    exp_prefetch, text=True, capture_output=True, cwd=test_temp_dir.name
                ),
                call(
                    exp_fasterq, text=True, capture_output=True, cwd=test_temp_dir.name
                ),
            ]
        )
        mock_rm.assert_called_with(os.path.join(test_temp_dir.name, "testaccA.sra"))
        mock_space_check.assert_not_called()

    @patch("shutil.rmtree")
    @patch("subprocess.run", return_value=MagicMock(returncode=0))
    @patch("q2_fondue.sequences._has_enough_space", return_value=True)
    def test_run_cmd_fasterq_sra_directory(
        self, mock_space_check, mock_subprocess, mock_rm
    ):
        test_temp_dir = self.move_files_to_tmp_dir(["testaccA.fastq"])
        os.makedirs(f"{test_temp_dir.name}/testaccA")

        exp_prefetch = ["prefetch", "-X", "u", "-O", "testaccA", "testaccA"]
        exp_fasterq = [
            "fasterq-dump",
            "-e",
            "6",
            "--size-check",
            "on",
            "-x",
            "testaccA",
        ]

        _run_fasterq_dump(
            "testaccA",
            test_temp_dir.name,
            threads=6,
            key_file="",
            retries=0,
        )
        mock_subprocess.assert_has_calls(
            [
                call(
                    exp_prefetch, text=True, capture_output=True, cwd=test_temp_dir.name
                ),
                call(
                    exp_fasterq, text=True, capture_output=True, cwd=test_temp_dir.name
                ),
            ]
        )
        mock_rm.assert_called_with(os.path.join(test_temp_dir.name, "testaccA"))
        mock_space_check.assert_not_called()

    @patch("shutil.rmtree")
    @patch("subprocess.run", return_value=MagicMock(returncode=0))
    @patch("q2_fondue.sequences._has_enough_space", return_value=True)
    def test_run_cmd_fasterq_with_restricted_key(
        self, mock_space_check, mock_subprocess, mock_rm
    ):
        test_temp_dir = self.move_files_to_tmp_dir(["testaccA.fastq"])
        os.makedirs(f"{test_temp_dir.name}/testaccA")

        key = "mykey.ngc"
        exp_prefetch = [
            "prefetch",
            "-X",
            "u",
            "-O",
            "testaccA",
            "--ngc",
            key,
            "testaccA",
        ]
        exp_fasterq = [
            "fasterq-dump",
            "-e",
            "6",
            "--size-check",
            "on",
            "-x",
            "--ngc",
            key,
            "testaccA",
        ]

        _run_fasterq_dump(
            "testaccA",
            test_temp_dir.name,
            threads=6,
            key_file=key,
            retries=0,
        )
        mock_subprocess.assert_has_calls(
            [
                call(
                    exp_prefetch, text=True, capture_output=True, cwd=test_temp_dir.name
                ),
                call(
                    exp_fasterq, text=True, capture_output=True, cwd=test_temp_dir.name
                ),
            ]
        )
        mock_rm.assert_called_with(os.path.join(test_temp_dir.name, "testaccA"))
        mock_space_check.assert_not_called()

    @patch("os.remove")
    @patch("subprocess.run", return_value=MagicMock(returncode=0))
    @patch("q2_fondue.sequences._has_enough_space", return_value=True)
    def test_run_fasterq_dump_for_all(self, mock_space_check, mock_subprocess, mock_rm):
        test_temp_dir = self.move_files_to_tmp_dir(["testaccA.fastq", "testaccA.sra"])
        exp_prefetch = ["prefetch", "-X", "u", "-O", "testaccA", "testaccA"]
        exp_fasterq = [
            "fasterq-dump",
            "-e",
            "6",
            "--size-check",
            "on",
            "-x",
            "testaccA",
        ]

        with self.assertLogs("q2_fondue.sequences", level="INFO") as cm:
            success, error_msg = _run_fasterq_dump(
                "testaccA",
                test_temp_dir.name,
                threads=6,
                key_file="",
                retries=0,
            )
            self.assertTrue(success)
            self.assertIsNone(error_msg)
            mock_subprocess.assert_has_calls(
                [
                    call(
                        exp_prefetch,
                        text=True,
                        capture_output=True,
                        cwd=test_temp_dir.name,
                    ),
                    call(
                        exp_fasterq,
                        text=True,
                        capture_output=True,
                        cwd=test_temp_dir.name,
                    ),
                ]
            )
            mock_rm.assert_called_with(os.path.join(test_temp_dir.name, "testaccA.sra"))
            mock_space_check.assert_not_called()
            self.assertIn("INFO:q2_fondue.sequences:Downloading sequences", cm.output)
            self.assertIn(
                "INFO:q2_fondue.sequences:Successfully downloaded sequences", cm.output
            )

    @patch("time.sleep")
    @patch("subprocess.run", return_value=MagicMock(stderr="Some error", returncode=1))
    @patch("q2_fondue.sequences._has_enough_space", return_value=True)
    def test_run_fasterq_dump_for_all_error(
        self, mock_space_check, mock_subprocess, mock_sleep
    ):
        test_temp_dir = MockTempDir()
        with self.assertLogs("q2_fondue.sequences", level="INFO") as cm:
            success, error_msg = _run_fasterq_dump(
                "testaccERROR", test_temp_dir.name, threads=6, key_file="", retries=1
            )
            self.assertFalse(success)
            self.assertEqual(error_msg, "Some error")
            # check retry procedure:
            self.assertEqual(mock_subprocess.call_count, 2)
            mock_space_check.assert_not_called()
            self.assertIn(
                "ERROR:q2_fondue.sequences:Fetching failed. Error: Some error",
                cm.output,
            )
            self.assertIn(
                "ERROR:q2_fondue.sequences:Failed to download sequences", cm.output
            )

    @patch("shutil.rmtree")
    @patch("shutil.disk_usage", side_effect=[(0, 0, 10), (0, 0, 2)])
    @patch("subprocess.run", return_value=MagicMock(returncode=0))
    @patch("q2_fondue.sequences._has_enough_space", return_value=False)
    def test_run_fasterq_dump_for_all_space_error(
        self, mock_space_check, mock_subprocess, mock_disk_usage, mock_rm
    ):
        # test checking that space availability break procedure works
        test_temp_dir = MockTempDir()
        os.makedirs(f"{test_temp_dir.name}/testaccA")

        with self.assertLogs("q2_fondue.sequences", level="INFO"):
            success, error_msg = _run_fasterq_dump(
                "testaccERROR",
                test_temp_dir.name,
                threads=6,
                key_file="",
                retries=2,
            )
            self.assertFalse(success)
            self.assertEqual(error_msg, "Not enough space for fasterq-dump")
            mock_subprocess.assert_not_called()
            self.assertEqual(mock_disk_usage.call_count, 2)
            mock_space_check.assert_called_once_with("testaccERROR", test_temp_dir.name)

    def test_process_downloaded_sequences_single(self):
        ids = ["testaccA", "testacc_1"]
        test_temp_dir = self.move_files_to_tmp_dir([f"{x}.fastq" for x in ids])

        single, paired = _process_downloaded_sequences(
            accession_id="testaccA",
            output_dir=test_temp_dir.name,
        )
        self.assertListEqual(
            single,
            [
                (
                    os.path.join(test_temp_dir.name, "testaccA_01_L001_R1_001.fastq"),
                    False,
                )
            ],
        )
        self.assertListEqual(paired, [])

    def test_process_downloaded_sequences_paired(self):
        ids = ["testacc_1", "testacc_2"]
        test_temp_dir = self.move_files_to_tmp_dir([f"{x}.fastq" for x in ids])

        single, paired = _process_downloaded_sequences(
            accession_id="testacc",
            output_dir=test_temp_dir.name,
        )
        self.assertListEqual(single, [])
        self.assertListEqual(
            paired,
            [
                (
                    os.path.join(test_temp_dir.name, "testacc_00_L001_R1_001.fastq"),
                    True,
                ),
                (
                    os.path.join(test_temp_dir.name, "testacc_00_L001_R2_001.fastq"),
                    True,
                ),
            ],
        )

    def test_process_downloaded_sequences_paired_n_single_content(self):
        ids = ["testaccHYB", "testaccHYB_1", "testaccHYB_2"]
        test_temp_dir = self.move_files_to_tmp_dir([f"{x}.fastq" for x in ids])

        single, paired = _process_downloaded_sequences(
            accession_id="testaccHYB",
            output_dir=test_temp_dir.name,
        )

        self.assertListEqual(
            single,
            [
                (
                    os.path.join(test_temp_dir.name, "testaccHYB_01_L001_R1_001.fastq"),
                    False,
                )
            ],
        )
        self.assertListEqual(
            paired,
            [
                (
                    os.path.join(test_temp_dir.name, "testaccHYB_00_L001_R1_001.fastq"),
                    True,
                ),
                (
                    os.path.join(test_temp_dir.name, "testaccHYB_00_L001_R2_001.fastq"),
                    True,
                ),
            ],
        )

        # test that file contents are the same
        self.assertTrue(
            filecmp.cmp(single[0][0], self.get_data_path("testaccHYB.fastq"))
        )
        self.assertTrue(
            filecmp.cmp(paired[0][0], self.get_data_path("testaccHYB_1.fastq"))
        )
        self.assertTrue(
            filecmp.cmp(paired[1][0], self.get_data_path("testaccHYB_2.fastq"))
        )

    def test_write_empty_casava_single(self):
        casava_out = CasavaOneEightSingleLanePerSampleDirFmt()
        with self.assertLogs("q2_fondue.sequences", level="INFO") as cm:
            _write_empty_casava("single", str(casava_out), "ABC")
            self.assertTrue(
                os.path.isfile(
                    os.path.join(str(casava_out), "xxx_01_L001_R1_001.fastq.gz")
                )
            )
            self.assertIn(
                "WARNING:q2_fondue.sequences:No single-end " "sequences available",
                cm.output,
            )

    def test_write_empty_casava_paired(self):
        casava_out = CasavaOneEightSingleLanePerSampleDirFmt()
        with self.assertLogs("q2_fondue.sequences", level="INFO") as cm:
            _write_empty_casava("paired", str(casava_out), "ABC")

            self.assertTrue(
                os.path.isfile(
                    os.path.join(str(casava_out), "xxx_00_L001_R1_001.fastq.gz")
                )
            )
            self.assertTrue(
                os.path.isfile(
                    os.path.join(str(casava_out), "xxx_00_L001_R2_001.fastq.gz")
                )
            )
            self.assertIn(
                "WARNING:q2_fondue.sequences:No paired-end " "sequences available",
                cm.output,
            )

    def test_write_to_casava_dir_single(self):
        casava_out = CasavaOneEightSingleLanePerSampleDirFmt()
        single_files = ["testaccA_01_L001_R1_001.fastq"]
        test_temp_dir = self.move_files_to_tmp_dir(single_files)

        _write_to_casava(
            [(x, False) for x in single_files],
            test_temp_dir.name,
            str(casava_out),
            "testaccA",
        )

        self.assertTrue(
            os.path.isfile(os.path.join(str(casava_out), f"{single_files[0]}.gz"))
        )
        self.assertTupleEqual((1, [3]), self._validate_sequences_in_samples(casava_out))

    def test_write_to_casava_dir_paired(self):
        casava_out = CasavaOneEightSingleLanePerSampleDirFmt()
        paired_files = ["testacc_00_L001_R1_001.fastq", "testacc_00_L001_R2_001.fastq"]
        test_temp_dir = self.move_files_to_tmp_dir(paired_files)

        _write_to_casava(
            [(x, True) for x in paired_files],
            test_temp_dir.name,
            str(casava_out),
            "testacc",
        )

        self.assertTrue(
            os.path.isfile(os.path.join(str(casava_out), f"{paired_files[0]}.gz"))
        )
        self.assertTrue(
            os.path.isfile(os.path.join(str(casava_out), f"{paired_files[1]}.gz"))
        )
        self.assertTupleEqual(
            (2, [3, 3]), self._validate_sequences_in_samples(casava_out)
        )


class TestSequenceFetching(SequenceTests):

    @patch("tempfile.TemporaryDirectory")
    @patch("q2_fondue.sequences._remove_empty", return_value=["s1", "p1"])
    @patch("q2_fondue.sequences._make_empty_artifact")
    def test_get_sequences_pipeline(self, mock_empty, mock_remove, mock_tmpdir):
        # metadata contains two run IDs
        ids = self.prepare_metadata("SRR1234567", to_artifact=True)

        ctx = MagicMock()
        failed_artifact = Artifact.import_data(
            "SRAFailedIDs",
            pd.DataFrame(columns=["Error message"], index=pd.Index([], name="ID")),
        )
        obs_single = CasavaOneEightSingleLanePerSampleDirFmt()
        obs_paired = CasavaOneEightSingleLanePerSampleDirFmt()
        obs_combined = CasavaOneEightSingleLanePerSampleDirFmt()
        action_get = MagicMock(return_value=(obs_single, obs_paired, failed_artifact))
        action_combine = MagicMock(return_value=(obs_combined,))
        ctx.get_action.side_effect = lambda plugin, action: {
            ("fondue", "_get_sequences"): action_get,
            ("fondue", "combine_seqs"): action_combine,
        }[(plugin, action)]
        ctx.make_artifact.return_value = failed_artifact

        casava_single, casava_paired, failed_ids = get_sequences(
            ctx, ids, email="some@where.com", retries=0, n_download_jobs=6
        )

        self.assertIsInstance(casava_single, CasavaOneEightSingleLanePerSampleDirFmt)
        self.assertIsInstance(casava_paired, CasavaOneEightSingleLanePerSampleDirFmt)
        pd.testing.assert_frame_equal(
            failed_ids.view(pd.DataFrame),
            pd.DataFrame([], index=pd.Index([], name="ID"), columns=["Error message"]),
            check_dtype=False,
        )
        mock_remove.assert_called_once_with(
            [obs_single, obs_single], [obs_paired, obs_paired]
        )
        action_get.assert_has_calls(
            [
                call("SRR123456", 0, 6, "INFO", False),
                call("SRR123457", 0, 6, "INFO", False),
            ],
            any_order=True,
        )
        action_combine.assert_has_calls([call("s1"), call("p1")])
        mock_empty.assert_not_called()

    @patch("tempfile.TemporaryDirectory")
    @patch("q2_fondue.sequences._remove_empty", return_value=["s1", "p1"])
    @patch("q2_fondue.sequences._make_empty_artifact")
    def test_get_sequences_pipeline_with_failed_all(
        self, mock_empty, mock_remove, mock_tmpdir
    ):
        # metadata contains two run IDs
        ids = self.prepare_metadata("SRR1234567", to_artifact=True)

        ctx = MagicMock()
        failed_df1 = pd.DataFrame(
            data={"Error message": ["Some error 1"]},
            index=pd.Index(["SRR123456"], name="ID"),
        )
        failed_df2 = pd.DataFrame(
            data={"Error message": ["Some error 2"]},
            index=pd.Index(["SRR123457"], name="ID"),
        )
        failed_artifact1 = Artifact.import_data("SRAFailedIDs", failed_df1)
        failed_artifact2 = Artifact.import_data("SRAFailedIDs", failed_df2)
        obs_single = CasavaOneEightSingleLanePerSampleDirFmt()
        obs_paired = CasavaOneEightSingleLanePerSampleDirFmt()
        obs_combined = CasavaOneEightSingleLanePerSampleDirFmt()
        action_get = MagicMock(
            side_effect=[
                (obs_single, obs_paired, failed_artifact1),
                (obs_single, obs_paired, failed_artifact2),
            ]
        )
        action_combine = MagicMock(return_value=(obs_combined,))
        ctx.get_action.side_effect = lambda plugin, action: {
            ("fondue", "_get_sequences"): action_get,
            ("fondue", "combine_seqs"): action_combine,
        }[(plugin, action)]
        ctx.make_artifact.return_value = failed_artifact2

        casava_single, casava_paired, failed_ids = get_sequences(
            ctx, ids, email="some@where.com", retries=0, n_download_jobs=6
        )

        self.assertIsInstance(casava_single, CasavaOneEightSingleLanePerSampleDirFmt)
        self.assertIsInstance(casava_paired, CasavaOneEightSingleLanePerSampleDirFmt)
        mock_remove.assert_called_once_with(
            [obs_single, obs_single], [obs_paired, obs_paired]
        )
        action_get.assert_has_calls(
            [
                call("SRR123456", 0, 6, "INFO", False),
                call("SRR123457", 0, 6, "INFO", False),
            ],
            any_order=True,
        )
        action_combine.assert_has_calls([call("s1"), call("p1")])
        mock_empty.assert_not_called()
        failed_df_combined = pd.DataFrame(
            data={"Error message": ["Some error 1", "Some error 2"]},
            index=pd.Index(["SRR123456", "SRR123457"], name="ID"),
        )
        pd.testing.assert_frame_equal(
            ctx.make_artifact.call_args.args[1], failed_df_combined
        )

    @parameterized.expand(
        [
            ("study", "SRP123456"),
            ("bioproject", "PRJNA734376"),
            ("experiment", "SRX123456"),
            ("sample", "SRS123456"),
        ]
    )
    @patch("tempfile.TemporaryDirectory")
    @patch("q2_fondue.sequences._remove_empty", return_value=["s1", "p1"])
    @patch("q2_fondue.sequences._make_empty_artifact")
    @patch("q2_fondue.sequences._get_run_ids", return_value=["SRR123456"])
    def test_get_sequences_pipeline_various_ids(
        self, id_type, acc_id, mock_get, mock_empty, mock_remove, mock_tmpdir
    ):
        # metadata contains two run IDs
        ids = self.prepare_metadata(acc_id, to_artifact=True)

        ctx = MagicMock()
        failed_artifact = Artifact.import_data(
            "SRAFailedIDs",
            pd.DataFrame(columns=["Error message"], index=pd.Index([], name="ID")),
        )
        obs_single = CasavaOneEightSingleLanePerSampleDirFmt()
        obs_paired = CasavaOneEightSingleLanePerSampleDirFmt()
        obs_combined = CasavaOneEightSingleLanePerSampleDirFmt()
        action_get = MagicMock(return_value=(obs_single, obs_paired, failed_artifact))
        action_combine = MagicMock(return_value=(obs_combined,))
        ctx.get_action.side_effect = lambda plugin, action: {
            ("fondue", "_get_sequences"): action_get,
            ("fondue", "combine_seqs"): action_combine,
        }[(plugin, action)]
        ctx.make_artifact.return_value = failed_artifact

        get_sequences(ctx, ids, email="some@where.com", retries=0, n_download_jobs=6)

        mock_get.assert_called_with(
            "some@where.com", 1, [acc_id], None, id_type, "INFO"
        )

    @patch("q2_fondue.sequences._run_fasterq_dump", return_value=(True, None))
    def test_get_sequences_single(self, mock_fasterq):
        files = ["testaccA.fastq"]
        mock_tmpdir = self.move_files_to_tmp_dir(files)

        with patch("tempfile.TemporaryDirectory", return_value=mock_tmpdir):
            single, paired, failed = _get_sequences("testaccA", 3, 6, "INFO", False)

        self.assertIsInstance(single, CasavaOneEightSingleLanePerSampleDirFmt)
        self.assertIsInstance(paired, CasavaOneEightSingleLanePerSampleDirFmt)
        self.assertIsInstance(failed, pd.DataFrame)

        obs_single_files = glob.glob(f"{str(single)}/*.fastq.gz")
        obs_paired_files = glob.glob(f"{str(paired)}/*.fastq.gz")
        exp_single_files = [
            os.path.join(str(single), "testaccA_01_L001_R1_001.fastq.gz")
        ]
        exp_paired_files = [
            os.path.join(str(paired), "xxx_00_L001_R1_001.fastq.gz"),
            os.path.join(str(paired), "xxx_00_L001_R2_001.fastq.gz"),
        ]
        self.assertListEqual(obs_single_files, exp_single_files)
        self.assertListEqual(obs_paired_files, exp_paired_files)
        pd.testing.assert_frame_equal(
            failed,
            pd.DataFrame(columns=["Error message"], index=pd.Index([], name="ID")),
            check_dtype=False,
        )

    @patch("q2_fondue.sequences._run_fasterq_dump", return_value=(True, None))
    def test_get_sequences_paired(self, mock_fasterq):
        files = ["testacc_1.fastq", "testacc_2.fastq"]
        mock_tmpdir = self.move_files_to_tmp_dir(files)

        with patch("tempfile.TemporaryDirectory", return_value=mock_tmpdir):
            single, paired, failed = _get_sequences("testacc", 3, 6, "INFO", False)

        self.assertIsInstance(single, CasavaOneEightSingleLanePerSampleDirFmt)
        self.assertIsInstance(paired, CasavaOneEightSingleLanePerSampleDirFmt)
        self.assertIsInstance(failed, pd.DataFrame)

        obs_single_files = glob.glob(f"{str(single)}/*.fastq.gz")
        obs_paired_files = glob.glob(f"{str(paired)}/*.fastq.gz")
        exp_single_files = [os.path.join(str(single), "xxx_01_L001_R1_001.fastq.gz")]
        exp_paired_files = [
            os.path.join(str(paired), "testacc_00_L001_R1_001.fastq.gz"),
            os.path.join(str(paired), "testacc_00_L001_R2_001.fastq.gz"),
        ]
        self.assertListEqual(obs_single_files, exp_single_files)
        self.assertListEqual(obs_paired_files, exp_paired_files)
        pd.testing.assert_frame_equal(
            failed,
            pd.DataFrame(columns=["Error message"], index=pd.Index([], name="ID")),
            check_dtype=False,
        )

    @patch("q2_fondue.sequences._run_fasterq_dump", return_value=(False, "Some error"))
    def test_get_sequences_no_success(self, mock_fasterq):
        single, paired, failed = _get_sequences("testacc", 3, 6, "INFO", False)

        self.assertIsInstance(single, CasavaOneEightSingleLanePerSampleDirFmt)
        self.assertIsInstance(paired, CasavaOneEightSingleLanePerSampleDirFmt)
        self.assertIsInstance(failed, pd.DataFrame)

        obs_single_files = glob.glob(f"{str(single)}/*.fastq.gz")
        obs_paired_files = glob.glob(f"{str(paired)}/*.fastq.gz")
        exp_single_files = [os.path.join(str(single), "xxx_01_L001_R1_001.fastq.gz")]
        exp_paired_files = [
            os.path.join(str(paired), "xxx_00_L001_R1_001.fastq.gz"),
            os.path.join(str(paired), "xxx_00_L001_R2_001.fastq.gz"),
        ]
        self.assertListEqual(obs_single_files, exp_single_files)
        self.assertListEqual(obs_paired_files, exp_paired_files)
        pd.testing.assert_frame_equal(
            failed,
            pd.DataFrame(
                data={"Error message": ["Some error"]},
                index=pd.Index(["testacc"], name="ID"),
            ),
            check_dtype=False,
        )

    @patch("q2_fondue.sequences._run_fasterq_dump", return_value=(True, None))
    def test_get_sequences_nothing_downloaded(self, mock_fasterq):
        with self.assertRaisesRegex(
            DownloadError,
            "Neither single- nor paired-end sequences could be downloaded",
        ):
            _get_sequences("ABC", 3, 6, "INFO", False)

    @patch.dict(os.environ, {"KEY_FILEPATH": "path/to/key.ngc"})
    def test_get_sequences_no_keyfile(self):
        with self.assertRaisesRegex(
            ValueError, "The provided dbGAP repository key filepath does not exist"
        ):
            _get_sequences("ABC", 3, 6, "INFO", True)


class TestSequenceCombining(SequenceTests):

    def load_seq_artifact(self, type="single", suffix=1):
        t = "" if type == "single" else "PairedEnd"
        return Artifact.import_data(
            f"SampleData[{t}SequencesWithQuality]",
            self.get_data_path(f"{type}{suffix}"),
            CasavaOneEightSingleLanePerSampleDirFmt,
        ).view(CasavaOneEightSingleLanePerSampleDirFmt)

    def test_combine_samples_single(self):
        seqs = [
            self.load_seq_artifact("single", 1),
            self.load_seq_artifact("single", 2),
        ]
        obs_seqs = combine_seqs(seqs=seqs)
        exp_ids = pd.Index(["SEQID1", "SEQID2", "SEQID3", "SEQID4"], name="sample-id")
        self.assertIsInstance(obs_seqs, CasavaOneEightSingleLanePerSampleDirFmt)
        pd.testing.assert_index_equal(obs_seqs.manifest.index, exp_ids)
        self.assertFalse(all(obs_seqs.manifest.reverse))

    def test_combine_samples_paired(self):
        seqs = [
            self.load_seq_artifact("paired", 1),
            self.load_seq_artifact("paired", 2),
        ]
        obs_seqs = combine_seqs(seqs=seqs)
        exp_ids = pd.Index(["SEQID1", "SEQID2", "SEQID3", "SEQID4"], name="sample-id")
        self.assertIsInstance(obs_seqs, CasavaOneEightSingleLanePerSampleDirFmt)
        pd.testing.assert_index_equal(obs_seqs.manifest.index, exp_ids)
        self.assertTrue(all(obs_seqs.manifest.reverse))

    def test_combine_samples_single_duplicated_error(self):
        seqs = [self.load_seq_artifact("single", 1)] * 2

        with self.assertRaisesRegex(
            ValueError, "Duplicate sequence files.*SEQID1, SEQID2."
        ):
            combine_seqs(seqs=seqs, on_duplicates="error")

    def test_combine_samples_single_duplicated_warning(self):
        seqs = [self.load_seq_artifact("single", 1)] * 2

        with self.assertWarnsRegex(
            Warning, "Duplicate sequence files.*dropped.*SEQID1, SEQID2."
        ):
            obs_seqs = combine_seqs(seqs=seqs, on_duplicates="warn")
            exp_ids = pd.Index(["SEQID1", "SEQID2"], name="sample-id")

            self.assertIsInstance(obs_seqs, CasavaOneEightSingleLanePerSampleDirFmt)
            pd.testing.assert_index_equal(obs_seqs.manifest.index, exp_ids)
            self.assertFalse(all(obs_seqs.manifest.reverse))

    def test_combine_samples_paired_duplicated_error(self):
        seqs = [self.load_seq_artifact("paired", 1)] * 2

        with self.assertRaisesRegex(
            ValueError, "Duplicate sequence files.*SEQID1, SEQID2."
        ):
            combine_seqs(seqs=seqs, on_duplicates="error")

    def test_combine_samples_paired_duplicated_warning(self):
        seqs = [self.load_seq_artifact("paired", 1)] * 2

        with self.assertWarnsRegex(
            Warning, "Duplicate sequence files.*dropped.*SEQID1, SEQID2."
        ):
            obs_seqs = combine_seqs(seqs=seqs, on_duplicates="warn")
            exp_ids = pd.Index(["SEQID1", "SEQID2"], name="sample-id")

            self.assertIsInstance(obs_seqs, CasavaOneEightSingleLanePerSampleDirFmt)
            pd.testing.assert_index_equal(obs_seqs.manifest.index, exp_ids)
            self.assertTrue(all(obs_seqs.manifest.reverse))

    def test_combine_samples_paired_with_empty_warning(self):
        seqs = [
            self.load_seq_artifact("paired", 1),
            self.load_seq_artifact("empty", ""),
        ]

        with self.assertWarnsRegex(
            Warning, "1 empty sequence files were found and excluded."
        ):
            obs_seqs = combine_seqs(seqs=seqs, on_duplicates="warn")
            exp_ids = pd.Index(["SEQID1", "SEQID2"], name="sample-id")

            self.assertIsInstance(obs_seqs, CasavaOneEightSingleLanePerSampleDirFmt)
            pd.testing.assert_index_equal(obs_seqs.manifest.index, exp_ids)
            self.assertTrue(all(obs_seqs.manifest.reverse))
