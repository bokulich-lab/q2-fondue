# ----------------------------------------------------------------------------
# Copyright (c) 2022, Bokulich Laboratories.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

import logging
import sys

import pandas as pd

PREFIX = {
    'run': ('SRR', 'ERR', 'DRR'),
    'experiment': ('SRX', 'ERX', 'DRX'),
    'sample': ('SRS', 'ERS', 'DRS'),
    'study': ('SRP', 'ERP', 'DRP'),
    'bioproject': ('PRJ', )
}


class InvalidIDs(Exception):
    pass


def get_attrs(obj, excluded=()):
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

    df.rename(columns=col_map, inplace=True)

    # rename Sample ID to Sample Accession (incompatible with qiime naming)
    df.rename(columns={'Sample ID': 'Sample Accession'}, inplace=True)

    return df


def set_up_entrezpy_logging(entrezpy_obj, log_level):
    """Sets up logging for the given Entrezpy object.

    Args:
        entrezpy_obj (object): An Entrezpy object that has a logger attribute.
        log_level (str): The log level to set.
    """
    handler = set_up_logging_handler()

    entrezpy_obj.logger.addHandler(handler)
    entrezpy_obj.logger.setLevel(log_level)

    if hasattr(entrezpy_obj, 'request_pool'):
        entrezpy_obj.request_pool.logger.addHandler(handler)
        entrezpy_obj.request_pool.logger.setLevel(log_level)


def set_up_logger(log_level, cls_obj=None, logger_name=None) -> logging.Logger:
    """Sets up the module/class logger.

    Args:
        log_level (str): The log level to set.
        cls_obj: Class instance for which the logger should be created.

    Returns:
        logging.Logger: The module logger.
    """
    if cls_obj:
        logger = logging.getLogger(
            f'{cls_obj.__module__}'
        )
    else:
        logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    handler = set_up_logging_handler()
    logger.addHandler(handler)
    return logger


def set_up_logging_handler():
    """Sets up logging handler."""
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '%(asctime)s [%(threadName)s] [%(levelname)s] '
        '[%(name)s] [%(accession_id)s]: %(message)s')
    handler.setFormatter(formatter)
    return handler
