"""
Common helpers shared by all the ETL jobs.

Paths are resolved relative to this file so the jobs work the same on the
Airflow workers and on a laptop.
"""
import logging
import os

import pandas as pd

logger = logging.getLogger("etl")

# src/etl/../../data
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
BASE_DIR = os.path.abspath(os.path.join(DATA_DIR, ".."))
PROCESSED_DIR = os.path.abspath(os.path.join(DATA_DIR, "processed"))


def full_path(path):
    """Turn a repo relative path (data/incoming/x.csv) into an absolute one."""
    if os.path.isabs(path):
        return path
    return os.path.normpath(os.path.join(BASE_DIR, path))


def read_csv(path, **kwargs):
    path = full_path(path)
    if not os.path.exists(path):
        raise FileNotFoundError("input file not found: %s (did the upstream drop run?)" % path)
    logger.info("reading %s", path)
    df = pd.read_csv(path, **kwargs)
    # upstream exports sometimes come with spaces in the header
    df.columns = [str(c).strip() for c in df.columns]
    return df


def write_output(df, name):
    """Write a dataframe to data/processed/<name>.csv and return the path."""
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    out = os.path.join(PROCESSED_DIR, name + ".csv")
    df.to_csv(out, index=False)
    logger.info("wrote %s rows to %s", len(df), out)
    return out


def read_output(name, **kwargs):
    """Read back something written with write_output (used to pass data between tasks)."""
    return read_csv(os.path.join(PROCESSED_DIR, name + ".csv"), **kwargs)
