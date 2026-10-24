"""
Common helpers shared by all the ETL jobs.

Paths are resolved relative to this file so the jobs work the same in Lambda and on a
laptop. In Lambda (LANDING_BUCKET is set) the data/ folder is the landing bucket:
data/incoming/<file> is s3://<bucket>/incoming/<file> and the job outputs go to
s3://<bucket>/processed/<name>.csv. Without LANDING_BUCKET the files are read from and
written to data/ in the repository.
"""

import io
import logging
import os

import pandas as pd

logger = logging.getLogger("etl")

# src/etl/../../data
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
BASE_DIR = os.path.abspath(os.path.join(DATA_DIR, ".."))
PROCESSED_DIR = os.path.abspath(os.path.join(DATA_DIR, "processed"))


def landing_bucket():
    """The landing bucket of the Lambda functions (None when running locally)."""
    return os.environ.get("LANDING_BUCKET") or None


def full_path(path):
    """Turn a repo relative path (data/incoming/x.csv) into an absolute one."""
    if os.path.isabs(path):
        return path
    return os.path.normpath(os.path.join(BASE_DIR, path))


def s3_key(path):
    """The landing bucket key of a data/ path (data/incoming/x.csv -> incoming/x.csv)."""
    relative = os.path.relpath(full_path(path), os.path.abspath(DATA_DIR))
    return relative.replace(os.sep, "/")


def _s3():
    import boto3  # only the Lambda functions talk to S3

    return boto3.client("s3")


def read_csv(path, **kwargs):
    bucket = landing_bucket()
    if bucket:
        key = s3_key(path)
        logger.info("reading s3://%s/%s", bucket, key)
        body = _s3().get_object(Bucket=bucket, Key=key)["Body"].read()
        df = pd.read_csv(io.BytesIO(body), **kwargs)
    else:
        path = full_path(path)
        if not os.path.exists(path):
            raise FileNotFoundError(f"input file not found: {path} (did the upstream drop run?)")
        logger.info("reading %s", path)
        df = pd.read_csv(path, **kwargs)
    # upstream exports sometimes come with spaces in the header
    df.columns = [str(c).strip() for c in df.columns]
    return df


def write_output(df, name):
    """Write a dataframe to data/processed/<name>.csv (the landing bucket in Lambda), return where."""
    bucket = landing_bucket()
    if bucket:
        key = f"processed/{name}.csv"
        _s3().put_object(Bucket=bucket, Key=key, Body=df.to_csv(index=False).encode("utf-8"))
        out = f"s3://{bucket}/{key}"
    else:
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        out = os.path.join(PROCESSED_DIR, name + ".csv")
        df.to_csv(out, index=False)
    logger.info("wrote %s rows to %s", len(df), out)
    return out


def read_output(name, **kwargs):
    """Read back something written with write_output (used to pass data between steps)."""
    return read_csv(os.path.join(PROCESSED_DIR, name + ".csv"), **kwargs)
