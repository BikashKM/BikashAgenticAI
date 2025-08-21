from __future__ import annotations
import io
import boto3
import pandas as pd
from urllib.parse import urlparse

class S3Connector:
    def __init__(self, region: str):
        self.region = region
        self.s3 = boto3.client('s3', region_name=region)

    def _parse(self, uri: str):
        p = urlparse(uri)
        bucket = p.netloc
        key = p.path.lstrip('/')
        return bucket, key

    def write_csv(self, df: pd.DataFrame, uri: str):
        bucket, key = self._parse(uri)
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        self.s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue().encode('utf-8'))

    def read_csv(self, uri: str) -> pd.DataFrame:
        bucket, key = self._parse(uri)
        obj = self.s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(io.BytesIO(obj['Body'].read()))
