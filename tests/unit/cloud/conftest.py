"""Shared moto + AWS credential fixtures for cloud unit tests."""

from __future__ import annotations

from collections.abc import Iterator

import pytest


@pytest.fixture
def aws_test_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Minimal AWS env vars for moto / aioboto3."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def moto_s3_endpoint(aws_test_env: None) -> Iterator[str]:
    """ThreadedMotoServer for S3 — aioboto3 async transport needs real HTTP."""
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0)
    server.start()
    assert server._server is not None
    port = server._server.server_address[1]
    endpoint = f"http://127.0.0.1:{port}"
    yield endpoint
    server.stop()


@pytest.fixture
def moto_ec2_endpoint(aws_test_env: None) -> Iterator[str]:
    """ThreadedMotoServer for EC2 — aioboto3 async transport needs real HTTP."""
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0)
    server.start()
    assert server._server is not None
    port = server._server.server_address[1]
    endpoint = f"http://127.0.0.1:{port}"
    yield endpoint
    server.stop()


def create_s3_bucket(endpoint: str, bucket: str, *, region: str = "us-east-1") -> None:
    import boto3

    boto3.client("s3", endpoint_url=endpoint, region_name=region).create_bucket(Bucket=bucket)
