import base64
from datetime import datetime, timezone
from io import BytesIO
from uuid import uuid4

import pytest
from sqlalchemy import text

from dbd.client.async_client import ApiResponse
from dbd.contracts.identity import parse_code
from dbd.runtime.worker import process
from dbd.storage.archive_backend import S3ArchiveBackend
from dbd.storage.mysql.repositories.worker_repository import WorkerRepository

pytestmark = pytest.mark.integration


class FakeS3:
    def __init__(self, *, fail_put=False):
        self.objects = {}
        self.fail_put = fail_put

    def put_object(self, **kwargs):
        if self.fail_put:
            raise OSError("synthetic object-store write failure")
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = bytes(kwargs["Body"])
        return {}

    def get_object(self, *, Bucket, Key):
        return {"Body": BytesIO(self.objects[(Bucket, Key)])}

    def delete_object(self, *, Bucket, Key):
        self.objects.pop((Bucket, Key), None)
        return {}


class NoFetchApi:
    calls = 0

    def __init__(self, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def fetch(self, *args, **kwargs):
        self.__class__.calls += 1
        raise AssertionError("external project resume must not refetch DBD")


def external_backend(client=None):
    key = base64.urlsafe_b64encode(bytes(range(32))).decode("ascii")
    return S3ArchiveBackend(
        bucket="integration-archive",
        prefix="dbd/integration",
        encryption_key_b64=key,
        key_id="integration-v1",
        client=client or FakeS3(),
    )


async def new_profile_task(repo, owner):
    identity = parse_code("7" + str(int(uuid4().hex[:10], 16)).zfill(13))
    run = await repo.create_run("external-" + uuid4().hex, ["profile"])
    await repo.import_batch(run, [identity], ("profile",))
    await repo.ready(run)
    task = await repo.claim(run, owner)
    timestamp = datetime.now(timezone.utc).replace(tzinfo=None)
    response = ApiResponse(
        {
            "jpNo": identity.jp_no,
            "jpTypeCode": identity.jp_type_code,
            "jpName": "External Durable Resume Co",
            "submitFinYear": None,
        },
        200,
        timestamp,
        timestamp,
    )
    return run, identity, task, response


async def test_external_archive_is_reference_only_and_resumes_without_http(repo):
    NoFetchApi.calls = 0
    external_repo = WorkerRepository(repo.engine, archive_backend=external_backend())
    run, identity, task, response = await new_profile_task(
        external_repo, "external-first-process"
    )
    archive_id = await external_repo.archive(task, response, "/synthetic/profile", None)

    async with repo.engine.connect() as conn:
        archive = (
            await conn.execute(
                text("""
                SELECT payload,payload_zstd,payload_backend,payload_object_key,payload_nonce,
                       payload_key_id,payload_cipher_sha256,payload_sha256,payload_codec
                FROM response_archive WHERE id=:id
                """),
                {"id": archive_id},
            )
        ).mappings().one()
        assert archive["payload"] == ""
        assert archive["payload_zstd"] is None
        assert archive["payload_backend"] == "s3"
        assert archive["payload_object_key"]
        assert len(archive["payload_nonce"]) == 12
        assert archive["payload_key_id"] == "integration-v1"
        assert len(archive["payload_cipher_sha256"]) == 64
        assert len(archive["payload_sha256"]) == 64
        assert archive["payload_codec"].startswith("aesgcm+")

    await external_repo.release("external-first-process")
    loaded = await external_repo.load_archive(archive_id)
    assert loaded["data"]["jpNo"] == identity.jp_no

    result = await process(external_repo, run, concurrency=2, client_factory=NoFetchApi)
    assert result == {"done": 1}
    assert NoFetchApi.calls == 0


async def test_failed_external_put_cannot_create_archive_or_project_stage(repo):
    owner = "external-put-failure"
    external_repo = WorkerRepository(
        repo.engine,
        archive_backend=external_backend(FakeS3(fail_put=True)),
    )
    _, _, task, response = await new_profile_task(external_repo, owner)

    with pytest.raises(OSError, match="synthetic object-store write failure"):
        await external_repo.archive(task, response, "/synthetic/profile", None)

    async with repo.engine.connect() as conn:
        task_row = (
            await conn.execute(
                text("SELECT stage,status,raw_response_id FROM endpoint_tasks WHERE id=:id"),
                {"id": task["id"]},
            )
        ).mappings().one()
        archive_count = (
            await conn.execute(
                text("SELECT COUNT(*) FROM response_archive WHERE task_id=:id"),
                {"id": task["id"]},
            )
        ).scalar_one()

    assert task_row == {"stage": "fetch", "status": "running", "raw_response_id": None}
    assert archive_count == 0
    await external_repo.release(owner)
