from __future__ import annotations

import json
from hashlib import sha256

from ....core.telemetry import DISCOVERY_PAGE_RESULTS, DISCOVERY_VALID_ROWS, PIPELINE_BATCH_SIZE
from ....runtime.delta import MASTER_HASH_VERSION, master_fingerprint
from ..engine import connect
from .company_repository import company_upsert_sql, company_values


def _replace_rejections(cur, bucket_id, page_no, rejections, requested):
    cur.execute(
        """
        UPDATE discovery_rejections SET resolved_at=UTC_TIMESTAMP(6)
        WHERE bucket_id=%s AND page_no=%s AND resolved_at IS NULL
        """,
        (bucket_id, page_no),
    )
    for rejection in rejections:
        raw = rejection["payload"]
        payload = json.dumps(raw, ensure_ascii=False, default=str)
        cur.execute(
            """
            INSERT INTO discovery_rejections(
              bucket_id,page_no,row_index,jp_no,jp_type_code,error_code,error_message,
              payload,payload_sha256,first_seen_at,last_seen_at,resolved_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL)
            ON DUPLICATE KEY UPDATE
              error_code=VALUES(error_code),error_message=VALUES(error_message),
              last_seen_at=VALUES(last_seen_at),resolved_at=NULL
            """,
            (
                bucket_id,
                page_no,
                rejection["row_index"],
                raw.get("jpNo") if isinstance(raw, dict) else None,
                raw.get("jpTypeCode") if isinstance(raw, dict) else None,
                rejection["error_code"],
                rejection["error_message"][:500],
                payload,
                sha256(payload.encode()).hexdigest(),
                requested,
                requested,
            ),
        )


def _aggregate_sightings(prepared: list[tuple[dict, int]]) -> list[tuple]:
    """Collapse duplicate observations inside one DB microbatch without changing audit semantics."""
    aggregate: dict[tuple[str, str], dict] = {}
    for record, _archive_id in prepared:
        requested = record["requested"]
        for profile in record["profiles"]:
            key = (profile.jp_type_code, profile.jp_no)
            item = aggregate.get(key)
            if item is None:
                aggregate[key] = {
                    "jp_no": profile.jp_no,
                    "jp_type_code": profile.jp_type_code,
                    "first_bucket_id": record["bucket_id"],
                    "times_seen": 1,
                    "first_seen_at": requested,
                    "last_seen_at": requested,
                }
            else:
                item["times_seen"] += 1
                item["first_seen_at"] = min(item["first_seen_at"], requested)
                item["last_seen_at"] = max(item["last_seen_at"], requested)
    return [
        (
            item["jp_no"],
            item["jp_type_code"],
            item["first_bucket_id"],
            item["times_seen"],
            item["first_seen_at"],
            item["last_seen_at"],
        )
        for item in aggregate.values()
    ]


def persist_page_batch(records: list[dict]) -> list[dict]:
    """Commit many Search pages while preserving one checkpoint/archive per page."""
    if not records:
        return []

    outcomes = []
    with connect() as conn:
        try:
            with conn.cursor() as cur:
                prepared = []
                for record in records:
                    archive_id = record.get("archive_id")
                    if archive_id is None:
                        payload = record.get("canonical_payload")
                        if payload is None:
                            payload = json.dumps(record["data"], ensure_ascii=False, default=str)
                        payload_sha256 = record.get("payload_sha256") or sha256(payload.encode()).hexdigest()
                        cur.execute(
                            """
                            INSERT INTO response_archive(
                              endpoint,request_path,request_context,http_status,
                              requested_at,received_at,payload,payload_sha256
                            ) VALUES (
                              'search_page','/api/v1/company-profiles/infos',%s,200,%s,
                              UTC_TIMESTAMP(6),%s,%s
                            )
                            """,
                            (
                                json.dumps(
                                    {
                                        "method": "POST",
                                        "body": record["filters"].payload(record["page_no"]),
                                        "bucket_id": record["bucket_id"],
                                        "page_no": record["page_no"],
                                        "purpose": "page",
                                    }
                                ),
                                record["requested"],
                                payload,
                                payload_sha256,
                            ),
                        )
                        archive_id = int(cur.lastrowid)
                    cur.execute(
                        """
                        INSERT INTO discovery_pages(bucket_id,page_no,archive_id)
                        VALUES (%s,%s,%s)
                        ON DUPLICATE KEY UPDATE archive_id=VALUES(archive_id)
                        """,
                        (record["bucket_id"], record["page_no"], archive_id),
                    )
                    prepared.append((record, archive_id))

                company_rows = []
                master_rows = []
                for record, archive_id in prepared:
                    requested = record["requested"]
                    for profile in record.get("projection_profiles", record["profiles"]):
                        company_rows.append(company_values(profile, None, requested, archive_id))
                        master_rows.append(
                            (
                                profile.jp_no,
                                profile.jp_type_code,
                                profile.jp_stat_code,
                                requested if profile.jp_stat_code is not None else None,
                                master_fingerprint(profile),
                                MASTER_HASH_VERSION,
                                requested,
                                archive_id,
                                requested,
                                requested,
                            )
                        )

                if company_rows:
                    insert_company = (
                        company_upsert_sql()
                        .split(" ON DUPLICATE KEY UPDATE")[0]
                        .replace("INSERT INTO", "INSERT IGNORE INTO", 1)
                    )
                    cur.executemany(insert_company, company_rows)

                if master_rows:
                    cur.executemany(
                        """
                        INSERT INTO company_master(
                          jp_no,jp_type_code,observed_jp_stat_code,observed_status_at,
                          master_hash,master_hash_version,last_master_changed_at,
                          latest_search_archive_id,first_seen_at,last_seen_at
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE
                          last_seen_at=GREATEST(last_seen_at,VALUES(last_seen_at)),
                          observed_jp_stat_code=IF(
                            VALUES(observed_jp_stat_code) IS NULL,
                            observed_jp_stat_code,VALUES(observed_jp_stat_code)
                          ),
                          observed_status_at=IF(
                            VALUES(observed_jp_stat_code) IS NULL,
                            observed_status_at,VALUES(observed_status_at)
                          ),
                          change_mask=IF(company_master.master_hash <=> VALUES(master_hash),0,127),
                          last_master_changed_at=IF(
                            company_master.master_hash <=> VALUES(master_hash),
                            company_master.last_master_changed_at,VALUES(last_master_changed_at)
                          ),
                          master_hash=VALUES(master_hash),
                          master_hash_version=VALUES(master_hash_version),
                          latest_search_archive_id=VALUES(latest_search_archive_id)
                        """,
                        master_rows,
                    )

                sighting_rows = _aggregate_sightings(prepared)
                if sighting_rows:
                    cur.executemany(
                        """
                        INSERT INTO discovery_sightings(
                          jp_no,jp_type_code,first_bucket_id,times_seen,first_seen_at,last_seen_at
                        ) VALUES (%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE
                          times_seen=times_seen+VALUES(times_seen),
                          first_seen_at=LEAST(first_seen_at,VALUES(first_seen_at)),
                          last_seen_at=GREATEST(last_seen_at,VALUES(last_seen_at))
                        """,
                        sighting_rows,
                    )

                for record, archive_id in prepared:
                    rejections = list(record.get("rejections") or [])
                    _replace_rejections(
                        cur,
                        record["bucket_id"],
                        record["page_no"],
                        rejections,
                        record["requested"],
                    )
                    status = "partial" if rejections else "done"
                    written = len(record["profiles"])
                    cur.execute(
                        """
                        UPDATE discovery_pages
                        SET status=%s,rows_written=%s,rejected_rows=%s,last_error=%s,
                            updated_at=UTC_TIMESTAMP(6)
                        WHERE bucket_id=%s AND page_no=%s
                        """,
                        (
                            status,
                            written,
                            len(rejections),
                            "One or more rows quarantined" if rejections else None,
                            record["bucket_id"],
                            record["page_no"],
                        ),
                    )
                    cur.execute(
                        "UPDATE response_archive SET processing_status=%s,error_code=%s WHERE id=%s",
                        (
                            "invalid" if rejections else "processed",
                            "partial_contract_rejection" if rejections else None,
                            archive_id,
                        ),
                    )
                    outcomes.append(
                        {
                            "page_no": record["page_no"],
                            "archive_id": archive_id,
                            "status": status,
                            "written": written,
                            "rejected": len(rejections),
                            "projected": len(record.get("projection_profiles", record["profiles"])),
                        }
                    )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise

    PIPELINE_BATCH_SIZE.labels("discovery_pages").observe(len(records))
    for outcome in outcomes:
        DISCOVERY_VALID_ROWS.inc(outcome["written"])
        DISCOVERY_PAGE_RESULTS.labels(outcome["status"]).inc()
    return outcomes
