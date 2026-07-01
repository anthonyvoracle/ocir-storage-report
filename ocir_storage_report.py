#!/usr/bin/env python3
"""
Build an OCIR storage utilization report for one OCI region.

The report walks container images visible from a compartment, fetches full image
metadata, deduplicates stored blobs by digest, and writes CSVs that reconcile
image/layer attribution back to an estimated regional billable total.

Requires:
  pip install oci

Example:
  python3 -m venv .venv
  source .venv/bin/activate
  pip install -r requirements.txt
  ./ocir_storage_report.py --region us-ashburn-1 --output-dir ./out
  ./ocir_storage_report.py --region us-ashburn-1 --compartment-id <compartment_ocid>
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import sqlite3
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, DefaultDict, Dict, Iterable, List, Optional, Sequence, Set, Tuple


IMAGE_FIELDS = [
    "region",
    "compartment_id",
    "repository_name",
    "repository_id",
    "image_id",
    "display_name",
    "digest",
    "version",
    "versions",
    "lifecycle_state",
    "layer_count",
    "layers_size_bytes",
    "manifest_size_bytes",
    "image_naive_total_bytes",
    "unique_layer_bytes_referenced",
    "shared_layer_bytes_referenced",
    "exclusive_layer_bytes",
    "exclusive_billable_bytes",
    "equal_share_attributed_billable_bytes",
    "equal_share_attributed_billable_human",
    "pull_count",
    "time_created",
    "time_last_pulled",
]


@dataclass
class LayerRecord:
    digest: str
    size_in_bytes: int
    time_created: str
    image_ids: Set[str] = field(default_factory=set)
    repositories: Set[str] = field(default_factory=set)


@dataclass
class ImageRecord:
    region: str
    id: str
    compartment_id: str
    repository_id: str
    repository_name: str
    display_name: str
    digest: str
    version: str
    versions: List[str]
    lifecycle_state: str
    layers_size_in_bytes: int
    manifest_size_in_bytes: int
    pull_count: int
    time_created: str
    time_last_pulled: str
    layer_digests: List[str] = field(default_factory=list)
    layers: List[LayerRecord] = field(default_factory=list)


@dataclass
class ManifestRecord:
    digest: str
    size_in_bytes: int
    image_ids: Set[str] = field(default_factory=set)
    repositories: Set[str] = field(default_factory=set)


def import_oci() -> Any:
    try:
        import oci  # type: ignore
    except ImportError as exc:
        raise SystemExit(
            "Missing dependency: install the OCI Python SDK with `pip install oci`."
        ) from exc
    return oci


def as_int(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def as_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


def list_to_str(values: Sequence[str]) -> str:
    return ";".join(sorted(v for v in values if v))


def fmt_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    value = float(n)
    for unit in units:
        if value < 1000.0 or unit == units[-1]:
            return f"{value:.2f} {unit}"
        value /= 1000.0
    return f"{n} B"


def allocate_bytes(total: int, refs: Sequence[str]) -> Dict[str, int]:
    if not refs:
        return {}
    sorted_refs = sorted(refs)
    base, remainder = divmod(total, len(sorted_refs))
    return {
        ref: base + (1 if index < remainder else 0)
        for index, ref in enumerate(sorted_refs)
    }


def paged_items(list_func: Any, *args: Any, **kwargs: Any) -> Iterable[Any]:
    page: Optional[str] = None
    while True:
        if page:
            kwargs["page"] = page
        response = list_func(*args, **kwargs)
        data = response.data
        items = getattr(data, "items", data)
        for item in items:
            yield item
        page = response.headers.get("opc-next-page")
        if not page:
            break


def make_artifacts_client(oci: Any, config: Dict[str, Any], region: str) -> Any:
    region_config = dict(config)
    region_config["region"] = region
    return oci.artifacts.ArtifactsClient(
        region_config, retry_strategy=oci.retry.DEFAULT_RETRY_STRATEGY
    )


_THREAD_LOCAL = threading.local()


def get_thread_client(oci: Any, config: Dict[str, Any], region: str) -> Any:
    client = getattr(_THREAD_LOCAL, "artifacts_client", None)
    if client is None:
        client = make_artifacts_client(oci, config, region)
        _THREAD_LOCAL.artifacts_client = client
    return client


def get_versions(image: Any) -> List[str]:
    versions = getattr(image, "versions", None) or []
    result = []
    for version in versions:
        name = getattr(version, "version", None)
        if name:
            result.append(str(name))
    fallback = getattr(image, "version", None)
    if fallback and str(fallback) not in result:
        result.append(str(fallback))
    return result


def init_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.create_function("fmt_bytes", 1, fmt_bytes)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS run_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS images (
            id TEXT PRIMARY KEY,
            region TEXT NOT NULL,
            compartment_id TEXT NOT NULL,
            repository_id TEXT NOT NULL,
            repository_name TEXT NOT NULL,
            display_name TEXT NOT NULL,
            digest TEXT NOT NULL,
            version TEXT NOT NULL,
            versions TEXT NOT NULL,
            lifecycle_state TEXT NOT NULL,
            layers_size_in_bytes INTEGER NOT NULL,
            manifest_size_in_bytes INTEGER NOT NULL,
            pull_count INTEGER NOT NULL,
            time_created TEXT NOT NULL,
            time_last_pulled TEXT NOT NULL,
            fetched_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS layers (
            digest TEXT PRIMARY KEY,
            size_in_bytes INTEGER NOT NULL,
            time_created TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS image_layers (
            image_id TEXT NOT NULL,
            layer_digest TEXT NOT NULL,
            position INTEGER NOT NULL,
            PRIMARY KEY (image_id, layer_digest, position)
        );

        CREATE TABLE IF NOT EXISTS manifests (
            digest TEXT PRIMARY KEY,
            size_in_bytes INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS image_manifests (
            image_id TEXT PRIMARY KEY,
            manifest_digest TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS fetch_errors (
            image_id TEXT PRIMARY KEY,
            repository_name TEXT NOT NULL,
            display_name TEXT NOT NULL,
            error TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_images_repository_id ON images(repository_id);
        CREATE INDEX IF NOT EXISTS idx_images_digest ON images(digest);
        CREATE INDEX IF NOT EXISTS idx_image_layers_layer_digest ON image_layers(layer_digest);
        CREATE INDEX IF NOT EXISTS idx_image_manifests_digest ON image_manifests(manifest_digest);
        """
    )
    return conn


def scope_values(
    region: str,
    compartment_id: str,
    include_subtree: bool,
    lifecycle_state: str,
    repository_name: Optional[str],
    repository_id: Optional[str],
) -> Dict[str, str]:
    return {
        "region": region,
        "compartment_id": compartment_id,
        "include_subtree": str(bool(include_subtree)),
        "lifecycle_state": lifecycle_state or "",
        "repository_name": repository_name or "",
        "repository_id": repository_id or "",
    }


def ensure_scope(conn: sqlite3.Connection, values: Dict[str, str]) -> None:
    existing = {
        row["key"]: row["value"]
        for row in conn.execute("SELECT key, value FROM run_meta WHERE key LIKE 'scope_%'")
    }
    expected = {f"scope_{key}": value for key, value in values.items()}
    mismatches = [
        (key, existing.get(key), value)
        for key, value in expected.items()
        if key in existing and existing[key] != value
    ]
    if mismatches:
        details = ", ".join(
            f"{key}: existing={old!r}, requested={new!r}"
            for key, old, new in mismatches
        )
        raise SystemExit(
            "The existing state database was created for a different scan scope. "
            f"{details}. Use a new --state-db or pass --reset-state."
        )
    conn.executemany(
        "INSERT OR REPLACE INTO run_meta(key, value) VALUES (?, ?)",
        sorted(expected.items()),
    )
    conn.execute(
        "INSERT OR REPLACE INTO run_meta(key, value) VALUES (?, ?)",
        ("schema_version", "2"),
    )
    conn.commit()


def already_fetched(conn: sqlite3.Connection, image_id: str) -> bool:
    row = conn.execute("SELECT 1 FROM images WHERE id = ?", (image_id,)).fetchone()
    return row is not None


def summary_payload(summary: Any) -> Dict[str, str]:
    return {
        "id": as_str(getattr(summary, "id", "")),
        "repository_name": as_str(getattr(summary, "repository_name", "")),
        "display_name": as_str(getattr(summary, "display_name", "")),
    }


def fetch_image_payload(
    oci: Any,
    config: Dict[str, Any],
    region: str,
    image_id: str,
) -> Dict[str, Any]:
    client = get_thread_client(oci, config, region)
    image = client.get_container_image(image_id).data
    layers = []
    for index, layer in enumerate(getattr(image, "layers", None) or []):
        digest = as_str(getattr(layer, "digest", ""))
        if not digest:
            continue
        layers.append(
            {
                "digest": digest,
                "size_in_bytes": as_int(getattr(layer, "size_in_bytes", 0)),
                "time_created": as_str(getattr(layer, "time_created", "")),
                "position": index,
            }
        )

    return {
        "region": region,
        "id": as_str(image.id),
        "compartment_id": as_str(image.compartment_id),
        "repository_id": as_str(image.repository_id),
        "repository_name": as_str(image.repository_name),
        "display_name": as_str(image.display_name),
        "digest": as_str(image.digest),
        "version": as_str(getattr(image, "version", "")),
        "versions": list_to_str(get_versions(image)),
        "lifecycle_state": as_str(image.lifecycle_state),
        "layers_size_in_bytes": as_int(image.layers_size_in_bytes),
        "manifest_size_in_bytes": as_int(image.manifest_size_in_bytes),
        "pull_count": as_int(image.pull_count),
        "time_created": as_str(image.time_created),
        "time_last_pulled": as_str(getattr(image, "time_last_pulled", "")),
        "layers": layers,
    }


def persist_image(conn: sqlite3.Connection, payload: Dict[str, Any]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT OR REPLACE INTO images (
            id, region, compartment_id, repository_id, repository_name, display_name,
            digest, version, versions, lifecycle_state, layers_size_in_bytes,
            manifest_size_in_bytes, pull_count, time_created, time_last_pulled,
            fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload["id"],
            payload["region"],
            payload["compartment_id"],
            payload["repository_id"],
            payload["repository_name"],
            payload["display_name"],
            payload["digest"],
            payload["version"],
            payload["versions"],
            payload["lifecycle_state"],
            payload["layers_size_in_bytes"],
            payload["manifest_size_in_bytes"],
            payload["pull_count"],
            payload["time_created"],
            payload["time_last_pulled"],
            now,
        ),
    )
    conn.execute("DELETE FROM image_layers WHERE image_id = ?", (payload["id"],))
    for layer in payload["layers"]:
        conn.execute(
            """
            INSERT INTO layers(digest, size_in_bytes, time_created)
            VALUES (?, ?, ?)
            ON CONFLICT(digest) DO UPDATE SET
                size_in_bytes = max(layers.size_in_bytes, excluded.size_in_bytes),
                time_created = CASE
                    WHEN layers.time_created = '' THEN excluded.time_created
                    ELSE layers.time_created
                END
            """,
            (layer["digest"], layer["size_in_bytes"], layer["time_created"]),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO image_layers(image_id, layer_digest, position)
            VALUES (?, ?, ?)
            """,
            (payload["id"], layer["digest"], layer["position"]),
        )

    manifest_digest = payload["digest"] or payload["id"]
    conn.execute(
        """
        INSERT INTO manifests(digest, size_in_bytes)
        VALUES (?, ?)
        ON CONFLICT(digest) DO UPDATE SET
            size_in_bytes = max(manifests.size_in_bytes, excluded.size_in_bytes)
        """,
        (manifest_digest, payload["manifest_size_in_bytes"]),
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO image_manifests(image_id, manifest_digest)
        VALUES (?, ?)
        """,
        (payload["id"], manifest_digest),
    )
    conn.execute("DELETE FROM fetch_errors WHERE image_id = ?", (payload["id"],))


def persist_error(
    conn: sqlite3.Connection,
    summary: Dict[str, str],
    error: BaseException,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO fetch_errors(
            image_id, repository_name, display_name, error, updated_at
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            summary["id"],
            summary["repository_name"],
            summary["display_name"],
            repr(error),
            datetime.now(timezone.utc).isoformat(),
        ),
    )


def list_image_summaries(
    client: Any,
    compartment_id: str,
    include_subtree: bool,
    lifecycle_state: str,
    limit: int,
    repository_name: Optional[str],
    repository_id: Optional[str],
) -> Iterable[Any]:
    list_kwargs: Dict[str, Any] = {
        "compartment_id_in_subtree": include_subtree,
        "limit": limit,
    }
    if lifecycle_state:
        list_kwargs["lifecycle_state"] = lifecycle_state
    if repository_name:
        list_kwargs["repository_name"] = repository_name
    if repository_id:
        list_kwargs["repository_id"] = repository_id
    yield from paged_items(client.list_container_images, compartment_id, **list_kwargs)


def prune_unlisted_images(conn: sqlite3.Connection, listed_image_ids: Set[str]) -> int:
    conn.execute("DROP TABLE IF EXISTS current_scan_images")
    conn.execute("CREATE TEMP TABLE current_scan_images(id TEXT PRIMARY KEY)")
    conn.executemany(
        "INSERT INTO current_scan_images(id) VALUES (?)",
        ((image_id,) for image_id in sorted(listed_image_ids)),
    )
    pruned = conn.execute(
        """
        SELECT COUNT(*) AS count
        FROM images
        WHERE id NOT IN (SELECT id FROM current_scan_images)
        """
    ).fetchone()["count"]
    conn.execute(
        """
        DELETE FROM image_layers
        WHERE image_id NOT IN (SELECT id FROM current_scan_images)
        """
    )
    conn.execute(
        """
        DELETE FROM image_manifests
        WHERE image_id NOT IN (SELECT id FROM current_scan_images)
        """
    )
    conn.execute(
        """
        DELETE FROM fetch_errors
        WHERE image_id NOT IN (SELECT id FROM current_scan_images)
        """
    )
    conn.execute(
        """
        DELETE FROM images
        WHERE id NOT IN (SELECT id FROM current_scan_images)
        """
    )
    conn.execute("DROP TABLE current_scan_images")
    return int(pruned)


def collect_images_to_db(
    conn: sqlite3.Connection,
    oci: Any,
    config: Dict[str, Any],
    client: Any,
    region: str,
    compartment_id: str,
    include_subtree: bool,
    lifecycle_state: str,
    page_size: int,
    repository_name: Optional[str],
    repository_id: Optional[str],
    workers: int,
    max_pending: int,
    commit_interval: int,
    resume: bool,
    refresh: bool,
    fail_fast: bool,
) -> Dict[str, int]:
    pending: Dict[Future[Dict[str, Any]], Dict[str, str]] = {}
    stats = {
        "listed": 0,
        "submitted": 0,
        "skipped": 0,
        "fetched": 0,
        "failed": 0,
        "pruned": 0,
    }
    listed_image_ids: Set[str] = set()
    started = time.monotonic()

    def drain(completed: Iterable[Future[Dict[str, Any]]]) -> None:
        for future in completed:
            summary = pending.pop(future)
            try:
                payload = future.result()
                persist_image(conn, payload)
                stats["fetched"] += 1
            except Exception as exc:
                stats["failed"] += 1
                persist_error(conn, summary, exc)
                print(
                    f"Failed to fetch {summary['display_name'] or summary['id']}: {exc}",
                    file=sys.stderr,
                )
                if fail_fast:
                    raise
            processed = stats["fetched"] + stats["failed"]
            if processed and processed % commit_interval == 0:
                conn.commit()
                elapsed = max(time.monotonic() - started, 1.0)
                print(
                    "Progress: "
                    f"listed={stats['listed']} submitted={stats['submitted']} "
                    f"skipped={stats['skipped']} fetched={stats['fetched']} "
                    f"failed={stats['failed']} rate={stats['fetched'] / elapsed:.1f}/s",
                    file=sys.stderr,
                )

    with ThreadPoolExecutor(max_workers=workers) as executor:
        for summary in list_image_summaries(
            client=client,
            compartment_id=compartment_id,
            include_subtree=include_subtree,
            lifecycle_state=lifecycle_state,
            limit=page_size,
            repository_name=repository_name,
            repository_id=repository_id,
        ):
            stats["listed"] += 1
            image_id = as_str(getattr(summary, "id", ""))
            if not image_id:
                continue
            listed_image_ids.add(image_id)
            if resume and not refresh and already_fetched(conn, image_id):
                stats["skipped"] += 1
                continue

            future = executor.submit(fetch_image_payload, oci, config, region, image_id)
            pending[future] = summary_payload(summary)
            stats["submitted"] += 1

            if len(pending) >= max_pending:
                done, _ = wait(pending.keys(), return_when=FIRST_COMPLETED)
                drain(done)

        while pending:
            done, _ = wait(pending.keys(), return_when=FIRST_COMPLETED)
            drain(done)

    stats["pruned"] = prune_unlisted_images(conn, listed_image_ids)
    conn.commit()
    return stats


def collect_images(
    client: Any,
    region: str,
    compartment_id: str,
    include_subtree: bool,
    lifecycle_state: str,
    limit: int,
    repository_name: Optional[str],
) -> List[ImageRecord]:
    list_kwargs: Dict[str, Any] = {
        "compartment_id_in_subtree": include_subtree,
        "limit": limit,
    }
    if lifecycle_state:
        list_kwargs["lifecycle_state"] = lifecycle_state
    if repository_name:
        list_kwargs["repository_name"] = repository_name

    summaries = list(
        paged_items(
            client.list_container_images,
            compartment_id,
            **list_kwargs,
        )
    )

    records: List[ImageRecord] = []
    for index, summary in enumerate(summaries, start=1):
        print(
            f"Fetching image metadata {index}/{len(summaries)}: "
            f"{getattr(summary, 'display_name', getattr(summary, 'id', 'unknown'))}",
            file=sys.stderr,
        )
        image = client.get_container_image(summary.id).data
        layers = getattr(image, "layers", None) or []
        layer_records = [
            LayerRecord(
                digest=as_str(getattr(layer, "digest", "")),
                size_in_bytes=as_int(getattr(layer, "size_in_bytes", 0)),
                time_created=as_str(getattr(layer, "time_created", "")),
            )
            for layer in layers
        ]
        layer_digests = [layer.digest for layer in layer_records if layer.digest]
        records.append(
            ImageRecord(
                region=region,
                id=as_str(image.id),
                compartment_id=as_str(image.compartment_id),
                repository_id=as_str(image.repository_id),
                repository_name=as_str(image.repository_name),
                display_name=as_str(image.display_name),
                digest=as_str(image.digest),
                version=as_str(getattr(image, "version", "")),
                versions=get_versions(image),
                lifecycle_state=as_str(image.lifecycle_state),
                layers_size_in_bytes=as_int(image.layers_size_in_bytes),
                manifest_size_in_bytes=as_int(image.manifest_size_in_bytes),
                pull_count=as_int(image.pull_count),
                time_created=as_str(image.time_created),
                time_last_pulled=as_str(getattr(image, "time_last_pulled", "")),
                layer_digests=layer_digests,
                layers=layer_records,
            )
        )
    return records


def build_layer_index(images: Sequence[ImageRecord]) -> Dict[str, LayerRecord]:
    layers_by_digest: Dict[str, LayerRecord] = {}
    image_by_id = {image.id: image for image in images}

    for image in images:
        for layer in image.layers:
            digest = layer.digest
            size = layer.size_in_bytes
            if not digest:
                continue
            info = layers_by_digest.setdefault(
                digest,
                LayerRecord(
                    digest=digest,
                    size_in_bytes=size,
                    time_created=layer.time_created,
                ),
            )
            info.size_in_bytes = max(info.size_in_bytes, size)
            info.image_ids.add(image.id)
            info.repositories.add(image.repository_name)

    known_refs: DefaultDict[str, Set[str]] = defaultdict(set)
    for layer in layers_by_digest.values():
        for image_id in layer.image_ids:
            known_refs[image_id].add(layer.digest)

    missing = [
        (image_by_id[image_id].display_name, digest)
        for image_id, image in image_by_id.items()
        for digest in image.layer_digests
        if digest not in known_refs[image_id]
    ]
    if missing:
        print(
            f"Warning: {len(missing)} layer references were present in summaries "
            "but not in fetched layer metadata.",
            file=sys.stderr,
        )
    return layers_by_digest


def build_manifest_index(images: Sequence[ImageRecord]) -> Dict[str, ManifestRecord]:
    manifests: Dict[str, ManifestRecord] = {}
    for image in images:
        digest = image.digest or image.id
        info = manifests.setdefault(
            digest,
            ManifestRecord(digest=digest, size_in_bytes=image.manifest_size_in_bytes),
        )
        info.size_in_bytes = max(info.size_in_bytes, image.manifest_size_in_bytes)
        info.image_ids.add(image.id)
        info.repositories.add(image.repository_name)
    return manifests


def write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[Dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def image_rows(
    images: Sequence[ImageRecord],
    layers: Dict[str, LayerRecord],
    manifests: Dict[str, ManifestRecord],
    layer_allocations: Dict[str, Dict[str, int]],
    manifest_allocations: Dict[str, Dict[str, int]],
) -> Iterable[Dict[str, Any]]:
    for image in sorted(images, key=lambda item: (item.repository_name, item.display_name)):
        unique_layer_bytes = 0
        shared_layer_bytes = 0
        attributed_layer_bytes = 0
        exclusive_layer_bytes = 0

        for digest in image.layer_digests:
            layer = layers.get(digest)
            if not layer:
                continue
            refs = max(len(layer.image_ids), 1)
            unique_layer_bytes += layer.size_in_bytes
            attributed_layer_bytes += layer_allocations.get(digest, {}).get(image.id, 0)
            if refs == 1:
                exclusive_layer_bytes += layer.size_in_bytes
            else:
                shared_layer_bytes += layer.size_in_bytes

        manifest = manifests.get(image.digest or image.id)
        manifest_refs = max(len(manifest.image_ids), 1) if manifest else 1
        attributed_manifest_bytes = manifest_allocations.get(
            image.digest or image.id, {}
        ).get(image.id, 0)
        exclusive_manifest_bytes = image.manifest_size_in_bytes if manifest_refs == 1 else 0
        attributed_total = attributed_layer_bytes + attributed_manifest_bytes
        exclusive_total = exclusive_layer_bytes + exclusive_manifest_bytes

        yield {
            "region": image.region,
            "compartment_id": image.compartment_id,
            "repository_name": image.repository_name,
            "repository_id": image.repository_id,
            "image_id": image.id,
            "display_name": image.display_name,
            "digest": image.digest,
            "version": image.version,
            "versions": list_to_str(image.versions),
            "lifecycle_state": image.lifecycle_state,
            "layer_count": len(image.layer_digests),
            "layers_size_bytes": image.layers_size_in_bytes,
            "manifest_size_bytes": image.manifest_size_in_bytes,
            "image_naive_total_bytes": image.layers_size_in_bytes
            + image.manifest_size_in_bytes,
            "unique_layer_bytes_referenced": unique_layer_bytes,
            "shared_layer_bytes_referenced": shared_layer_bytes,
            "exclusive_layer_bytes": exclusive_layer_bytes,
            "exclusive_billable_bytes": exclusive_total,
            "equal_share_attributed_billable_bytes": attributed_total,
            "equal_share_attributed_billable_human": fmt_bytes(attributed_total),
            "pull_count": image.pull_count,
            "time_created": image.time_created,
            "time_last_pulled": image.time_last_pulled,
        }


def layer_rows(
    layers: Dict[str, LayerRecord],
    image_lookup: Dict[str, ImageRecord],
) -> Iterable[Dict[str, Any]]:
    for layer in sorted(layers.values(), key=lambda item: item.size_in_bytes, reverse=True):
        image_names = [
            image_lookup[image_id].display_name
            for image_id in sorted(layer.image_ids)
            if image_id in image_lookup
        ]
        yield {
            "layer_digest": layer.digest,
            "size_bytes": layer.size_in_bytes,
            "size_human": fmt_bytes(layer.size_in_bytes),
            "image_ref_count": len(layer.image_ids),
            "repository_ref_count": len(layer.repositories),
            "equal_share_bytes_per_image_ref": layer.size_in_bytes
            // max(len(layer.image_ids), 1),
            "repositories": list_to_str(list(layer.repositories)),
            "images": list_to_str(image_names),
            "time_created": layer.time_created,
        }


def layer_ref_rows(
    layers: Dict[str, LayerRecord],
    image_lookup: Dict[str, ImageRecord],
    layer_allocations: Dict[str, Dict[str, int]],
) -> Iterable[Dict[str, Any]]:
    for layer in sorted(layers.values(), key=lambda item: item.digest):
        for image_id in sorted(layer.image_ids):
            image = image_lookup[image_id]
            yield {
                "region": image.region,
                "repository_name": image.repository_name,
                "image_id": image.id,
                "display_name": image.display_name,
                "image_digest": image.digest,
                "layer_digest": layer.digest,
                "layer_size_bytes": layer.size_in_bytes,
                "layer_ref_count": len(layer.image_ids),
                "equal_share_attributed_bytes": layer_allocations.get(
                    layer.digest, {}
                ).get(image_id, 0),
            }


def repository_rows(image_rows_data: Sequence[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    by_repo: DefaultDict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "image_count": 0,
            "naive_total_bytes": 0,
            "exclusive_billable_bytes": 0,
            "equal_share_attributed_billable_bytes": 0,
        }
    )
    for row in image_rows_data:
        key = row["repository_id"] or row["repository_name"]
        item = by_repo[key]
        item["region"] = row["region"]
        item["repository_name"] = row["repository_name"]
        item["repository_id"] = row["repository_id"]
        item["compartment_id"] = row["compartment_id"]
        item["image_count"] += 1
        item["naive_total_bytes"] += int(row["image_naive_total_bytes"])
        item["exclusive_billable_bytes"] += int(row["exclusive_billable_bytes"])
        item["equal_share_attributed_billable_bytes"] += int(
            row["equal_share_attributed_billable_bytes"]
        )

    for item in sorted(
        by_repo.values(),
        key=lambda row: row["equal_share_attributed_billable_bytes"],
        reverse=True,
    ):
        item["naive_total_human"] = fmt_bytes(item["naive_total_bytes"])
        item["exclusive_billable_human"] = fmt_bytes(item["exclusive_billable_bytes"])
        item["equal_share_attributed_billable_human"] = fmt_bytes(
            item["equal_share_attributed_billable_bytes"]
        )
        yield item


def write_report(
    output_dir: Path,
    region: str,
    compartment_id: str,
    include_subtree: bool,
    images: Sequence[ImageRecord],
    layers: Dict[str, LayerRecord],
    manifests: Dict[str, ManifestRecord],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    image_lookup = {image.id: image for image in images}
    layer_allocations = {
        digest: allocate_bytes(layer.size_in_bytes, list(layer.image_ids))
        for digest, layer in layers.items()
    }
    manifest_allocations = {
        digest: allocate_bytes(manifest.size_in_bytes, list(manifest.image_ids))
        for digest, manifest in manifests.items()
    }
    image_rows_data = list(
        image_rows(images, layers, manifests, layer_allocations, manifest_allocations)
    )

    unique_layer_total = sum(layer.size_in_bytes for layer in layers.values())
    unique_manifest_total = sum(manifest.size_in_bytes for manifest in manifests.values())
    estimated_billable_total = unique_layer_total + unique_manifest_total
    naive_total = sum(
        image.layers_size_in_bytes + image.manifest_size_in_bytes for image in images
    )

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "region": region,
        "scope_compartment_id": compartment_id,
        "scope_includes_subtree": include_subtree,
        "image_count": len(images),
        "repository_count": len({image.repository_id for image in images}),
        "unique_layer_count": len(layers),
        "unique_manifest_count": len(manifests),
        "naive_image_total_bytes": naive_total,
        "naive_image_total_human": fmt_bytes(naive_total),
        "unique_layer_total_bytes": unique_layer_total,
        "unique_layer_total_human": fmt_bytes(unique_layer_total),
        "unique_manifest_total_bytes": unique_manifest_total,
        "unique_manifest_total_human": fmt_bytes(unique_manifest_total),
        "estimated_billable_total_bytes": estimated_billable_total,
        "estimated_billable_total_human": fmt_bytes(estimated_billable_total),
        "dedup_savings_bytes": naive_total - estimated_billable_total,
        "dedup_savings_human": fmt_bytes(naive_total - estimated_billable_total),
        "note": (
            "Estimated billable total is unique layer blobs plus unique image "
            "manifest bytes from ContainerImage metadata. OCI billing exports "
            "remain the source of truth for invoiced charges."
        ),
    }

    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8"
    )

    write_csv(output_dir / "images.csv", IMAGE_FIELDS, image_rows_data)
    write_csv(
        output_dir / "layers.csv",
        [
            "layer_digest",
            "size_bytes",
            "size_human",
            "image_ref_count",
            "repository_ref_count",
            "equal_share_bytes_per_image_ref",
            "repositories",
            "images",
            "time_created",
        ],
        layer_rows(layers, image_lookup),
    )
    write_csv(
        output_dir / "layer_refs.csv",
        [
            "region",
            "repository_name",
            "image_id",
            "display_name",
            "image_digest",
            "layer_digest",
            "layer_size_bytes",
            "layer_ref_count",
            "equal_share_attributed_bytes",
        ],
        layer_ref_rows(layers, image_lookup, layer_allocations),
    )
    write_csv(
        output_dir / "repositories.csv",
        [
            "region",
            "compartment_id",
            "repository_name",
            "repository_id",
            "image_count",
            "naive_total_bytes",
            "naive_total_human",
            "exclusive_billable_bytes",
            "exclusive_billable_human",
            "equal_share_attributed_billable_bytes",
            "equal_share_attributed_billable_human",
        ],
        repository_rows(image_rows_data),
    )

    print(json.dumps(summary, indent=2, sort_keys=True))
    print(f"\nWrote report files to {output_dir}", file=sys.stderr)


def build_attribution_table(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DROP TABLE IF EXISTS image_attribution;

        CREATE TEMP TABLE image_attribution AS
        WITH
        layer_ref_base AS (
            SELECT DISTINCT image_id, layer_digest
            FROM image_layers
        ),
        layer_refs AS (
            SELECT
                b.image_id,
                b.layer_digest,
                l.size_in_bytes,
                COUNT(*) OVER (PARTITION BY b.layer_digest) AS ref_count,
                ROW_NUMBER() OVER (
                    PARTITION BY b.layer_digest ORDER BY b.image_id
                ) AS ref_rank
            FROM layer_ref_base b
            JOIN layers l ON l.digest = b.layer_digest
        ),
        layer_attribution AS (
            SELECT
                image_id,
                SUM(size_in_bytes) AS unique_layer_bytes_referenced,
                SUM(CASE WHEN ref_count > 1 THEN size_in_bytes ELSE 0 END)
                    AS shared_layer_bytes_referenced,
                SUM(CASE WHEN ref_count = 1 THEN size_in_bytes ELSE 0 END)
                    AS exclusive_layer_bytes,
                SUM(
                    CAST(size_in_bytes / ref_count AS INTEGER)
                    + CASE WHEN ref_rank <= (size_in_bytes % ref_count) THEN 1 ELSE 0 END
                ) AS attributed_layer_bytes
            FROM layer_refs
            GROUP BY image_id
        ),
        manifest_refs AS (
            SELECT
                im.image_id,
                im.manifest_digest,
                m.size_in_bytes,
                COUNT(*) OVER (PARTITION BY im.manifest_digest) AS ref_count,
                ROW_NUMBER() OVER (
                    PARTITION BY im.manifest_digest ORDER BY im.image_id
                ) AS ref_rank
            FROM image_manifests im
            JOIN manifests m ON m.digest = im.manifest_digest
        ),
        manifest_attribution AS (
            SELECT
                image_id,
                SUM(CASE WHEN ref_count = 1 THEN size_in_bytes ELSE 0 END)
                    AS exclusive_manifest_bytes,
                SUM(
                    CAST(size_in_bytes / ref_count AS INTEGER)
                    + CASE WHEN ref_rank <= (size_in_bytes % ref_count) THEN 1 ELSE 0 END
                ) AS attributed_manifest_bytes
            FROM manifest_refs
            GROUP BY image_id
        )
        SELECT
            i.id AS image_id,
            COALESCE(la.unique_layer_bytes_referenced, 0)
                AS unique_layer_bytes_referenced,
            COALESCE(la.shared_layer_bytes_referenced, 0)
                AS shared_layer_bytes_referenced,
            COALESCE(la.exclusive_layer_bytes, 0) AS exclusive_layer_bytes,
            COALESCE(la.attributed_layer_bytes, 0) AS attributed_layer_bytes,
            COALESCE(ma.exclusive_manifest_bytes, 0) AS exclusive_manifest_bytes,
            COALESCE(ma.attributed_manifest_bytes, 0) AS attributed_manifest_bytes,
            COALESCE(la.exclusive_layer_bytes, 0)
                + COALESCE(ma.exclusive_manifest_bytes, 0)
                AS exclusive_billable_bytes,
            COALESCE(la.attributed_layer_bytes, 0)
                + COALESCE(ma.attributed_manifest_bytes, 0)
                AS equal_share_attributed_billable_bytes
        FROM images i
        LEFT JOIN layer_attribution la ON la.image_id = i.id
        LEFT JOIN manifest_attribution ma ON ma.image_id = i.id;

        CREATE INDEX idx_image_attribution_image_id
            ON image_attribution(image_id);
        """
    )


def write_query_csv(
    conn: sqlite3.Connection,
    path: Path,
    fieldnames: Sequence[str],
    query: str,
    params: Sequence[Any] = (),
) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in conn.execute(query, params):
            writer.writerow({field: row[field] for field in fieldnames})


def pct(part: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return part * 100.0 / total


def query_rows(
    conn: sqlite3.Connection,
    query: str,
    params: Sequence[Any] = (),
) -> List[Dict[str, Any]]:
    return [dict(row) for row in conn.execute(query, params)]


def confidence_status_class(status: str) -> str:
    lowered = status.lower()
    if lowered in {"pass", "warning", "fail"}:
        return lowered
    return "warning"


def write_storage_visuals(
    conn: sqlite3.Connection,
    output_dir: Path,
    summary: Dict[str, Any],
) -> None:
    billable_total = int(summary["estimated_billable_total_bytes"])

    object_rows = query_rows(
        conn,
        """
        WITH layer_ref_counts AS (
            SELECT
                l.digest,
                l.size_in_bytes,
                COUNT(DISTINCT il.image_id) AS ref_count
            FROM layers l
            LEFT JOIN image_layers il ON il.layer_digest = l.digest
            GROUP BY l.digest
        ),
        manifest_ref_counts AS (
            SELECT
                m.digest,
                m.size_in_bytes,
                COUNT(DISTINCT im.image_id) AS ref_count
            FROM manifests m
            LEFT JOIN image_manifests im ON im.manifest_digest = m.digest
            GROUP BY m.digest
        )
        SELECT
            'physical_objects' AS view,
            'Referenced layer blobs' AS category,
            COUNT(*) AS item_count,
            COALESCE(SUM(size_in_bytes), 0) AS size_bytes,
            'Layer rows referenced by one or more scanned image manifests.' AS note
        FROM layer_ref_counts
        WHERE ref_count > 0
        UNION ALL
        SELECT
            'physical_objects',
            'Local unreferenced layer rows',
            COUNT(*),
            COALESCE(SUM(size_in_bytes), 0),
            'Rows in the local state DB with no scanned image manifest reference.'
        FROM layer_ref_counts
        WHERE ref_count = 0
        UNION ALL
        SELECT
            'physical_objects',
            'Referenced image manifests',
            COUNT(*),
            COALESCE(SUM(size_in_bytes), 0),
            'Manifest rows referenced by one or more scanned container images.'
        FROM manifest_ref_counts
        WHERE ref_count > 0
        UNION ALL
        SELECT
            'physical_objects',
            'Local unreferenced manifest rows',
            COUNT(*),
            COALESCE(SUM(size_in_bytes), 0),
            'Manifest rows in the local state DB with no scanned image reference.'
        FROM manifest_ref_counts
        WHERE ref_count = 0
        ORDER BY size_bytes DESC, category
        """,
    )

    tag_rows = query_rows(
        conn,
        """
        SELECT
            'tag_status' AS view,
            CASE
                WHEN TRIM(COALESCE(i.version, '')) <> ''
                  OR TRIM(COALESCE(i.versions, '')) <> ''
                THEN 'Versioned images'
                ELSE 'Unversioned images'
            END AS category,
            COUNT(*) AS item_count,
            COALESCE(SUM(a.equal_share_attributed_billable_bytes), 0) AS size_bytes,
            'Equal-share attributed bytes. Buckets reconcile to the billable estimate.'
                AS note
        FROM images i
        JOIN image_attribution a ON a.image_id = i.id
        GROUP BY category
        ORDER BY size_bytes DESC, category
        """,
    )

    reuse_rows = query_rows(
        conn,
        """
        WITH layer_ref_counts AS (
            SELECT
                l.digest,
                l.size_in_bytes,
                COUNT(DISTINCT il.image_id) AS ref_count
            FROM layers l
            LEFT JOIN image_layers il ON il.layer_digest = l.digest
            GROUP BY l.digest
        )
        SELECT
            'layer_reuse' AS view,
            CASE
                WHEN ref_count = 0 THEN 'Local unreferenced layer rows'
                WHEN ref_count = 1 THEN 'Exclusive layers'
                ELSE 'Shared layers'
            END AS category,
            COUNT(*) AS item_count,
            COALESCE(SUM(size_in_bytes), 0) AS size_bytes,
            'Unique physical layer bytes grouped by manifest reference count.' AS note
        FROM layer_ref_counts
        GROUP BY category
        ORDER BY size_bytes DESC, category
        """,
    )

    top_repo_rows = query_rows(
        conn,
        """
        SELECT
            min(i.repository_name) AS repository_name,
            COUNT(*) AS image_count,
            COALESCE(SUM(a.equal_share_attributed_billable_bytes), 0) AS size_bytes
        FROM images i
        JOIN image_attribution a ON a.image_id = i.id
        GROUP BY i.repository_id
        ORDER BY size_bytes DESC, repository_name
        LIMIT 12
        """,
    )
    retention_policy = summary.get("retention_policy", {})
    retention_created_days = int(retention_policy.get("created_days", 90))
    retention_last_pulled_days = int(retention_policy.get("last_pulled_days", 90))
    retention_repo_version_limit = int(
        retention_policy.get("repository_image_limit", 10)
    )
    retention_exclusive_bytes = int(
        retention_policy.get("exclusive_bytes", 1000 * 1000 * 1000)
    )
    retention_criteria_rows = query_rows(
        conn,
        f"""
        WITH repo_counts AS (
            SELECT repository_id, COUNT(*) AS repository_image_count
            FROM images
            GROUP BY repository_id
        ),
        base AS (
            SELECT
                i.id,
                i.version,
                i.versions,
                i.pull_count,
                i.time_created,
                i.time_last_pulled,
                rc.repository_image_count,
                a.exclusive_billable_bytes,
                a.equal_share_attributed_billable_bytes
            FROM images i
            JOIN image_attribution a ON a.image_id = i.id
            JOIN repo_counts rc ON rc.repository_id = i.repository_id
        ),
        criteria AS (
            SELECT
                'Unversioned images' AS criterion,
                id,
                exclusive_billable_bytes,
                equal_share_attributed_billable_bytes,
                'Both version fields are empty.' AS note
            FROM base
            WHERE TRIM(COALESCE(version, '')) = ''
              AND TRIM(COALESCE(versions, '')) = ''
            UNION ALL
            SELECT
                'Never pulled',
                id,
                exclusive_billable_bytes,
                equal_share_attributed_billable_bytes,
                'Pull count is zero.'
            FROM base
            WHERE pull_count = 0
            UNION ALL
            SELECT
                'No last-pulled timestamp',
                id,
                exclusive_billable_bytes,
                equal_share_attributed_billable_bytes,
                'No time_last_pulled value was returned.'
            FROM base
            WHERE TRIM(COALESCE(time_last_pulled, '')) = ''
            UNION ALL
            SELECT
                'Not pulled in {retention_last_pulled_days}+ days',
                id,
                exclusive_billable_bytes,
                equal_share_attributed_billable_bytes,
                'Last pull is older than the configured retention window.'
            FROM base
            WHERE TRIM(COALESCE(time_last_pulled, '')) <> ''
              AND CAST(julianday('now') - julianday(time_last_pulled) AS INTEGER) >= ?
            UNION ALL
            SELECT
                'Created {retention_created_days}+ days ago',
                id,
                exclusive_billable_bytes,
                equal_share_attributed_billable_bytes,
                'Image creation time is older than the configured retention window.'
            FROM base
            WHERE TRIM(COALESCE(time_created, '')) <> ''
              AND CAST(julianday('now') - julianday(time_created) AS INTEGER) >= ?
            UNION ALL
            SELECT
                'High exclusive storage',
                id,
                exclusive_billable_bytes,
                equal_share_attributed_billable_bytes,
                'Exclusive billable bytes meet or exceed {fmt_bytes(retention_exclusive_bytes)}.'
            FROM base
            WHERE exclusive_billable_bytes >= ?
            UNION ALL
            SELECT
                'Repository over {retention_repo_version_limit} images',
                id,
                exclusive_billable_bytes,
                equal_share_attributed_billable_bytes,
                'Repository scanned image count exceeds the configured limit.'
            FROM base
            WHERE repository_image_count > ?
        )
        SELECT
            criterion,
            COUNT(DISTINCT id) AS image_count,
            COALESCE(SUM(exclusive_billable_bytes), 0) AS exclusive_bytes,
            COALESCE(SUM(equal_share_attributed_billable_bytes), 0)
                AS attributed_bytes,
            note
        FROM criteria
        GROUP BY criterion, note
        ORDER BY attributed_bytes DESC, image_count DESC, criterion
        """,
        (
            retention_last_pulled_days,
            retention_created_days,
            retention_exclusive_bytes,
            retention_repo_version_limit,
        ),
    )
    top_deletion_candidate_rows = query_rows(
        conn,
        """
        WITH repo_counts AS (
            SELECT repository_id, COUNT(*) AS repository_image_count
            FROM images
            GROUP BY repository_id
        ),
        deletion_base AS (
            SELECT
                i.repository_name,
                i.display_name,
                i.version,
                i.versions,
                i.id AS image_id,
                i.pull_count,
                i.time_created,
                i.time_last_pulled,
                rc.repository_image_count,
                a.exclusive_billable_bytes,
                a.equal_share_attributed_billable_bytes,
                CASE
                    WHEN TRIM(COALESCE(i.time_created, '')) <> ''
                    THEN CAST(julianday('now') - julianday(i.time_created) AS INTEGER)
                    ELSE NULL
                END AS age_days,
                CASE
                    WHEN TRIM(COALESCE(i.time_last_pulled, '')) <> ''
                    THEN CAST(julianday('now') - julianday(i.time_last_pulled) AS INTEGER)
                    ELSE NULL
                END AS last_pulled_age_days,
                TRIM(
                    CASE
                        WHEN TRIM(COALESCE(i.version, '')) = ''
                          AND TRIM(COALESCE(i.versions, '')) = ''
                        THEN 'unversioned image; '
                        ELSE ''
                    END
                    || CASE
                        WHEN i.pull_count = 0 THEN 'never pulled; '
                        ELSE ''
                    END
                    || CASE
                        WHEN TRIM(COALESCE(i.time_last_pulled, '')) = ''
                        THEN 'no last pulled timestamp; '
                        ELSE ''
                    END
                    || CASE
                        WHEN TRIM(COALESCE(i.time_last_pulled, '')) <> ''
                          AND CAST(julianday('now') - julianday(i.time_last_pulled)
                              AS INTEGER) >= ?
                        THEN 'not pulled within retention window; '
                        ELSE ''
                    END
                    || CASE
                        WHEN TRIM(COALESCE(i.time_created, '')) <> ''
                          AND CAST(julianday('now') - julianday(i.time_created)
                              AS INTEGER) >= ?
                        THEN 'older than retention window; '
                        ELSE ''
                    END
                    || CASE
                        WHEN a.exclusive_billable_bytes >= ?
                        THEN 'high exclusive billable bytes; '
                        ELSE ''
                    END
                    || CASE
                        WHEN rc.repository_image_count > ?
                        THEN 'repository exceeds image count limit; '
                        ELSE ''
                    END,
                    '; '
                ) AS deletion_candidate_reason
            FROM images i
            JOIN image_attribution a ON a.image_id = i.id
            JOIN repo_counts rc ON rc.repository_id = i.repository_id
        )
        SELECT
            repository_name,
            display_name,
            version,
            versions,
            image_id,
            pull_count,
            time_created,
            time_last_pulled,
            repository_image_count,
            exclusive_billable_bytes,
            equal_share_attributed_billable_bytes,
            age_days,
            last_pulled_age_days,
            deletion_candidate_reason
        FROM deletion_base
        WHERE deletion_candidate_reason <> ''
        ORDER BY
            exclusive_billable_bytes DESC,
            equal_share_attributed_billable_bytes DESC,
            repository_name,
            display_name,
            image_id
        LIMIT 12
        """,
        (
            retention_last_pulled_days,
            retention_created_days,
            retention_exclusive_bytes,
            retention_repo_version_limit,
        ),
    )

    breakdown_rows = object_rows + tag_rows + reuse_rows
    breakdown_fields = [
        "view",
        "category",
        "item_count",
        "size_bytes",
        "size_human",
        "share_of_estimated_billable_percent",
        "note",
    ]
    with (output_dir / "storage_breakdown.csv").open(
        "w", newline="", encoding="utf-8"
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=breakdown_fields)
        writer.writeheader()
        for row in breakdown_rows:
            size_bytes = int(row["size_bytes"] or 0)
            writer.writerow(
                {
                    "view": row["view"],
                    "category": row["category"],
                    "item_count": int(row["item_count"] or 0),
                    "size_bytes": size_bytes,
                    "size_human": fmt_bytes(size_bytes),
                    "share_of_estimated_billable_percent": f"{pct(size_bytes, billable_total):.2f}",
                    "note": row["note"],
                }
            )

    def esc(value: Any) -> str:
        return html.escape(str(value), quote=True)

    def segment_bar(rows: Sequence[Dict[str, Any]], total: int) -> str:
        colors = ["#2f6f73", "#d35d33", "#6f5aa8", "#c89d2d", "#60717a"]
        pieces = []
        for index, row in enumerate(rows):
            size_bytes = int(row["size_bytes"] or 0)
            width = pct(size_bytes, total)
            if size_bytes == 0:
                continue
            pieces.append(
                '<span class="segment" '
                f'style="width:{width:.4f}%;background:{colors[index % len(colors)]}" '
                f'title="{esc(row["category"])}: {esc(fmt_bytes(size_bytes))}"></span>'
            )
        if not pieces:
            return '<div class="empty">No bytes in this view</div>'
        return '<div class="stacked">' + "".join(pieces) + "</div>"

    def legend(rows: Sequence[Dict[str, Any]], total: int) -> str:
        colors = ["#2f6f73", "#d35d33", "#6f5aa8", "#c89d2d", "#60717a"]
        items = []
        for index, row in enumerate(rows):
            size_bytes = int(row["size_bytes"] or 0)
            items.append(
                "<tr>"
                f'<td><span class="swatch" style="background:{colors[index % len(colors)]}"></span>'
                f'{esc(row["category"])}</td>'
                f'<td class="num">{int(row["item_count"] or 0):,}</td>'
                f'<td class="num">{esc(fmt_bytes(size_bytes))}</td>'
                f'<td class="num">{pct(size_bytes, total):.1f}%</td>'
                "</tr>"
            )
        return "".join(items)

    def repo_bars(rows: Sequence[Dict[str, Any]]) -> str:
        max_bytes = max((int(row["size_bytes"] or 0) for row in rows), default=0)
        bars = []
        for row in rows:
            size_bytes = int(row["size_bytes"] or 0)
            width = pct(size_bytes, max_bytes)
            bars.append(
                '<div class="repo-row">'
                f'<div class="repo-name">{esc(row["repository_name"])}</div>'
                '<div class="repo-meter">'
                f'<span style="width:{width:.4f}%"></span>'
                "</div>"
                f'<div class="repo-value">{esc(fmt_bytes(size_bytes))}</div>'
                "</div>"
            )
        return "".join(bars) or '<div class="empty">No repositories found</div>'

    def retention_criteria_view(rows: Sequence[Dict[str, Any]]) -> str:
        max_bytes = max(
            (int(row["attributed_bytes"] or 0) for row in rows),
            default=0,
        )
        items = []
        for row in rows:
            attributed_bytes = int(row["attributed_bytes"] or 0)
            exclusive_bytes = int(row["exclusive_bytes"] or 0)
            width = pct(attributed_bytes, max_bytes)
            items.append(
                '<div class="retention-row">'
                '<div>'
                f'<div class="retention-name">{esc(row["criterion"])}</div>'
                f'<div class="muted">{esc(row["note"])}</div>'
                "</div>"
                '<div class="retention-meter">'
                f'<span style="width:{width:.4f}%"></span>'
                "</div>"
                '<div class="retention-stats">'
                f'<strong>{int(row["image_count"] or 0):,}</strong> images'
                f'<br><span>{esc(fmt_bytes(attributed_bytes))} attributed</span>'
                f'<br><span>{esc(fmt_bytes(exclusive_bytes))} exclusive</span>'
                "</div>"
                "</div>"
            )
        return "".join(items) or '<div class="empty">No retention criteria matched</div>'

    def top_deletion_candidates(rows: Sequence[Dict[str, Any]]) -> str:
        max_bytes = max(
            (int(row["exclusive_billable_bytes"] or 0) for row in rows),
            default=0,
        )
        items = []
        for row in rows:
            exclusive_bytes = int(row["exclusive_billable_bytes"] or 0)
            attributed_bytes = int(row["equal_share_attributed_billable_bytes"] or 0)
            width = pct(exclusive_bytes, max_bytes)
            version = row["version"] or row["versions"] or "unversioned"
            age = row["age_days"] if row["age_days"] is not None else ""
            pulled_age = (
                row["last_pulled_age_days"]
                if row["last_pulled_age_days"] is not None
                else ""
            )
            items.append(
                '<div class="candidate-row">'
                '<div>'
                f'<div class="retention-name">{esc(row["display_name"])}</div>'
                f'<div class="muted">{esc(row["repository_name"])} | {esc(version)}</div>'
                f'<div class="candidate-reason">{esc(row["deletion_candidate_reason"])}</div>'
                "</div>"
                '<div class="retention-meter">'
                f'<span style="width:{width:.4f}%"></span>'
                "</div>"
                '<div class="retention-stats">'
                f'<strong>{esc(fmt_bytes(exclusive_bytes))}</strong> exclusive'
                f'<br><span>{esc(fmt_bytes(attributed_bytes))} attributed</span>'
                f'<br><span>pulls {int(row["pull_count"] or 0):,}'
                f' | age {esc(age)}d | pulled {esc(pulled_age)}d</span>'
                "</div>"
                "</div>"
            )
        return "".join(items) or '<div class="empty">No deletion candidates found</div>'

    object_visual_total = sum(int(row["size_bytes"] or 0) for row in object_rows)
    reuse_visual_total = sum(int(row["size_bytes"] or 0) for row in reuse_rows)
    confidence = summary.get("confidence", {})
    confidence_status = str(confidence.get("status", "WARNING"))
    confidence_metrics = confidence.get("metrics", {})
    confidence_class = confidence_status_class(confidence_status)
    confidence_reason = confidence.get(
        "reason",
        "Confidence details were not available for this report.",
    )
    confidence_html = f"""
  <section class="confidence confidence-{esc(confidence_class)}">
    <div>
      <h2>Report Confidence</h2>
      <div class="muted">{esc(confidence_reason)}</div>
    </div>
    <div class="confidence-status">{esc(confidence_status)}</div>
    <div class="confidence-grid">
      <div>Listed<strong>{int(confidence_metrics.get("listed_images", 0)):,}</strong></div>
      <div>Fetched<strong>{int(confidence_metrics.get("fetched_images", 0)):,}</strong></div>
      <div>Skipped<strong>{int(confidence_metrics.get("skipped_images", 0)):,}</strong></div>
      <div>Failed<strong>{int(confidence_metrics.get("failed_fetches", 0)):,}</strong></div>
      <div>Pruned<strong>{int(confidence_metrics.get("pruned_images", 0)):,}</strong></div>
      <div>Fetch errors<strong>{int(confidence_metrics.get("fetch_errors", 0)):,}</strong></div>
      <div>Attribution delta<strong>{esc(fmt_bytes(abs(int(confidence_metrics.get("attribution_delta_bytes", 0)))))}</strong></div>
      <div>Local unreferenced<strong>{esc(fmt_bytes(int(confidence_metrics.get("local_unreferenced_bytes", 0))))}</strong></div>
    </div>
  </section>
"""
    retention_html = f"""
  <section>
    <h2>Retention Policy Signals</h2>
    <div class="policy-grid">
      <div>Created age<strong>{retention_created_days:,}+ days</strong></div>
      <div>Last pulled<strong>{retention_last_pulled_days:,}+ days</strong></div>
      <div>Repo image limit<strong>{retention_repo_version_limit:,}</strong></div>
      <div>Exclusive storage<strong>{esc(fmt_bytes(retention_exclusive_bytes))}+</strong></div>
    </div>
    <div class="muted retention-help">
      Criteria overlap by design. Use this view to see which retention policy
      knobs identify the most attributed and exclusive storage outside the
      configured retention targets.
    </div>
    {retention_criteria_view(retention_criteria_rows)}
  </section>

  <section>
    <h2>Top Deletion Candidates</h2>
    <div class="muted retention-help">
      Ordered by exclusive billable bytes first, because exclusive-heavy images
      are more likely to free storage when removed.
    </div>
    {top_deletion_candidates(top_deletion_candidate_rows)}
  </section>
"""
    generated_at = esc(summary["generated_at"])
    report_html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>OCIR Storage Dashboard</title>
  <style>
    :root {{
      color-scheme: light;
      --ink: #202427;
      --muted: #657076;
      --line: #d8dee2;
      --panel: #f7f8f8;
      --bar-bg: #e6eaed;
    }}
    body {{
      margin: 0;
      font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--ink);
      background: #fff;
    }}
    main {{
      max-width: 1180px;
      margin: 0 auto;
      padding: 28px 24px 40px;
    }}
    header {{
      display: flex;
      justify-content: space-between;
      gap: 20px;
      align-items: flex-end;
      border-bottom: 1px solid var(--line);
      padding-bottom: 18px;
    }}
    h1, h2 {{
      margin: 0;
      letter-spacing: 0;
    }}
    h1 {{
      font-size: 26px;
    }}
    h2 {{
      font-size: 17px;
      margin-bottom: 12px;
    }}
    .muted {{
      color: var(--muted);
    }}
    .summary {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin: 18px 0 24px;
    }}
    .metric {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 12px;
    }}
    .metric strong {{
      display: block;
      font-size: 19px;
      margin-top: 2px;
    }}
    .confidence {{
      display: grid;
      grid-template-columns: minmax(180px, 1fr) auto;
      gap: 14px;
      align-items: start;
      border: 1px solid var(--line);
      border-left-width: 6px;
      border-radius: 6px;
      padding: 14px;
      margin: 18px 0 24px;
      background: var(--panel);
    }}
    .confidence-pass {{
      border-left-color: #2f6f73;
    }}
    .confidence-warning {{
      border-left-color: #c89d2d;
    }}
    .confidence-fail {{
      border-left-color: #d35d33;
    }}
    .confidence-status {{
      font-weight: 700;
      letter-spacing: 0;
      padding: 4px 8px;
      border-radius: 4px;
      background: #fff;
      border: 1px solid var(--line);
    }}
    .confidence-grid {{
      grid-column: 1 / -1;
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 8px;
      margin-top: 2px;
    }}
    .confidence-grid div {{
      background: #fff;
      border: 1px solid var(--line);
      border-radius: 5px;
      padding: 8px;
      color: var(--muted);
    }}
    .confidence-grid strong {{
      display: block;
      color: var(--ink);
      margin-top: 2px;
    }}
    .policy-grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 8px;
      margin: 8px 0 10px;
    }}
    .policy-grid div {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 5px;
      padding: 8px;
      color: var(--muted);
    }}
    .policy-grid strong {{
      display: block;
      color: var(--ink);
      margin-top: 2px;
    }}
    .retention-help {{
      margin-bottom: 10px;
    }}
    .retention-row,
    .candidate-row {{
      display: grid;
      grid-template-columns: minmax(240px, 1.4fr) minmax(180px, 1fr) 160px;
      gap: 10px;
      align-items: center;
      padding: 9px 0;
      border-bottom: 1px solid var(--line);
    }}
    .retention-name {{
      font-weight: 650;
      overflow-wrap: anywhere;
    }}
    .candidate-reason {{
      color: var(--muted);
      margin-top: 3px;
      overflow-wrap: anywhere;
    }}
    .retention-meter {{
      height: 16px;
      background: var(--bar-bg);
      border-radius: 4px;
      overflow: hidden;
      border: 1px solid var(--line);
    }}
    .retention-meter span {{
      display: block;
      height: 100%;
      background: #6f5aa8;
    }}
    .retention-stats {{
      text-align: right;
      white-space: nowrap;
      color: var(--muted);
    }}
    .retention-stats strong {{
      color: var(--ink);
    }}
    .grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 18px;
    }}
    section {{
      border-top: 1px solid var(--line);
      padding-top: 18px;
      margin-top: 18px;
    }}
    .stacked {{
      display: flex;
      overflow: hidden;
      height: 28px;
      background: var(--bar-bg);
      border-radius: 5px;
      border: 1px solid var(--line);
    }}
    .segment {{
      min-width: 2px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 10px;
    }}
    th, td {{
      padding: 7px 6px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: middle;
    }}
    th {{
      color: var(--muted);
      font-weight: 600;
    }}
    .num {{
      text-align: right;
      white-space: nowrap;
    }}
    .swatch {{
      display: inline-block;
      width: 10px;
      height: 10px;
      border-radius: 2px;
      margin-right: 8px;
      vertical-align: -1px;
    }}
    .repo-row {{
      display: grid;
      grid-template-columns: minmax(190px, 1fr) minmax(180px, 2fr) 100px;
      gap: 10px;
      align-items: center;
      margin: 8px 0;
    }}
    .repo-name {{
      overflow-wrap: anywhere;
    }}
    .repo-meter {{
      height: 16px;
      background: var(--bar-bg);
      border-radius: 4px;
      overflow: hidden;
      border: 1px solid var(--line);
    }}
    .repo-meter span {{
      display: block;
      height: 100%;
      background: #2f6f73;
    }}
    .repo-value {{
      text-align: right;
      white-space: nowrap;
    }}
    .note {{
      background: #fff7df;
      border: 1px solid #e8d28a;
      border-radius: 6px;
      padding: 10px 12px;
      margin-top: 18px;
    }}
    .empty {{
      color: var(--muted);
      padding: 8px 0;
    }}
    @media (max-width: 820px) {{
      header, .grid {{
        display: block;
      }}
      .summary {{
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }}
      .confidence {{
        display: block;
      }}
      .confidence-status {{
        display: inline-block;
        margin-top: 10px;
      }}
      .confidence-grid {{
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }}
      .policy-grid {{
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }}
      .retention-row,
      .candidate-row {{
        grid-template-columns: 1fr;
      }}
      .retention-stats {{
        text-align: left;
      }}
      .repo-row {{
        grid-template-columns: 1fr;
      }}
      .repo-value {{
        text-align: left;
      }}
    }}
  </style>
</head>
<body>
<main>
  <header>
    <div>
      <h1>OCIR Storage Dashboard</h1>
      <div class="muted">{esc(summary["region"])} generated {generated_at}</div>
    </div>
    <div class="muted">Source: ocir_inventory.sqlite</div>
  </header>

  <div class="summary">
    <div class="metric">Estimated billable<strong>{esc(summary["estimated_billable_total_human"])}</strong></div>
    <div class="metric">Naive image total<strong>{esc(summary["naive_image_total_human"])}</strong></div>
    <div class="metric">Dedup savings<strong>{esc(summary["dedup_savings_human"])}</strong></div>
    <div class="metric">Images / repos<strong>{int(summary["image_count"]):,} / {int(summary["repository_count"]):,}</strong></div>
  </div>

  {confidence_html}

  {retention_html}

  <div class="grid">
    <section>
      <h2>Physical Object Composition</h2>
      {segment_bar(object_rows, object_visual_total)}
      <table>
        <thead><tr><th>Category</th><th class="num">Items</th><th class="num">Size</th><th class="num">Share</th></tr></thead>
        <tbody>{legend(object_rows, object_visual_total)}</tbody>
      </table>
    </section>

    <section>
      <h2>Image Attribution By Tag Status</h2>
      {segment_bar(tag_rows, billable_total)}
      <table>
        <thead><tr><th>Category</th><th class="num">Images</th><th class="num">Size</th><th class="num">Share</th></tr></thead>
        <tbody>{legend(tag_rows, billable_total)}</tbody>
      </table>
    </section>
  </div>

  <section>
    <h2>Layer Reuse</h2>
    {segment_bar(reuse_rows, reuse_visual_total)}
    <table>
      <thead><tr><th>Category</th><th class="num">Layers</th><th class="num">Size</th><th class="num">Share of layers</th></tr></thead>
      <tbody>{legend(reuse_rows, reuse_visual_total)}</tbody>
    </table>
  </section>

  <section>
    <h2>Top Repositories By Attributed Storage</h2>
    {repo_bars(top_repo_rows)}
  </section>

  <div class="note">
    This report estimates OCIR storage from OCI ContainerImage metadata. It does
    not inspect the registry's raw blob store, does not prove hidden orphan blob
    counts, and does not replace OCI billing exports for invoiced charges.
  </div>
</main>
</body>
</html>
"""
    (output_dir / "dashboard.html").write_text(report_html, encoding="utf-8")


def write_report_from_db(
    conn: sqlite3.Connection,
    output_dir: Path,
    region: str,
    compartment_id: str,
    include_subtree: bool,
    include_ref_lists: bool,
    skip_layer_refs: bool,
    retention_created_days: int = 90,
    retention_last_pulled_days: int = 90,
    retention_repo_version_limit: int = 10,
    retention_exclusive_bytes: int = 1000 * 1000 * 1000,
    stats: Optional[Dict[str, int]] = None,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    build_attribution_table(conn)

    totals = conn.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM images) AS image_count,
            (SELECT COUNT(DISTINCT repository_id) FROM images) AS repository_count,
            (
                SELECT COUNT(*)
                FROM layers l
                WHERE EXISTS (
                    SELECT 1
                    FROM image_layers il
                    WHERE il.layer_digest = l.digest
                )
            ) AS unique_layer_count,
            (
                SELECT COUNT(*)
                FROM manifests m
                WHERE EXISTS (
                    SELECT 1
                    FROM image_manifests im
                    WHERE im.manifest_digest = m.digest
                )
            ) AS unique_manifest_count,
            (SELECT COALESCE(SUM(layers_size_in_bytes + manifest_size_in_bytes), 0)
                FROM images) AS naive_image_total_bytes,
            (
                SELECT COALESCE(SUM(l.size_in_bytes), 0)
                FROM layers l
                WHERE EXISTS (
                    SELECT 1
                    FROM image_layers il
                    WHERE il.layer_digest = l.digest
                )
            ) AS unique_layer_total_bytes,
            (
                SELECT COALESCE(SUM(m.size_in_bytes), 0)
                FROM manifests m
                WHERE EXISTS (
                    SELECT 1
                    FROM image_manifests im
                    WHERE im.manifest_digest = m.digest
                )
            ) AS unique_manifest_total_bytes,
            (SELECT COUNT(*) FROM fetch_errors) AS fetch_error_count
        """
    ).fetchone()
    unique_layer_total = int(totals["unique_layer_total_bytes"])
    unique_manifest_total = int(totals["unique_manifest_total_bytes"])
    naive_total = int(totals["naive_image_total_bytes"])
    estimated_billable_total = unique_layer_total + unique_manifest_total
    unreferenced = conn.execute(
        """
        SELECT
            (
                SELECT COUNT(*)
                FROM layers l
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM image_layers il
                    WHERE il.layer_digest = l.digest
                )
            ) AS local_unreferenced_layer_count,
            (
                SELECT COALESCE(SUM(l.size_in_bytes), 0)
                FROM layers l
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM image_layers il
                    WHERE il.layer_digest = l.digest
                )
            ) AS local_unreferenced_layer_bytes,
            (
                SELECT COUNT(*)
                FROM manifests m
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM image_manifests im
                    WHERE im.manifest_digest = m.digest
                )
            ) AS local_unreferenced_manifest_count,
            (
                SELECT COALESCE(SUM(m.size_in_bytes), 0)
                FROM manifests m
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM image_manifests im
                    WHERE im.manifest_digest = m.digest
                )
            ) AS local_unreferenced_manifest_bytes
        """
    ).fetchone()
    attributed_total = conn.execute(
        """
        SELECT COALESCE(SUM(equal_share_attributed_billable_bytes), 0) AS total
        FROM image_attribution
        """
    ).fetchone()["total"]
    repository_attributed_total = conn.execute(
        """
        SELECT COALESCE(SUM(repository_total), 0) AS total
        FROM (
            SELECT SUM(a.equal_share_attributed_billable_bytes) AS repository_total
            FROM images i
            JOIN image_attribution a ON a.image_id = i.id
            GROUP BY i.repository_id
        )
        """
    ).fetchone()["total"]

    stats_data = stats or {}
    local_unreferenced_layer_bytes = int(unreferenced["local_unreferenced_layer_bytes"])
    local_unreferenced_manifest_bytes = int(
        unreferenced["local_unreferenced_manifest_bytes"]
    )
    local_unreferenced_bytes = (
        local_unreferenced_layer_bytes + local_unreferenced_manifest_bytes
    )
    attribution_delta = int(attributed_total) - estimated_billable_total
    repository_delta = int(repository_attributed_total) - estimated_billable_total
    confidence_status = "PASS"
    confidence_reasons = []
    if attribution_delta != 0 or repository_delta != 0:
        confidence_status = "FAIL"
        confidence_reasons.append("billable attribution totals do not reconcile")
    if int(totals["fetch_error_count"]) or int(stats_data.get("failed", 0)):
        if confidence_status != "FAIL":
            confidence_status = "WARNING"
        confidence_reasons.append("one or more image metadata fetches failed")
    if local_unreferenced_bytes:
        if confidence_status != "FAIL":
            confidence_status = "WARNING"
        confidence_reasons.append(
            "local state contains unreferenced layer or manifest rows excluded from billable total"
        )
    confidence_reason = (
        "; ".join(confidence_reasons)
        if confidence_reasons
        else "all report reconciliation checks passed"
    )

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "region": region,
        "scope_compartment_id": compartment_id,
        "scope_includes_subtree": include_subtree,
        "image_count": int(totals["image_count"]),
        "repository_count": int(totals["repository_count"]),
        "unique_layer_count": int(totals["unique_layer_count"]),
        "unique_manifest_count": int(totals["unique_manifest_count"]),
        "naive_image_total_bytes": naive_total,
        "naive_image_total_human": fmt_bytes(naive_total),
        "unique_layer_total_bytes": unique_layer_total,
        "unique_layer_total_human": fmt_bytes(unique_layer_total),
        "unique_manifest_total_bytes": unique_manifest_total,
        "unique_manifest_total_human": fmt_bytes(unique_manifest_total),
        "estimated_billable_total_bytes": estimated_billable_total,
        "estimated_billable_total_human": fmt_bytes(estimated_billable_total),
        "local_unreferenced_layer_count": int(
            unreferenced["local_unreferenced_layer_count"]
        ),
        "local_unreferenced_layer_bytes": local_unreferenced_layer_bytes,
        "local_unreferenced_layer_human": fmt_bytes(local_unreferenced_layer_bytes),
        "local_unreferenced_manifest_count": int(
            unreferenced["local_unreferenced_manifest_count"]
        ),
        "local_unreferenced_manifest_bytes": local_unreferenced_manifest_bytes,
        "local_unreferenced_manifest_human": fmt_bytes(
            local_unreferenced_manifest_bytes
        ),
        "equal_share_attributed_total_bytes": int(attributed_total),
        "equal_share_attributed_total_human": fmt_bytes(int(attributed_total)),
        "repository_attributed_total_bytes": int(repository_attributed_total),
        "repository_attributed_total_human": fmt_bytes(
            int(repository_attributed_total)
        ),
        "dedup_savings_bytes": naive_total - estimated_billable_total,
        "dedup_savings_human": fmt_bytes(naive_total - estimated_billable_total),
        "fetch_error_count": int(totals["fetch_error_count"]),
        "collection_stats": stats_data,
        "retention_policy": {
            "created_days": retention_created_days,
            "last_pulled_days": retention_last_pulled_days,
            "repository_image_limit": retention_repo_version_limit,
            "exclusive_bytes": retention_exclusive_bytes,
            "exclusive_human": fmt_bytes(retention_exclusive_bytes),
        },
        "confidence": {
            "status": confidence_status,
            "reason": confidence_reason,
            "checks": {
                "equal_share_attribution_reconciles": attribution_delta == 0,
                "repository_attribution_reconciles": repository_delta == 0,
                "fetch_errors_absent": int(totals["fetch_error_count"]) == 0,
            },
            "metrics": {
                "listed_images": int(stats_data.get("listed", 0)),
                "fetched_images": int(stats_data.get("fetched", 0)),
                "skipped_images": int(stats_data.get("skipped", 0)),
                "failed_fetches": int(stats_data.get("failed", 0)),
                "pruned_images": int(stats_data.get("pruned", 0)),
                "fetch_errors": int(totals["fetch_error_count"]),
                "attribution_delta_bytes": attribution_delta,
                "repository_delta_bytes": repository_delta,
                "local_unreferenced_bytes": local_unreferenced_bytes,
                "local_unreferenced_layer_bytes": local_unreferenced_layer_bytes,
                "local_unreferenced_manifest_bytes": local_unreferenced_manifest_bytes,
            },
        },
        "note": (
            "Estimated billable total is unique layer blobs plus unique image "
            "manifest bytes from ContainerImage metadata. OCI billing exports "
            "remain the source of truth for invoiced charges."
        ),
    }

    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8"
    )

    write_query_csv(
        conn,
        output_dir / "images.csv",
        IMAGE_FIELDS,
        """
        SELECT
            i.region,
            i.compartment_id,
            i.repository_name,
            i.repository_id,
            i.id AS image_id,
            i.display_name,
            i.digest,
            i.version,
            i.versions,
            i.lifecycle_state,
            COALESCE((
                SELECT COUNT(*) FROM image_layers il WHERE il.image_id = i.id
            ), 0) AS layer_count,
            i.layers_size_in_bytes AS layers_size_bytes,
            i.manifest_size_in_bytes AS manifest_size_bytes,
            i.layers_size_in_bytes + i.manifest_size_in_bytes
                AS image_naive_total_bytes,
            a.unique_layer_bytes_referenced,
            a.shared_layer_bytes_referenced,
            a.exclusive_layer_bytes,
            a.exclusive_billable_bytes,
            a.equal_share_attributed_billable_bytes,
            fmt_bytes(a.equal_share_attributed_billable_bytes)
                AS equal_share_attributed_billable_human,
            i.pull_count,
            i.time_created,
            i.time_last_pulled
        FROM images i
        JOIN image_attribution a ON a.image_id = i.id
        ORDER BY i.repository_name, i.display_name, i.id
        """,
    )
    write_query_csv(
        conn,
        output_dir / "unversioned_images.csv",
        IMAGE_FIELDS,
        """
        SELECT
            i.region,
            i.compartment_id,
            i.repository_name,
            i.repository_id,
            i.id AS image_id,
            i.display_name,
            i.digest,
            i.version,
            i.versions,
            i.lifecycle_state,
            COALESCE((
                SELECT COUNT(*) FROM image_layers il WHERE il.image_id = i.id
            ), 0) AS layer_count,
            i.layers_size_in_bytes AS layers_size_bytes,
            i.manifest_size_in_bytes AS manifest_size_bytes,
            i.layers_size_in_bytes + i.manifest_size_in_bytes
                AS image_naive_total_bytes,
            a.unique_layer_bytes_referenced,
            a.shared_layer_bytes_referenced,
            a.exclusive_layer_bytes,
            a.exclusive_billable_bytes,
            a.equal_share_attributed_billable_bytes,
            fmt_bytes(a.equal_share_attributed_billable_bytes)
                AS equal_share_attributed_billable_human,
            i.pull_count,
            i.time_created,
            i.time_last_pulled
        FROM images i
        JOIN image_attribution a ON a.image_id = i.id
        WHERE TRIM(COALESCE(i.version, '')) = ''
          AND TRIM(COALESCE(i.versions, '')) = ''
        ORDER BY i.repository_name, i.display_name, i.id
        """,
    )
    write_query_csv(
        conn,
        output_dir / "deletion_candidates.csv",
        [
            "region",
            "compartment_id",
            "repository_name",
            "repository_id",
            "repository_image_count",
            "display_name",
            "version",
            "versions",
            "image_id",
            "digest",
            "time_created",
            "time_last_pulled",
            "pull_count",
            "layer_count",
            "exclusive_billable_bytes",
            "exclusive_billable_human",
            "equal_share_attributed_billable_bytes",
            "equal_share_attributed_billable_human",
            "exclusive_ratio",
            "age_days",
            "last_pulled_age_days",
            "deletion_candidate_reason",
        ],
        """
        WITH repo_counts AS (
            SELECT repository_id, COUNT(*) AS repository_image_count
            FROM images
            GROUP BY repository_id
        ),
        deletion_base AS (
            SELECT
                i.region,
                i.compartment_id,
                i.repository_name,
                i.repository_id,
                rc.repository_image_count,
                i.display_name,
                i.version,
                i.versions,
                i.id AS image_id,
                i.digest,
                i.time_created,
                i.time_last_pulled,
                i.pull_count,
                COALESCE((
                    SELECT COUNT(*) FROM image_layers il WHERE il.image_id = i.id
                ), 0) AS layer_count,
                a.exclusive_billable_bytes,
                fmt_bytes(a.exclusive_billable_bytes) AS exclusive_billable_human,
                a.equal_share_attributed_billable_bytes,
                fmt_bytes(a.equal_share_attributed_billable_bytes)
                    AS equal_share_attributed_billable_human,
                CASE
                    WHEN a.equal_share_attributed_billable_bytes > 0
                    THEN ROUND(
                        CAST(a.exclusive_billable_bytes AS REAL)
                        / a.equal_share_attributed_billable_bytes,
                        4
                    )
                    ELSE 0
                END AS exclusive_ratio,
                CASE
                    WHEN TRIM(COALESCE(i.time_created, '')) <> ''
                    THEN CAST(julianday('now') - julianday(i.time_created) AS INTEGER)
                    ELSE NULL
                END AS age_days,
                CASE
                    WHEN TRIM(COALESCE(i.time_last_pulled, '')) <> ''
                    THEN CAST(julianday('now') - julianday(i.time_last_pulled) AS INTEGER)
                    ELSE NULL
                END AS last_pulled_age_days,
                TRIM(
                    CASE
                        WHEN TRIM(COALESCE(i.version, '')) = ''
                          AND TRIM(COALESCE(i.versions, '')) = ''
                        THEN 'unversioned image; '
                        ELSE ''
                    END
                    || CASE
                        WHEN i.pull_count = 0 THEN 'never pulled; '
                        ELSE ''
                    END
                    || CASE
                        WHEN TRIM(COALESCE(i.time_last_pulled, '')) = ''
                        THEN 'no last pulled timestamp; '
                        ELSE ''
                    END
                    || CASE
                        WHEN TRIM(COALESCE(i.time_last_pulled, '')) <> ''
                          AND CAST(julianday('now') - julianday(i.time_last_pulled)
                              AS INTEGER) >= ?
                        THEN 'not pulled within retention window; '
                        ELSE ''
                    END
                    || CASE
                        WHEN TRIM(COALESCE(i.time_created, '')) <> ''
                          AND CAST(julianday('now') - julianday(i.time_created)
                              AS INTEGER) >= ?
                        THEN 'older than retention window; '
                        ELSE ''
                    END
                    || CASE
                        WHEN a.exclusive_billable_bytes >= ?
                        THEN 'high exclusive billable bytes; '
                        ELSE ''
                    END
                    || CASE
                        WHEN rc.repository_image_count > ?
                        THEN 'repository exceeds image count limit; '
                        ELSE ''
                    END,
                    '; '
                ) AS deletion_candidate_reason
            FROM images i
            JOIN image_attribution a ON a.image_id = i.id
            JOIN repo_counts rc ON rc.repository_id = i.repository_id
        )
        SELECT
            region,
            compartment_id,
            repository_name,
            repository_id,
            repository_image_count,
            display_name,
            version,
            versions,
            image_id,
            digest,
            time_created,
            time_last_pulled,
            pull_count,
            layer_count,
            exclusive_billable_bytes,
            exclusive_billable_human,
            equal_share_attributed_billable_bytes,
            equal_share_attributed_billable_human,
            exclusive_ratio,
            age_days,
            last_pulled_age_days,
            deletion_candidate_reason
        FROM deletion_base
        WHERE deletion_candidate_reason <> ''
        ORDER BY
            exclusive_billable_bytes DESC,
            equal_share_attributed_billable_bytes DESC,
            repository_name,
            display_name,
            image_id
        """,
        (
            retention_last_pulled_days,
            retention_created_days,
            retention_exclusive_bytes,
            retention_repo_version_limit,
        ),
    )

    repo_expr = (
        "REPLACE(group_concat(DISTINCT i.repository_name), ',', ';')"
        if include_ref_lists
        else "''"
    )
    image_expr = (
        "REPLACE(group_concat(DISTINCT i.display_name), ',', ';')"
        if include_ref_lists
        else "''"
    )
    write_query_csv(
        conn,
        output_dir / "layers.csv",
        [
            "layer_digest",
            "size_bytes",
            "size_human",
            "image_ref_count",
            "repository_ref_count",
            "equal_share_bytes_per_image_ref",
            "repositories",
            "images",
            "time_created",
        ],
        f"""
        WITH layer_ref_base AS (
            SELECT DISTINCT image_id, layer_digest
            FROM image_layers
        )
        SELECT
            l.digest AS layer_digest,
            l.size_in_bytes AS size_bytes,
            fmt_bytes(l.size_in_bytes) AS size_human,
            COUNT(b.image_id) AS image_ref_count,
            COUNT(DISTINCT i.repository_id) AS repository_ref_count,
            CAST(
                l.size_in_bytes
                / CASE WHEN COUNT(b.image_id) > 0 THEN COUNT(b.image_id) ELSE 1 END
                AS INTEGER
            )
                AS equal_share_bytes_per_image_ref,
            {repo_expr} AS repositories,
            {image_expr} AS images,
            l.time_created
        FROM layers l
        LEFT JOIN layer_ref_base b ON b.layer_digest = l.digest
        LEFT JOIN images i ON i.id = b.image_id
        GROUP BY l.digest
        ORDER BY l.size_in_bytes DESC, l.digest
        """,
    )
    write_query_csv(
        conn,
        output_dir / "unreferenced_layers.csv",
        [
            "layer_digest",
            "size_bytes",
            "size_human",
            "time_created",
        ],
        """
        SELECT
            l.digest AS layer_digest,
            l.size_in_bytes AS size_bytes,
            fmt_bytes(l.size_in_bytes) AS size_human,
            l.time_created
        FROM layers l
        WHERE NOT EXISTS (
            SELECT 1
            FROM image_layers il
            WHERE il.layer_digest = l.digest
        )
        ORDER BY l.size_in_bytes DESC, l.digest
        """,
    )
    write_query_csv(
        conn,
        output_dir / "unreferenced_manifests.csv",
        [
            "manifest_digest",
            "size_bytes",
            "size_human",
        ],
        """
        SELECT
            m.digest AS manifest_digest,
            m.size_in_bytes AS size_bytes,
            fmt_bytes(m.size_in_bytes) AS size_human
        FROM manifests m
        WHERE NOT EXISTS (
            SELECT 1
            FROM image_manifests im
            WHERE im.manifest_digest = m.digest
        )
        ORDER BY m.size_in_bytes DESC, m.digest
        """,
    )

    if not skip_layer_refs:
        write_query_csv(
            conn,
            output_dir / "layer_refs.csv",
            [
                "region",
                "repository_name",
                "image_id",
                "display_name",
                "image_digest",
                "layer_digest",
                "layer_size_bytes",
                "layer_ref_count",
                "equal_share_attributed_bytes",
            ],
            """
            WITH
            layer_ref_base AS (
                SELECT DISTINCT image_id, layer_digest
                FROM image_layers
            ),
            layer_refs AS (
                SELECT
                    b.image_id,
                    b.layer_digest,
                    l.size_in_bytes,
                    COUNT(*) OVER (PARTITION BY b.layer_digest) AS ref_count,
                    ROW_NUMBER() OVER (
                        PARTITION BY b.layer_digest ORDER BY b.image_id
                    ) AS ref_rank
                FROM layer_ref_base b
                JOIN layers l ON l.digest = b.layer_digest
            )
            SELECT
                i.region,
                i.repository_name,
                i.id AS image_id,
                i.display_name,
                i.digest AS image_digest,
                lr.layer_digest,
                lr.size_in_bytes AS layer_size_bytes,
                lr.ref_count AS layer_ref_count,
                CAST(lr.size_in_bytes / lr.ref_count AS INTEGER)
                    + CASE
                        WHEN lr.ref_rank <= (lr.size_in_bytes % lr.ref_count)
                        THEN 1 ELSE 0
                    END AS equal_share_attributed_bytes
            FROM layer_refs lr
            JOIN images i ON i.id = lr.image_id
            ORDER BY lr.layer_digest, i.repository_name, i.display_name, i.id
            """,
        )

    write_query_csv(
        conn,
        output_dir / "repositories.csv",
        [
            "region",
            "compartment_id",
            "repository_name",
            "repository_id",
            "image_count",
            "naive_total_bytes",
            "naive_total_human",
            "exclusive_billable_bytes",
            "exclusive_billable_human",
            "equal_share_attributed_billable_bytes",
            "equal_share_attributed_billable_human",
        ],
        """
        SELECT
            i.region,
            min(i.compartment_id) AS compartment_id,
            min(i.repository_name) AS repository_name,
            i.repository_id,
            COUNT(*) AS image_count,
            SUM(i.layers_size_in_bytes + i.manifest_size_in_bytes)
                AS naive_total_bytes,
            fmt_bytes(SUM(i.layers_size_in_bytes + i.manifest_size_in_bytes))
                AS naive_total_human,
            SUM(a.exclusive_billable_bytes) AS exclusive_billable_bytes,
            fmt_bytes(SUM(a.exclusive_billable_bytes)) AS exclusive_billable_human,
            SUM(a.equal_share_attributed_billable_bytes)
                AS equal_share_attributed_billable_bytes,
            fmt_bytes(SUM(a.equal_share_attributed_billable_bytes))
                AS equal_share_attributed_billable_human
        FROM images i
        JOIN image_attribution a ON a.image_id = i.id
        GROUP BY i.region, i.repository_id
        ORDER BY equal_share_attributed_billable_bytes DESC, repository_name
        """,
    )

    if summary["fetch_error_count"]:
        write_query_csv(
            conn,
            output_dir / "fetch_errors.csv",
            ["image_id", "repository_name", "display_name", "error", "updated_at"],
            """
            SELECT image_id, repository_name, display_name, error, updated_at
            FROM fetch_errors
            ORDER BY updated_at DESC
            """,
        )

    write_storage_visuals(conn, output_dir, summary)

    print(json.dumps(summary, indent=2, sort_keys=True))
    print(f"\nWrote report files to {output_dir}", file=sys.stderr)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report estimated OCIR storage utilization for one OCI region."
    )
    parser.add_argument("--region", help="OCI region to scan, for example us-ashburn-1.")
    parser.add_argument(
        "--compartment-id",
        help=(
            "Compartment OCID to scan. Defaults to the tenancy OCID from the OCI "
            "config, which allows --include-subtree."
        ),
    )
    parser.add_argument(
        "--include-subtree",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include child compartments. Only valid when compartment-id is the tenancy/root compartment.",
    )
    parser.add_argument(
        "--lifecycle-state",
        default="AVAILABLE",
        help="Container image lifecycle state filter. Default: AVAILABLE.",
    )
    parser.add_argument(
        "--repository-name",
        help="Optional repository-name filter. OCI supports exact names and wildcard suffixes like foo*.",
    )
    parser.add_argument(
        "--repository-id",
        help="Optional repository OCID filter. Useful for sharding very large registries.",
    )
    parser.add_argument(
        "--config-file",
        default="~/.oci/config",
        help="OCI config file path. Default: ~/.oci/config.",
    )
    parser.add_argument("--profile", default="DEFAULT", help="OCI config profile.")
    parser.add_argument(
        "--page-size",
        type=int,
        default=1000,
        help="List API page size. Default: 1000.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=16,
        help="Concurrent get_container_image workers. Default: 16.",
    )
    parser.add_argument(
        "--max-pending",
        type=int,
        default=0,
        help="Maximum queued image fetches. Default: workers * 4.",
    )
    parser.add_argument(
        "--commit-interval",
        type=int,
        default=500,
        help="Commit and print progress after this many completed fetches. Default: 500.",
    )
    parser.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Reuse fetched image metadata in the state database. Default: true.",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Refetch images even if they already exist in the state database.",
    )
    parser.add_argument(
        "--state-db",
        type=Path,
        help="SQLite state database path. Default: <output-dir>/ocir_inventory.sqlite.",
    )
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="Delete the state database before scanning. Use when changing scan scope.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop on the first failed image metadata fetch.",
    )
    parser.add_argument(
        "--include-ref-lists",
        action="store_true",
        help=(
            "Include repository/image name lists in layers.csv. Off by default "
            "because very shared layers can produce huge cells."
        ),
    )
    parser.add_argument(
        "--skip-layer-refs",
        action="store_true",
        help="Skip layer_refs.csv. Useful when the image-layer edge list is very large.",
    )
    parser.add_argument(
        "--retention-created-days",
        type=int,
        default=90,
        help=(
            "Flag deletion candidates created at least this many days ago in "
            "deletion_candidates.csv. Default: 90."
        ),
    )
    parser.add_argument(
        "--retention-last-pulled-days",
        type=int,
        default=90,
        help=(
            "Flag deletion candidates not pulled within this many days in "
            "deletion_candidates.csv. Default: 90."
        ),
    )
    parser.add_argument(
        "--retention-repo-version-limit",
        type=int,
        default=10,
        help=(
            "Flag images in repositories with more than this many scanned images. "
            "Default: 10."
        ),
    )
    parser.add_argument(
        "--retention-exclusive-bytes",
        type=int,
        default=1000 * 1000 * 1000,
        help=(
            "Flag images with at least this many exclusive billable bytes. "
            "Default: 1000000000 (1 GB)."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("ocir-storage-report"),
        help="Directory for summary.json and CSV outputs.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    oci = import_oci()

    if args.workers < 1:
        raise SystemExit("--workers must be at least 1.")
    if args.page_size < 1:
        raise SystemExit("--page-size must be at least 1.")
    if args.commit_interval < 1:
        raise SystemExit("--commit-interval must be at least 1.")
    if args.max_pending < 0:
        raise SystemExit("--max-pending cannot be negative.")
    if args.retention_created_days < 0:
        raise SystemExit("--retention-created-days cannot be negative.")
    if args.retention_last_pulled_days < 0:
        raise SystemExit("--retention-last-pulled-days cannot be negative.")
    if args.retention_repo_version_limit < 0:
        raise SystemExit("--retention-repo-version-limit cannot be negative.")
    if args.retention_exclusive_bytes < 0:
        raise SystemExit("--retention-exclusive-bytes cannot be negative.")

    config = oci.config.from_file(args.config_file, args.profile)
    region = args.region or config.get("region")
    if not region:
        raise SystemExit("Pass --region or set region in the OCI config profile.")

    compartment_id = args.compartment_id or config.get("tenancy")
    if not compartment_id:
        raise SystemExit(
            "Pass --compartment-id or use an OCI config profile with a tenancy OCID."
        )

    if args.include_subtree and compartment_id != config.get("tenancy"):
        print(
            "Warning: --include-subtree is only valid from the tenancy/root compartment. "
            "Disabling subtree traversal for this non-root compartment.",
            file=sys.stderr,
        )
        args.include_subtree = False

    args.output_dir.mkdir(parents=True, exist_ok=True)
    state_db = args.state_db or (args.output_dir / "ocir_inventory.sqlite")
    if args.reset_state:
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(str(state_db) + suffix)
            if candidate.exists():
                candidate.unlink()

    conn = init_db(state_db)
    ensure_scope(
        conn,
        scope_values(
            region=region,
            compartment_id=compartment_id,
            include_subtree=args.include_subtree,
            lifecycle_state=args.lifecycle_state,
            repository_name=args.repository_name,
            repository_id=args.repository_id,
        ),
    )

    client = make_artifacts_client(oci, config, region)
    stats = collect_images_to_db(
        conn=conn,
        oci=oci,
        config=config,
        client=client,
        region=region,
        compartment_id=compartment_id,
        include_subtree=args.include_subtree,
        lifecycle_state=args.lifecycle_state,
        page_size=args.page_size,
        repository_name=args.repository_name,
        repository_id=args.repository_id,
        workers=args.workers,
        max_pending=args.max_pending or args.workers * 4,
        commit_interval=args.commit_interval,
        resume=args.resume,
        refresh=args.refresh,
        fail_fast=args.fail_fast,
    )
    write_report_from_db(
        conn=conn,
        output_dir=args.output_dir,
        region=region,
        compartment_id=compartment_id,
        include_subtree=args.include_subtree,
        include_ref_lists=args.include_ref_lists,
        skip_layer_refs=args.skip_layer_refs,
        retention_created_days=args.retention_created_days,
        retention_last_pulled_days=args.retention_last_pulled_days,
        retention_repo_version_limit=args.retention_repo_version_limit,
        retention_exclusive_bytes=args.retention_exclusive_bytes,
        stats=stats,
    )
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
