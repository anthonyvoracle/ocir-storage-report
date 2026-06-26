#!/usr/bin/env python3
"""
ocir_unique_layers.py

Estimate OCIR-style storage footprint from one JSON manifest per image.

Expected input:
  - JSON produced by:
      docker manifest inspect --verbose <image> > image1.json
      docker manifest inspect --verbose <image> > image2.json
    or equivalent OCI registry manifest JSON.

Notes:
  - This script assumes each input file is a platform-specific image manifest
    that contains a top-level "layers" array.
  - If you pass a manifest list / image index, it will warn and skip it.
  - It computes:
      1) naive image-size total = sum of all layer sizes across all images
      2) unique-layer total = sum of each distinct layer digest once
"""

from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple


@dataclass
class LayerInfo:
    size: int = 0
    media_type: str = ""
    images: Set[str] = field(default_factory=set)


def load_manifest(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def image_name_from_path(path: Path) -> str:
    return path.stem


def iter_layers(manifest: dict) -> Iterable[Tuple[str, int, str]]:
    if isinstance(manifest, dict) and "manifests" in manifest and "layers" not in manifest:
        raise ValueError("manifest list / image index detected (no top-level layers[]).")

    layers = manifest.get("layers")
    if not isinstance(layers, list):
        raise ValueError("no top-level layers[] found")

    for layer in layers:
        if not isinstance(layer, dict):
            continue
        digest = layer.get("digest") or layer.get("blobSum")
        if not digest:
            continue
        size = layer.get("size")
        try:
            size_int = int(size) if size is not None else 0
        except (TypeError, ValueError):
            size_int = 0
        media_type = str(layer.get("mediaType", "") or "")
        yield digest, size_int, media_type


def fmt_bytes(n: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    value = float(n)
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{n} B"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Estimate OCIR-style storage by deduplicating layer digests."
    )
    parser.add_argument(
        "manifests",
        nargs="+",
        help="Paths to JSON manifest files, one per image.",
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="Print per-layer attribution as CSV.",
    )
    args = parser.parse_args()

    unique_layers: Dict[str, LayerInfo] = {}
    image_totals: List[Tuple[str, int, int]] = []  # image, layer_count, total_bytes
    naive_total = 0

    for raw_path in args.manifests:
        path = Path(raw_path)
        image = image_name_from_path(path)

        try:
            manifest = load_manifest(path)
            layers = list(iter_layers(manifest))
        except Exception as exc:
            print(f"Skipping {path}: {exc}", file=os.sys.stderr)
            continue

        image_bytes = 0
        for digest, size, media_type in layers:
            image_bytes += size
            info = unique_layers.setdefault(digest, LayerInfo(size=size, media_type=media_type))
            info.images.add(image)
            if info.size == 0 and size:
                info.size = size
            elif size and info.size and info.size != size:
                # Keep the larger value if manifests disagree on size, but flag it later.
                info.size = max(info.size, size)

        naive_total += image_bytes
        image_totals.append((image, len(layers), image_bytes))

    unique_total = sum(info.size for info in unique_layers.values())

    print("\nPer-image totals")
    print("image\tlayers\tbytes\tformatted")
    for image, layer_count, image_bytes in sorted(image_totals):
        print(f"{image}\t{layer_count}\t{image_bytes}\t{fmt_bytes(image_bytes)}")

    print("\nSummary")
    print(f"Images processed: {len(image_totals)}")
    print(f"Naive image-size total: {naive_total} ({fmt_bytes(naive_total)})")
    print(f"Unique layer count: {len(unique_layers)}")
    print(f"Unique-layer total: {unique_total} ({fmt_bytes(unique_total)})")
    print(f"Dedup savings: {naive_total - unique_total} ({fmt_bytes(naive_total - unique_total)})")

    print("\nTop shared layers")
    print("digest\tsize\trefs\tformatted_size")
    for digest, info in sorted(
        unique_layers.items(),
        key=lambda kv: (len(kv[1].images), kv[1].size),
        reverse=True,
    )[:25]:
        print(f"{digest}\t{info.size}\t{len(info.images)}\t{fmt_bytes(info.size)}")

    if args.csv:
        print("\nlayer_digest,size,reference_count,images")
        for digest, info in sorted(unique_layers.items(), key=lambda kv: kv[0]):
            imgs = ";".join(sorted(info.images))
            print(f"{digest},{info.size},{len(info.images)},\"{imgs}\"")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())