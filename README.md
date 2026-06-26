# OCIR Storage Report

This folder contains scripts for estimating Oracle Cloud Infrastructure Registry
(OCIR) storage utilization. The main script is `ocir_storage_report.py`.

`ocir_storage_report.py` scans OCIR container images in a single OCI region,
deduplicates image layers by digest, and writes CSV/JSON outputs that attribute
regional storage usage back to images, layers, and repositories.

## What It Reports

The script builds an estimated storage view from OCI Container Registry metadata:

- Images visible from a compartment, optionally including child compartments.
- Image manifest sizes.
- Image layer sizes.
- Unique layer storage by digest.
- Shared-layer references across images and repositories.
- Per-image and per-repository billable attribution.
- A regional estimated billable total.

The estimated billable total is calculated as:

```text
unique layer bytes + unique image manifest bytes
```

OCI billing exports remain the source of truth for invoiced charges. This report
is intended for operational visibility and chargeback-style attribution.

## Requirements

Create a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

You also need an OCI config profile with permission to list and read OCIR images:

```text
~/.oci/config
```

The default profile is `DEFAULT`.

## Basic Usage

Run a report for one region:

```bash
cd ocir-report
source .venv/bin/activate
./ocir_storage_report.py --region us-ashburn-1 --output-dir ./out
```

By default, the script uses the tenancy OCID from the OCI config as the scope
compartment and includes child compartments.

Run against a specific compartment:

```bash
./ocir_storage_report.py \
  --region us-ashburn-1 \
  --compartment-id <compartment_ocid> \
  --no-include-subtree \
  --output-dir ./out
```

Use a non-default OCI config profile:

```bash
./ocir_storage_report.py \
  --profile MY_PROFILE \
  --region us-phoenix-1 \
  --output-dir ./out-phx
```

Filter to one repository name:

```bash
./ocir_storage_report.py \
  --region us-ashburn-1 \
  --repository-name my-team/my-image \
  --output-dir ./out
```

## Large Registry Usage

For larger registries, such as a region with 200k+ images, use the SQLite-backed
resume and concurrency options:

```bash
./ocir_storage_report.py \
  --region us-ashburn-1 \
  --output-dir ./out-iad \
  --state-db ./out-iad/ocir_inventory.sqlite \
  --workers 32 \
  --commit-interval 1000 \
  --skip-layer-refs
```

The script writes fetched image metadata to the state database as it runs. If the
run is interrupted, rerun the same command and it will skip images already stored
in the database.

Useful large-run options:

- `--workers`: Number of concurrent `get_container_image` calls. Start with
  `16` or `32`; reduce this if OCI throttling is heavy.
- `--max-pending`: Maximum queued fetches. Defaults to `workers * 4`.
- `--commit-interval`: Number of completed fetches between SQLite commits and
  progress messages.
- `--resume` / `--no-resume`: Reuse or ignore existing image metadata in the
  state database. Resume is on by default.
- `--refresh`: Refetch images even when they already exist in the state
  database.
- `--state-db`: Explicit SQLite state database path.
- `--reset-state`: Delete the state database before scanning. Use this when
  intentionally changing scan scope.
- `--skip-layer-refs`: Skip `layer_refs.csv`, which can be very large because it
  has one row per image-to-layer relationship.
- `--include-ref-lists`: Include image/repository name lists in `layers.csv`.
  This is off by default because very shared layers can create huge CSV cells.

For sharding, run the script per repository OCID:

```bash
./ocir_storage_report.py \
  --region us-ashburn-1 \
  --repository-id <repository_ocid> \
  --output-dir ./out-repo-example \
  --workers 16
```

Use a separate `--state-db` per shard unless you are intentionally accumulating
the same scan scope.

## Publishing Safety

Do not commit generated report outputs. Files under `out*/` can contain OCIDs,
repository names, image names, digests, timestamps, and usage data from your
tenancy. The included `.gitignore` excludes those files by default.

## Output Files

The script writes the following files to `--output-dir`.

### `summary.json`

High-level regional totals:

- Image count.
- Repository count.
- Unique layer count.
- Unique manifest count.
- Naive image total bytes.
- Estimated billable total bytes.
- Deduplication savings.
- Fetch error count.
- Local unreferenced layer/manifest row counts from the SQLite state database.
- Collection statistics for the run.

### `dashboard.html`

A standalone visual dashboard for a quick storage-utilization read:

- Physical object composition: referenced layer blobs, manifests, and any local
  unreferenced layer/manifest rows in the state database.
- Image attribution by tag status: versioned images versus unversioned images,
  using equal-share attributed bytes so the buckets reconcile to the estimated
  billable total.
- Layer reuse: exclusive layers versus shared layers.
- Top repositories by attributed storage.

Open this file in a browser after the run finishes.

### `storage_breakdown.csv`

The same category rollups used by `dashboard.html`, in CSV form. This is useful
when you want to pivot or chart the dashboard inputs elsewhere.

### `images.csv`

One row per image. Useful columns include:

- `repository_name`
- `display_name`
- `digest`
- `versions`
- `layers_size_bytes`
- `manifest_size_bytes`
- `image_naive_total_bytes`
- `exclusive_billable_bytes`
- `equal_share_attributed_billable_bytes`

`image_naive_total_bytes` is the image's own layer plus manifest size before
deduplication.

`exclusive_billable_bytes` includes only bytes used by that image alone.

`equal_share_attributed_billable_bytes` splits shared layer and manifest bytes
equally across all images that reference them. These values are designed to
reconcile back to the regional estimated billable total.

### `layers.csv`

One row per unique layer digest:

- Layer size.
- Number of images referencing the layer.
- Number of repositories referencing the layer.
- Repositories and images using the layer.

Repository and image name lists are blank by default for scale. Pass
`--include-ref-lists` to populate them.

### `layer_refs.csv`

One row per image-to-layer reference:

- Image.
- Repository.
- Layer digest.
- Layer size.
- Layer reference count.
- Equal-share attributed bytes for that image/layer relationship.

This file can become very large for high image counts. Pass `--skip-layer-refs`
when you only need summary, image, layer, and repository rollups.

### `repositories.csv`

Repository-level rollup:

- Image count.
- Naive total bytes.
- Exclusive billable bytes.
- Equal-share attributed billable bytes.

This is usually the best file for a first-pass ownership or chargeback view.

## Attribution Model

OCIR stores shared layer blobs once, but many images can reference the same
layer. Because a shared layer cannot be fully attributed to every referencing
image without double-counting, the script reports multiple views:

- Naive size: counts each image independently, including duplicate layer refs.
- Exclusive size: counts only storage used by exactly one image.
- Equal-share attribution: splits shared bytes across all referencing images.

Use equal-share attribution when totals need to reconcile to the regional
estimated billable total.

## Important Caveats

- The script scans one OCI region per run. Run it once per region you want to
  report.
- The default execution path uses a SQLite state database at
  `<output-dir>/ocir_inventory.sqlite`.
- The report is based on OCI Container Registry image metadata, not billing
  exports.
- The unreferenced layer/blob check is limited to rows already present in the
  local SQLite state database. The scanner currently inventories blobs through
  image metadata, so an exact count of registry blobs that are not referenced by
  any image manifest requires an independent OCIR blob inventory source, if one
  is available for your tenancy.
- The script currently focuses on container images. If your OCIR usage includes
  Helm charts or generic OCI artifacts, the script should be extended to include
  those APIs.
- Deleted or unavailable images are excluded by default because
  `--lifecycle-state` defaults to `AVAILABLE`.
- Subtree scanning is only valid when the scope compartment is the tenancy/root
  compartment.

## Related Script

`ocir-footprint-script.py` is an offline manifest calculator. It reads local
manifest JSON files and deduplicates layers across those files. It is useful for
experiments, but it does not inventory OCIR resources from OCI.
