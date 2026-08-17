# Streaming Generator

Synthetic playback event generator + validator + S3 uploader.
Validates that the Phase 1 Streaming skills are implementable end-to-end.

Implements the happy path of:
[skills/aws/analytics-team/streaming/02_synthetic_data_generator.md](../skills/aws/analytics-team/streaming/02_synthetic_data_generator.md)

## Files

| File | Purpose |
|---|---|
| `generator.py` | Implements skill #02 — emits realistic playback events to local JSONL.gz |
| `validate_events.py` | Asserts output matches the schema and rules in skills #02 + #03 |
| `mock_tconsts.py` | 50 hardcoded popular IMDb titles (Shawshank, Dark Knight, etc.) — replaces real IMDb download |
| `upload_to_s3.py` | Uploads `./generator_out/` to a real S3 bucket (requires AWS creds) |
| `requirements.txt` | Python deps |

## Setup

```bash
cd streaming-generator
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

### 1. Generate ~1K events to local files

```bash
python generator.py \
  --start-ts 2026-05-02T20:00:00Z \
  --end-ts 2026-05-02T21:00:00Z \
  --rate 0.3 \
  --output-target file \
  --seed 42
```

Output lands at `./generator_out/yyyy=*/mm=*/dd=*/hh=*/events_*.jsonl.gz`.

### 2. Validate the output

```bash
python validate_events.py ./generator_out/
```

Runs 7 checks (schema, enums, sequencing, monotonicity, FK integrity, no-dup event_ids, distribution sanity). Exits 0 on PASS, 1 on FAIL.

### 3. Upload to S3 (requires AWS setup)

First do AWS setup (one-time):

```bash
brew install awscli
aws configure   # paste Access Key ID + Secret + region us-west-2
aws sts get-caller-identity   # verify
aws s3 mb s3://acme-dw-streaming-xs2026 --region us-west-2
```

Then upload:

```bash
python upload_to_s3.py \
  --bucket acme-dw-streaming-xs2026 \
  --prefix generator_replay/

# Verify
aws s3 ls s3://acme-dw-streaming-xs2026/generator_replay/ --recursive
```

Use `--dry-run` to preview without uploading.

## Scope (minimal validation harness)

In scope (skill #02 happy path):
- 100 customers x 20 device versions x 50 mock IMDb titles
- All 6 event types (play, pause, seek, resume, complete, exit)
- Position monotonicity enforced (jumps allowed only on `seek`)
- Time-of-day weights (sinusoidal, prime-time peak)
- Genre affinity (Dirichlet)
- Reproducible (same `--seed` = identical output)

Out of scope (deferred):
- Late events (1% lag), duplicates (0.5%), schema mismatches (0.1%)
- Series binge logic (70% next-episode chance)
- Universe persistence across runs
- Real IMDb data download
- `--output-target kinesis` (only `file` and `s3` work)
- Production scale (1M customers, 50M events/day)

## What success proves

| Skill | What this validates |
|---|---|
| #01 (IMDb) | tconst format is consistent in mock data -> real IMDb data will fit the same schema |
| #02 (Generator) | Full design is implementable from the spec alone. Output matches documented event schema, behaviour models, time-of-day distribution, and reproducibility |
| #03 (Landing) | Output JSONL.gz format and hour-partition layout match the real S3 landing zone |

If the validator passes after a generator run, the Phase 1 skills are confirmed coherent and end-to-end working.
