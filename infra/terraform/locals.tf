locals {
  name = "${var.project}-${var.env}"

  common_tags = {
    Project   = var.project
    Env       = var.env
    ManagedBy = "terraform"
    Repo      = "Full Stack Data Engineer - Streaming UseCases"
  }

  warehouse_uri = "s3://${var.data_bucket}/"

  # --- Glue catalog databases (one per medallion zone) -------------------------
  # Must match glue/common.py (DB_LANDING/RAW/PROCESSED/REPORTING).
  glue_databases = {
    landing   = "streaming_landing"
    raw       = "streaming_raw"
    processed = "streaming_processed"
    reporting = "streaming_reporting"
  }

  # Iceberg GlueCatalog lays each table out under s3://<bucket>/<db>.db/<table>/.
  zone_prefixes = {
    landing   = "${local.glue_databases.landing}.db/*"
    raw       = "${local.glue_databases.raw}.db/*"
    processed = "${local.glue_databases.processed}.db/*"
    reporting = "${local.glue_databases.reporting}.db/*"
    imdb      = "imdb_base/*"
    reports   = "reports/*"
  }

  # --- Glue jobs --------------------------------------------------------------
  # name = file under glue/jobs/ ; zone = the per-zone IAM role it runs as ;
  # args = job-specific --args Airflow passes (declared here so the job validates).
  # Names must match airflow/dags/common/config.py GLUE{}.
  glue_jobs = {
    streaming_imdb_to_raw           = { file = "imdb_to_raw.py", zone = "raw_writer", args = {} }
    streaming_raw_events            = { file = "raw_events.py", zone = "raw_writer", args = { "--data_interval_start" = "", "--data_interval_end" = "" } }
    streaming_raw_snapshots         = { file = "raw_snapshots.py", zone = "raw_writer", args = {} }
    streaming_processed_dims        = { file = "processed_dims.py", zone = "processed_writer", args = {} }
    streaming_fact_playback_events  = { file = "fact_playback_events.py", zone = "processed_writer", args = { "--data_interval_start" = "", "--data_interval_end" = "" } }
    streaming_fact_view_sessions    = { file = "fact_view_sessions.py", zone = "processed_writer", args = { "--data_interval_start" = "", "--data_interval_end" = "" } }
    streaming_fact_daily_engagement = { file = "fact_daily_engagement.py", zone = "processed_writer", args = { "--engagement_date" = "" } }
    streaming_reporting_aggregates  = { file = "reporting_aggregates.py", zone = "reporting_writer", args = { "--engagement_date" = "" } }
    streaming_compaction_landing    = { file = "compaction_landing.py", zone = "maintenance", args = {} }
    streaming_compaction_facts      = { file = "compaction_facts.py", zone = "maintenance", args = {} }
  }

  # --- Per-zone Glue role write scopes ----------------------------------------
  # Each role can READ the whole warehouse but WRITE only its zone's prefixes.
  glue_role_write_prefixes = {
    raw_writer = [local.zone_prefixes.raw]
    # "processed/*" is the legacy v1 layout where the conformed dims still live
    # (processed/streaming/dim_*). dims_refresh upserts them IN PLACE to preserve the
    # surrogate keys the v2 facts reference, so the role must write there too.
    processed_writer = [local.zone_prefixes.processed, "processed/*"]
    reporting_writer = [local.zone_prefixes.reporting]
    # Compaction rewrites + expires snapshots on landing and the facts: needs delete too.
    maintenance = [local.zone_prefixes.landing, local.zone_prefixes.processed]
  }

  # --- Lambda function names (match airflow config.py) ------------------------
  lambda_imdb_mirror = "streaming_imdb_mirror"
  lambda_html_render = "streaming_html_render"

  # Local relative paths to sibling code dirs (this module is infra/terraform/).
  repo_root  = "${path.module}/../.."
  glue_dir   = "${path.module}/../../glue"
  lambda_dir = "${path.module}/../../lambda"
  kafka_dir  = "${path.module}/../../kafka"

  # Glue scripts uploaded to the ops bucket; shared importables every job needs
  # on its PYTHONPATH (common.py for all, compaction_landing.py for compaction_facts).
  glue_extra_py_files = join(",", [
    "s3://${var.ops_bucket}/glue-scripts/common.py",
    "s3://${var.ops_bucket}/glue-scripts/jobs/compaction_landing.py",
  ])
}
