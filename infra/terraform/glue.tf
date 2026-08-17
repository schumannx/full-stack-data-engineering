# --- Catalog databases (one per medallion zone) --------------------------------
resource "aws_glue_catalog_database" "zones" {
  for_each     = local.glue_databases
  name         = each.value
  description  = "Streaming DW ${each.key} zone (Iceberg)."
  location_uri = "${local.warehouse_uri}${each.value}.db"
}

# --- PySpark jobs --------------------------------------------------------------
# One aws_glue_job per script under glue/jobs/, each running as its per-zone role.
resource "aws_glue_job" "jobs" {
  for_each = local.glue_jobs

  name              = each.key
  role_arn          = aws_iam_role.glue_zone[each.value.zone].arn
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  # Streaming microbatch retries are owned by Airflow; keep Glue's own retry at 0.
  max_retries     = 0
  timeout         = 60
  execution_class = "STANDARD"

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.ops.id}/glue-scripts/jobs/${each.value.file}"
  }

  default_arguments = merge(
    {
      "--job-language"                     = "python"
      "--datalake-formats"                 = "iceberg"
      "--enable-glue-datacatalog"          = "true"
      "--extra-py-files"                   = local.glue_extra_py_files
      "--TempDir"                          = "s3://${aws_s3_bucket.ops.id}/glue-temp/"
      "--enable-metrics"                   = "true"
      "--enable-continuous-cloudwatch-log" = "true"
      "--enable-job-insights"              = "true"
    },
    each.value.args,
  )

  depends_on = [
    aws_s3_object.glue_common,
    aws_s3_object.glue_job_scripts,
  ]
}
