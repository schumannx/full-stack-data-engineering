# infra/terraform/ — Phase 0 data-plane

The always-on AWS footprint for the pipeline (PLAN_v2.md §8 Phase 0). The
**prod Airflow** stack is the separate, apply-on-demand [airflow-ecs/](airflow-ecs/)
module — this root module does **not** create it.

## What it creates

| File | Resources |
|------|-----------|
| [s3.tf](s3.tf) | Data bucket (us-west-2, optional manage), us-east-1 **ops bucket** (Athena results + Glue scripts), script uploads |
| [glue.tf](glue.tf) | 4 catalog DBs (`streaming_landing/raw/processed/reporting`) + 10 PySpark jobs |
| [iam.tf](iam.tf) | Per-zone Glue roles, `airflow-execution` role, 2 Lambda roles, Kafka instance profile |
| [lambda.tf](lambda.tf) | `streaming_imdb_mirror`, `streaming_html_render` (zipped from `lambda/`) |
| [athena.tf](athena.tf) | Workgroup with us-east-1 result location |
| [kafka_ec2.tf](kafka_ec2.tf) | Single-node KRaft broker + co-located consumer (user-data pulls `kafka/`) |
| [redshift.tf](redshift.tf) | Redshift Serverless namespace + workgroup + Spectrum role |
| [networking.tf](networking.tf) | Default-VPC lookups + security groups |

Resource names are the contract with the rest of the repo: Glue job names match
`airflow/dags/common/config.py` `GLUE{}`, Lambda names match `LAMBDA_*`, DB names
match `glue/common.py`.

## Regions — why two providers
The data bucket / Iceberg warehouse is **us-west-2** (pre-exists from the
`streaming-etl` baseline); Glue, Athena, Lambda, Redshift, and Kafka EC2
are **us-east-1** (an org SCP blocks us-west-2 compute APIs for our IAM user).
Athena's result location must be co-located with the workgroup, hence the
dedicated us-east-1 **ops bucket**.

## Usage

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars      # optional; all vars have defaults
export TF_VAR_redshift_admin_password='...'        # or leave unset (Secrets Manager)

terraform init
terraform plan
terraform apply
```

### The data bucket already exists
It was created by the earlier project. Either:
- **Import it** (default, `manage_data_bucket=true`):
  ```bash
  terraform import 'aws_s3_bucket.data[0]' acme-dw-streaming-xs2026
  ```
  (provider `aws.data` / us-west-2 is used automatically.)
- or set `manage_data_bucket=false` to reference it read-only (no lifecycle/versioning managed here).

## Post-apply (manual, by design)
- **Glue external schema for Spectrum** — run once against the Redshift workgroup:
  ```sql
  CREATE EXTERNAL SCHEMA streaming_reporting
    FROM DATA CATALOG DATABASE 'streaming_reporting'
    IAM_ROLE '<redshift_spectrum role arn>'
    REGION 'us-east-1';
  ```
- **Launch the producer** ad-hoc over SSM (not Airflow):
  ```bash
  aws ssm start-session --target "$(terraform output -raw kafka_instance_id)"
  # then: /opt/streaming/kafka/.venv/bin/python /opt/streaming/kafka/producer.py --generate ...
  ```
- **Airflow `aws_default`** in dev = the `airflow_execution_role_arn` output (or the mounted `~/.aws` user with equivalent perms).

## Cost note
Kafka EC2 + the consumer are always-on (~$15/day data plane per DESIGN.md §5.2).
Redshift Serverless bills per-RPU-second while querying. The Airflow prod stack is
**not** here — spin it up from `airflow-ecs/` only for a demo, then destroy.
