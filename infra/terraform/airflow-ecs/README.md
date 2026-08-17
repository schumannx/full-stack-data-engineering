# infra/terraform/airflow-ecs/ — prod Airflow (apply-on-demand)

The CeleryExecutor Airflow stack on **ECS Fargate**: ALB + webserver, scheduler,
triggerer, worker services, with **RDS Postgres** (metadata + Celery result
backend) and **ElastiCache Redis** (broker) replacing the dev Compose containers
(PLAN_v2.md §5.1).

> ⚠️ **Committed but not left running.** ~$70–100/mo while up. Spin it up for a
> demo, then `terraform destroy`. Dev work uses `airflow/docker-compose.yaml`.

State is separate from the root module. It consumes two root-module outputs:
`airflow_execution_role_arn` (→ `var.airflow_execution_role_arn`, used as the ECS
**task role** so DAGs can call Glue/Lambda/Athena/Redshift).

## Apply flow

```bash
cd infra/terraform/airflow-ecs
export TF_VAR_airflow_admin_password='...'
export TF_VAR_airflow_execution_role_arn="$(terraform -chdir=.. output -raw airflow_execution_role_arn)"

# 1. Create ECR + stores first so we have a repo to push to (the services
#    reference var.airflow_image, so pass a placeholder for the first apply).
terraform init
terraform apply -target=aws_ecr_repository.airflow

# 2. Build & push the image (from repo root airflow/Dockerfile).
ECR=$(terraform output -raw ecr_repository_url)
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin "${ECR%/*}"
docker build -t "$ECR:latest" ../../../airflow
docker push "$ECR:latest"

# 3. Full apply with the real image.
terraform apply -var "airflow_image=$ECR:latest"
```

## One-off DB init (first apply only)

The services assume the metadata DB is migrated and an admin user exists. Run a
one-off task on the cluster using the scheduler task def with a command override:

```bash
CLUSTER=$(terraform output -raw cluster_name)
SUBNET=$(aws ec2 describe-subnets --filters Name=default-for-az,Values=true \
  --query 'Subnets[0].SubnetId' --output text)
SG=$(aws ec2 describe-security-groups --filters Name=group-name,Values=streaming-airflow-svc \
  --query 'SecurityGroups[0].GroupId' --output text)

aws ecs run-task --cluster "$CLUSTER" --launch-type FARGATE \
  --task-definition streaming-airflow-scheduler \
  --network-configuration "awsvpcConfiguration={subnets=[$SUBNET],securityGroups=[$SG],assignPublicIp=ENABLED}" \
  --overrides '{"containerOverrides":[{"name":"scheduler","command":["bash","-c","airflow db migrate && airflow users create --username admin --password '"$TF_VAR_airflow_admin_password"' --firstname a --lastname b --role Admin --email admin@example.com"]}]}'
```

Then open the UI: `terraform output webserver_url`.

## DAGs / connections
This stack runs the image built from `airflow/`, which bakes the DAGs in. The
`aws_default` connection needs no static keys — boto3 picks up the **task role**
(`var.airflow_execution_role_arn`). Set bucket/DPU **Variables** via the UI or
`AIRFLOW_VAR_*` env added to `local.airflow_env`.

## Teardown
```bash
terraform destroy
```
ECR `force_delete` and Secrets Manager `recovery_window_in_days=0` make this clean.
