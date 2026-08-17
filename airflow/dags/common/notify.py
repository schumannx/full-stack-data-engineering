"""SNS-backed alerting for the Streaming DW DAGs.

`notify_failure` is wired into DEFAULT_ARGS (common/config.py) so ANY task failure
emails the team via SNS. `sns_alert` is the generic publisher (used by the daily
dq_scorecard too).

The topic ARN comes from env STREAMING_ALERT_TOPIC_ARN (set from the Terraform
output: AIRFLOW_VAR_/env in compose, or the EC2 .env). If it's unset, alerts are
a no-op (logged) — so this is safe before the SNS topic exists.
"""

from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)


def sns_alert(subject: str, message: str) -> None:
    arn = os.environ.get("STREAMING_ALERT_TOPIC_ARN")
    if not arn:
        log.warning("STREAMING_ALERT_TOPIC_ARN unset; skipping alert: %s", subject)
        return
    import boto3  # lazy: keeps DAG parse light

    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    boto3.client("sns", region_name=region).publish(
        TopicArn=arn, Subject=subject[:100], Message=message
    )


def notify_failure(context) -> None:
    """on_failure_callback: email the failed task's details."""
    ti = context.get("task_instance")
    dag_id = getattr(context.get("dag"), "dag_id", "?")
    task_id = getattr(ti, "task_id", "?")
    subject = f"[Airflow FAILED] {dag_id}.{task_id}"
    message = (
        f"DAG:   {dag_id}\n"
        f"Task:  {task_id}\n"
        f"Run:   {context.get('run_id')}\n"
        f"When:  {context.get('logical_date')}\n"
        f"Error: {context.get('exception')}\n"
        f"Log:   {getattr(ti, 'log_url', '')}\n"
    )
    try:
        sns_alert(subject, message)
    except Exception:  # never let alerting failure mask the real failure
        log.exception("failed to publish failure alert")
