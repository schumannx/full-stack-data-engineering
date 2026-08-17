"""The Python that the YAML version STILL requires.

Two parts:
  1. A 3-line loader that turns the YAML into DAG objects (this is the only file
     Airflow's scheduler actually imports from the dags folder).
  2. callables/rollup_callables.py — where validate_etl / write_run_metadata had to
     move because YAML can't hold logic.

Compare the TOTAL here against the original daily_rollup.py. You traded ~100 lines of
typed, IDE-navigable Python for: a YAML file + this loader + a callables module — and
the hard part (validate_etl) is unchanged Python, just relocated and untyped at the
YAML boundary. That's the "lateral move" I described.
"""

# ---- part 1: dags/load_yaml.py (the entire loader) -------------------------------
from dagfactory import load_yaml_dags

load_yaml_dags(globals_dict=globals())  # finds *.yml in the dags dir, builds DAGs


# ---- part 2: dags/callables/rollup_callables.py (UNCHANGED logic, just moved) -----
# This is your original validate_etl body, verbatim. YAML pointed at it via
# python_callable_file/python_callable_name. Note: it's now a plain function the
# YAML references by string name — typos here fail at runtime, not import time.
def validate_etl(ds=None, **context):
    import sys
    from airflow.providers.amazon.aws.hooks.athena import AthenaHook
    from airflow.models import Variable

    sys.path.insert(0, "/opt/airflow/common")
    import validation  # the mounted ../common/validation.py

    hook = AthenaHook(aws_conn_id="aws_default", region_name="us-east-1")
    results_loc = Variable.get(
        "STREAMING_ATHENA_RESULTS",
        default_var="s3://acme-dw-streaming-xs2026-use1-ops/athena-results/validate/",
    )

    def run_query(sql: str):
        qid = hook.run_query(
            sql,
            query_context={"Database": "streaming_processed"},
            result_configuration={"OutputLocation": results_loc},
        )
        hook.poll_query_status(qid)
        res = hook.get_query_results(qid)
        rows = res["ResultSet"]["Rows"]
        header = [c["VarCharValue"] for c in rows[0]["Data"]]
        return [
            {header[i]: cell.get("VarCharValue") for i, cell in enumerate(r["Data"])}
            for r in rows[1:]
        ]

    validation.assert_all(run_query)
    return {"ds": ds, "assertions_passed": True}


def write_run_metadata(**context):
    return context.get("ti").xcom_pull(task_ids="validate_etl")
