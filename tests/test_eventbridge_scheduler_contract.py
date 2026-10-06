import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

SCHEDULE_FILE = (
    ROOT
    / "infra"
    / "aws"
    / "scheduler"
    / "vnjobs-data-pipeline-daily-dev.schedule.json"
)
TRUST_FILE = (
    ROOT
    / "infra"
    / "aws"
    / "iam"
    / "vnjobs-eventbridge-scheduler-role-dev.trust.json"
)
PERMISSION_FILE = (
    ROOT
    / "infra"
    / "aws"
    / "iam"
    / "vnjobs-eventbridge-scheduler-role-dev.policy.json"
)
WORKFLOW_FILE = (
    ROOT
    / "infra"
    / "aws"
    / "stepfunctions"
    / "vnjobs-data-pipeline-dev.asl.json"
)

ACCOUNT_ID = "490258149044"
REGION = "ap-southeast-1"

STATE_MACHINE_ARN = (
    f"arn:aws:states:{REGION}:{ACCOUNT_ID}:"
    "stateMachine:vnjobs-data-pipeline-dev"
)
SCHEDULER_ROLE_ARN = (
    f"arn:aws:iam::{ACCOUNT_ID}:"
    "role/vnjobs-eventbridge-scheduler-role-dev"
)
SCHEDULE_GROUP_ARN = (
    f"arn:aws:scheduler:{REGION}:{ACCOUNT_ID}:"
    "schedule-group/vnjobs-data-pipeline-dev"
)


def _load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _as_list(value):
    if isinstance(value, list):
        return value
    return [value]


def test_daily_schedule_contract():
    raw_schedule = SCHEDULE_FILE.read_text(encoding="utf-8")

    assert r"\u003c" not in raw_schedule.lower()

    schedule = json.loads(raw_schedule)

    assert schedule["Name"] == "vnjobs-data-pipeline-daily-dev"
    assert schedule["GroupName"] == "vnjobs-data-pipeline-dev"
    assert schedule["State"] == "DISABLED"

    assert schedule["ScheduleExpression"] == "cron(0 8 * * ? *)"
    assert (
        schedule["ScheduleExpressionTimezone"]
        == "Asia/Ho_Chi_Minh"
    )
    assert schedule["FlexibleTimeWindow"] == {"Mode": "OFF"}

    target = schedule["Target"]

    assert target["Arn"] == STATE_MACHINE_ARN
    assert target["RoleArn"] == SCHEDULER_ROLE_ARN
    assert target["RetryPolicy"] == {
        "MaximumEventAgeInSeconds": 3600,
        "MaximumRetryAttempts": 2,
    }

    target_input = json.loads(target["Input"])

    assert target_input == {
        "scheduled_time": "<aws.scheduler.scheduled-time>",
        "scheduler_execution_id": "<aws.scheduler.execution-id>",
    }


def test_scheduler_role_trust_is_scoped():
    trust = _load_json(TRUST_FILE)
    statements = _as_list(trust["Statement"])

    scheduler_statements = []

    for statement in statements:
        principal = statement.get("Principal", {})
        services = _as_list(principal.get("Service", []))
        actions = _as_list(statement.get("Action", []))

        if (
            statement.get("Effect") == "Allow"
            and "scheduler.amazonaws.com" in services
            and "sts:AssumeRole" in actions
        ):
            scheduler_statements.append(statement)

    assert len(scheduler_statements) == 1

    conditions = {}

    for condition_mapping in scheduler_statements[0].get(
        "Condition", {}
    ).values():
        if isinstance(condition_mapping, dict):
            conditions.update(condition_mapping)

    assert conditions["aws:SourceAccount"] == ACCOUNT_ID
    assert conditions["aws:SourceArn"] == SCHEDULE_GROUP_ARN


def test_scheduler_permission_is_least_privilege():
    policy = _load_json(PERMISSION_FILE)
    statements = _as_list(policy["Statement"])

    allowed_actions = set()
    allowed_resources = set()

    for statement in statements:
        if statement.get("Effect") != "Allow":
            continue

        allowed_actions.update(
            _as_list(statement.get("Action", []))
        )
        allowed_resources.update(
            _as_list(statement.get("Resource", []))
        )

    assert allowed_actions == {"states:StartExecution"}
    assert allowed_resources == {STATE_MACHINE_ARN}


def test_schedule_matches_step_functions_input_contract():
    workflow = _load_json(WORKFLOW_FILE)
    states = workflow["States"]

    assert workflow["StartAt"] == "Resolve Batch Date"
    assert len(states) == 21

    resolve_state = states["Resolve Batch Date"]
    derive_state = states["Derive Scheduled Batch Date"]

    assert resolve_state["Type"] == "Choice"
    assert derive_state["Type"] == "Pass"
    assert derive_state["Next"] == "Validate Input"

    routes = {
        choice["Next"]
        for choice in resolve_state.get("Choices", [])
    }

    if resolve_state.get("Default"):
        routes.add(resolve_state["Default"])

    assert "Validate Input" in routes
    assert "Derive Scheduled Batch Date" in routes

    derive_json = json.dumps(
        derive_state,
        ensure_ascii=False,
        sort_keys=True,
    )

    assert "$.scheduled_time" in derive_json
    assert "States.StringSplit" in derive_json
    assert "States.ArrayGetItem" in derive_json
    assert "eventbridge-scheduler" in derive_json
