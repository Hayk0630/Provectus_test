import pandas as pd

from src.validation import validate_batches, validate_employees


def test_validate_employees_accepts_valid_input() -> None:
    employees = pd.DataFrame(
        {
            "email": ["a@example.com", "b@example.com"],
            "full_name": ["A A", "B B"],
            "practice": ["Backend Engineering", "Frontend Engineering"],
            "level": ["L4", "L5"],
            "location": ["US", "PL"],
        }
    )

    issues = validate_employees(employees)
    assert not [i for i in issues if i.startswith("ERROR:")]


def test_validate_employees_detects_duplicates() -> None:
    employees = pd.DataFrame(
        {
            "email": ["dup@example.com", "dup@example.com"],
            "full_name": ["A A", "B B"],
            "practice": ["Backend Engineering", "Frontend Engineering"],
            "level": ["L4", "L5"],
            "location": ["US", "PL"],
        }
    )

    issues = validate_employees(employees)
    assert any("duplicate emails" in issue for issue in issues)


def test_validate_batches_detects_non_list_logevents() -> None:
    batches = pd.DataFrame(
        {
            "messageType": ["DATA_MESSAGE"],
            "owner": ["123"],
            "logGroup": ["/group"],
            "logStream": ["stream"],
            "subscriptionFilters": [["logs-to-s3"]],
            "logEvents": ["not_a_list"],
            "year": [2026],
            "month": [1],
            "day": [1],
        }
    )

    issues = validate_batches(batches)
    assert any("logEvents is not a list" in issue for issue in issues)
