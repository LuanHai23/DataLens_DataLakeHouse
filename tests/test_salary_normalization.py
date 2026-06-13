from jobs_normalization.spark.utils.normalization import parse_salary


def test_parse_usd_salary_range():
    assert parse_salary("$1,500 - $3,000") == (1500.0, 3000.0, "USD")


def test_parse_vnd_salary_range():
    assert parse_salary("20,000,000 - 35,000,000 VND") == (20_000_000.0, 35_000_000.0, "VND")


def test_parse_vnd_million_salary_range():
    assert parse_salary("20 - 35 triệu") == (20_000_000.0, 35_000_000.0, "VND")


def test_parse_up_to_usd_salary():
    assert parse_salary("Up to $2,000") == (None, 2000.0, "USD")


def test_parse_from_usd_salary():
    assert parse_salary("From $1,000") == (1000.0, None, "USD")


def test_parse_negotiable_salary():
    assert parse_salary("Thỏa thuận") == (None, None, "UNKNOWN")


def test_parse_hidden_salary():
    assert parse_salary("Login to view") == (None, None, "UNKNOWN")


def test_parse_missing_salary():
    assert parse_salary(None) == (None, None, "UNKNOWN")
