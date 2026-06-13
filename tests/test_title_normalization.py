from jobs_normalization.spark.utils.normalization import normalize_title


def test_normalize_title_trim_spaces():
    assert normalize_title("  Data Engineer  ") == "Data Engineer"


def test_normalize_title_collapse_multiple_spaces():
    assert normalize_title("Senior   Data    Engineer") == "Senior Data Engineer"


def test_normalize_title_remove_newlines_and_tabs():
    assert normalize_title("Data\nEngineer\tIntern") == "Data Engineer Intern"


def test_normalize_title_empty_when_none():
    assert normalize_title(None) == ""
