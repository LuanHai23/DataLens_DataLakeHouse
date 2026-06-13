from jobs_normalization.spark.utils.normalization import normalize_location

def test_normalize_ho_chi_minh_variants():
    assert normalize_location("Ho Chi Minh City") == "Ho Chi Minh"
    assert normalize_location("HCM") == "Ho Chi Minh"
    assert normalize_location("Sai Gon") == "Ho Chi Minh"


def test_normalize_ha_noi_variants():
    assert normalize_location("Hà Nội") == "Ha Noi"
    assert normalize_location("Hanoi") == "Ha Noi"
    assert normalize_location("HN") == "Ha Noi"


def test_normalize_da_nang_variants():
    assert normalize_location("Đà Nẵng") == "Da Nang"
    assert normalize_location("Danang") == "Da Nang"


def test_normalize_remote():
    assert normalize_location("Remote") == "Remote"
    assert normalize_location("Work from home") == "Remote"


def test_normalize_unknown_location():
    assert normalize_location("Binh Duong") == "Other"
    assert normalize_location(None) == "Other"
