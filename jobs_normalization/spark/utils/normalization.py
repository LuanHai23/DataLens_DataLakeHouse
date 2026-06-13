import re
import unicodedata
from typing import Optional, Tuple


NEGOTIABLE_PATTERNS = (
    "thỏa thuận",
    "thoa thuan",
    "negotiable",
    "hidden",
    "login to view",
    "không hiển thị",
    "khong hien thi",
    "salary not shown",
    "competitive",
    "you'll love it",
)


def _to_ascii_lower(value: Optional[str]) -> str:
    if value is None:
        return ""

    text = str(value)

    # Vietnamese special character fix
    text = text.replace("Đ", "D").replace("đ", "d")

    normalized = unicodedata.normalize("NFKD", text)
    ascii_text = "".join(ch for ch in normalized if not unicodedata.combining(ch))

    return ascii_text.lower().strip()


def _normalize_number_token(token: str) -> Optional[float]:
    """
    Convert salary number tokens into numeric values.

    Supported examples:
    - "1,500" -> 1500
    - "20,000,000" -> 20000000
    - "20.000.000" -> 20000000
    - "1.5" -> 1.5
    - "1,5" -> 1.5
    """
    if not token:
        return None

    token = token.strip()

    # Both separators exist: assume commas are thousands, dot is decimal.
    if "," in token and "." in token:
        token = token.replace(",", "")
        try:
            return float(token)
        except ValueError:
            return None

    # Multiple commas or dots usually mean thousands separators.
    if token.count(",") > 1:
        token = token.replace(",", "")
    elif token.count(".") > 1:
        token = token.replace(".", "")
    elif "," in token:
        left, right = token.split(",", 1)
        if len(right) == 3 and len(left) <= 3:
            token = left + right
        else:
            token = left + "." + right
    elif "." in token:
        left, right = token.split(".", 1)
        if len(right) == 3 and len(left) <= 3:
            token = left + right

    try:
        return float(token)
    except ValueError:
        return None


def parse_salary(salary_raw: Optional[str]) -> Tuple[Optional[float], Optional[float], str]:
    """
    Parse raw salary text into min_salary, max_salary, currency.

    Returns:
        (min_salary, max_salary, currency)
    """
    raw = "" if salary_raw is None else str(salary_raw).strip()
    lower = _to_ascii_lower(raw)

    if not lower or any(pattern in lower for pattern in NEGOTIABLE_PATTERNS):
        return None, None, "UNKNOWN"

    if "$" in raw or "usd" in lower:
        currency = "USD"
    elif any(pattern in lower for pattern in ["vnd", "vnd", "₫", "trieu", "tr ", " tr", "million"]):
        currency = "VND"
    else:
        currency = "UNKNOWN"

    multiplier = 1.0
    if currency == "VND":
        if re.search(r"\b(trieu|tr|million)\b", lower):
            multiplier = 1_000_000.0
        elif re.search(r"\b(nghin|k|thousand)\b", lower):
            multiplier = 1_000.0

    number_tokens = re.findall(r"\d+(?:[,.]\d+)*", lower)
    values = [_normalize_number_token(token) for token in number_tokens]
    values = [value * multiplier for value in values if value is not None]

    if not values:
        return None, None, currency

    first = values[0]
    second = values[1] if len(values) > 1 else None

    upper_bound_patterns = ("up to", "upto", "len den", "toi da", "tối đa")
    lower_bound_patterns = ("from", "starting from", "tu ")

    if any(pattern in lower for pattern in upper_bound_patterns):
        return None, first, currency

    if any(pattern in lower for pattern in lower_bound_patterns):
        return first, None, currency

    if second is not None:
        min_salary = min(first, second)
        max_salary = max(first, second)
        return min_salary, max_salary, currency

    return first, first, currency


def normalize_location(location_raw: Optional[str]) -> str:
    """Normalize raw location text into dashboard-friendly categories."""
    lower = _to_ascii_lower(location_raw)

    if not lower:
        return "Other"

    if re.search(r"ho chi minh|hcm|saigon|sai gon", lower):
        return "Ho Chi Minh"

    if re.search(r"ha noi|hanoi|\bhn\b", lower):
        return "Ha Noi"

    if re.search(r"da nang|danang", lower):
        return "Da Nang"

    if "remote" in lower or "work from home" in lower:
        return "Remote"

    return "Other"


def normalize_title(title_raw: Optional[str]) -> str:
    """
    Normalize job title for deduplication, search, and display.

    This keeps original capitalization as much as possible, but removes
    noisy whitespace and common line breaks.
    """
    if title_raw is None:
        return ""

    title = str(title_raw)
    title = re.sub(r"[\r\n\t]+", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title
