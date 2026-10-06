"""Classify safe TopCV page outcomes without logging page contents."""


def classify_topcv_page(
    *,
    status_code,
    final_url,
    title,
    body_text,
    job_card_count,
    job_link_count,
):
    """Return a stable, non-sensitive classification for diagnostics."""
    if job_card_count > 0:
        return "jobs_visible"

    combined_text = f"{title or ''}\n{body_text or ''}".lower()
    normalized_url = str(final_url or "").lower()

    cloudflare_markers = (
        "attention required",
        "sorry, you have been blocked",
        "cloudflare ray id",
        "error 1020",
        "cf-error",
    )
    if any(marker in combined_text for marker in cloudflare_markers):
        return "cloudflare_block"

    if status_code == 429:
        return "rate_limited"
    if status_code == 403:
        return "access_denied"

    challenge_markers = (
        "verify you are human",
        "checking your browser",
        "just a moment",
        "performing security verification",
        "turnstile",
        "captcha",
    )
    if any(marker in combined_text for marker in challenge_markers):
        return "anti_bot_challenge"

    if "dang-nhap" in normalized_url or "/login" in normalized_url:
        return "login_redirect"

    if job_link_count > 0:
        return "selector_drift"

    return "empty_unknown"
