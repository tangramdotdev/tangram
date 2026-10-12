def require_json(content_type: str | None) -> None:
    content_type = (content_type or "").split(";", 1)[0].strip().lower()
    if content_type.startswith(
        "application/vnd.tangram."
    ) and not content_type.endswith("+json"):
        raise ValueError(
            "the client does not support Tangram body prefix serialization"
        )
