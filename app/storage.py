"""Global storage for the last analysis result."""

last_result: dict | None = None


def store_last_result(result: dict) -> None:
    """Store the last analysis result."""
    global last_result
    last_result = result


def get_last_result() -> dict | None:
    """Retrieve the last analysis result."""
    return last_result
