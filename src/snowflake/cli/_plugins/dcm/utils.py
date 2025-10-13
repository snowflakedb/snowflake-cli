def format_refresh_results(refreshed_tables: list) -> str:
    """Format refresh results into a concise user-friendly message."""
    if not refreshed_tables:
        return "No dynamic tables found in the project."

    total_tables = len(refreshed_tables)
    refreshed_count = sum(
        1 for table in refreshed_tables if table.get("refreshed_dt_count", 0) > 0
    )
    up_to_date_count = total_tables - refreshed_count

    return f"{refreshed_count} dynamic table(s) refreshed. {up_to_date_count} dynamic table(s) up-to-date."
