# Return counts based on the queries provided

# Map payload column names to actual view column names (view uses aliases)
COLUMN_ALIAS_MAP = {
    "history_of_presenting_complaint": "hpc",
    "administrative_details": "admin_details",
}


def _resolve_column(column: str) -> str:
    """Resolve payload column name to actual view column name."""
    return COLUMN_ALIAS_MAP.get(column, column)


def generate_count_query(payload: list) -> str:
    """
    Generate a dynamic COUNT query for v_records_safe from payload filters.
    Only applies filters where isSelected is True (or entries is non-empty).

    Args:
        payload (list): List of filter dictionaries, e.g.,
            [
                {"column": "diagnosis", "entries": "malaria,fever", "exclude": ""},
                {"column": "age_years", "entries": "18", "exclude": ""}
            ]
    Returns:
        str: Ready-to-run SQL COUNT query
    """

    # Columns classified by type (use view column names: hpc, admin_details)
    multi_string_columns = {
        "diagnosis",
        "gender",
        "history_of_presenting_complaint",
        "hpc",
        "investigation",
        "medication",
        "administrative_details",
        "admin_details",
    }

    single_string_columns = {"firstname", "middlename", "lastname"}

    numeric_columns = {
        "age_years",
        "database_id",
        "record_id",
        "visit_id",
        "patient_folder_id",
        "batch_id",
        "session_id",
    }

    date_columns = {"date_of_visit"}

    # Base COUNT query
    query = "SELECT COUNT(DISTINCT visit_id) AS eligible_count FROM v_records_safe"
    conditions = []

    # Loop through payload filters and build WHERE conditions
    for item in payload:
        column = item.get("column")
        if not column:
            continue  # Skip if no column specified

        # Get filter value; skip if empty
        entries = str(item.get("entries", "")).strip()
        if not entries:
            continue

        # Resolve to actual view column name (e.g. history_of_presenting_complaint -> hpc)
        db_column = _resolve_column(column)

        # Determine if the filter is meant to exclude matching rows
        exclude = str(item.get("exclude", "")).strip().lower() == "true"

        # Numeric columns: use >= by default, < if exclude is True
        if column in numeric_columns:
            op = "<" if exclude else ">="
            conditions.append(f"{db_column} {op} {entries}")

        # Date columns: use = by default, != if exclude is True
        elif column in date_columns:
            op = "!=" if exclude else "="
            conditions.append(f"{db_column} {op} '{entries}'")

        # Single-value string columns (e.g., firstname, lastname)
        elif column in single_string_columns:
            val_safe = entries.replace("'", "''").lower()  # Escape single quotes
            op = "NOT LIKE" if exclude else "LIKE"
            conditions.append(f"LOWER({db_column}) {op} '%{val_safe}%'")

        # Multi-value string columns: include = OR (match any), exclude = AND (match none)
        elif column in multi_string_columns:
            values = [v.strip() for v in entries.split(",") if v.strip()]
            if not values:
                continue
            sub_conditions = []
            for v in values:
                v_safe = v.replace("'", "''").lower()  # Escape single quotes
                op = "NOT LIKE" if exclude else "LIKE"
                sub_conditions.append(f"LOWER({db_column}) {op} '%{v_safe}%'")
            join_op = " AND " if exclude else " OR "
            conditions.append("(" + join_op.join(sub_conditions) + ")")

    # Combine all conditions with AND
    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    return query
