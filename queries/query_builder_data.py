# Return datasets based on the queries provided

# Map payload column names to actual view column names (view uses aliases)
COLUMN_ALIAS_MAP = {
    "history_of_presenting_complaint": "hpc",
    "administrative_details": "admin_details",
}


def _resolve_column(column: str) -> str:
    """Resolve payload column name to actual view column name."""
    return COLUMN_ALIAS_MAP.get(column, column)


def _build_select_clause(payload: list) -> list[str]:
    """
    Build the SELECT clause from payload: only return columns that are queried.
    """
    seen = set()
    select_parts = []
    for item in payload:
        column = item.get("column")
        if not column:
            continue
        entries = str(item.get("entries", "")).strip()
        if not entries:
            continue
        if column in seen:
            continue
        seen.add(column)
        db_column = _resolve_column(column)
        if db_column != column:
            select_parts.append(f"{db_column} AS {column}")
        else:
            select_parts.append(column)
    return select_parts


def generate_dataset_query(payload: list) -> str:
    """
    Generate a dynamic dataset SELECT query for v_records_safe from payload filters.
    Returns only the columns that are queried in the payload.

    Args:
        payload (list): List of filter dictionaries
    Returns:
        str: Ready-to-run SELECT SQL query
    """
    multi_string_columns = {
        "diagnosis",
        "gender",
        "history_of_presenting_complaint",
        "hpc",
        "investigation",
        "medication",
        "admin_details",
        "administrative_details",
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

    # Build SELECT from payload: only return columns that are queried
    select_parts = _build_select_clause(payload)
    if not select_parts:
        select_parts = ["record_id"]  # fallback if no valid filters
    query = f"SELECT {', '.join(select_parts)} FROM v_records_safe"
    conditions = []

    for item in payload:
        column = item.get("column")
        if not column:
            continue

        # Get entries and skip if empty
        entries = str(item.get("entries", "")).strip()
        if not entries:
            continue

        # Resolve to actual view column name (e.g. history_of_presenting_complaint -> hpc)
        db_column = _resolve_column(column)

        # Determine if exclusion is requested
        exclude = str(item.get("exclude", "")).strip().lower() == "true"

        # Numeric columns
        if column in numeric_columns:
            op = "<" if exclude else ">="
            conditions.append(f"{db_column} {op} {entries}")

        # Date columns
        elif column in date_columns:
            op = "!=" if exclude else "="
            conditions.append(f"{db_column} {op} '{entries}'")

        # Single-value string columns
        elif column in single_string_columns:
            val_safe = entries.replace("'", "''").lower()
            op = "NOT LIKE" if exclude else "LIKE"
            conditions.append(f"LOWER({db_column}) {op} '%{val_safe}%'")

        # Multi-value string columns: include = OR (match any), exclude = AND (match none)
        elif column in multi_string_columns:
            values = [v.strip() for v in entries.split(",") if v.strip()]
            if not values:
                continue
            sub_conditions = []
            for v in values:
                v_safe = v.replace("'", "''").lower()
                op = "NOT LIKE" if exclude else "LIKE"
                sub_conditions.append(f"LOWER({db_column}) {op} '%{v_safe}%'")
            join_op = " AND " if exclude else " OR "
            conditions.append("(" + join_op.join(sub_conditions) + ")")

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY database_id, date_of_visit;"

    return query
