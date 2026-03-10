# Return counts based on the queries provided

def generate_count_query(payload: list) -> str:
    """
    Generate a dynamic COUNT query for v_records_safe from payload filters.
    'isSelected' is ignored — the query only uses 'entries' and 'exclude'.

    Args:
        payload (list): List of filter dictionaries, e.g.,
            [
                {"column": "diagnosis", "entries": "malaria,fever", "exclude": ""},
                {"column": "age_years", "entries": "18", "exclude": ""}
            ]
    Returns:
        str: Ready-to-run SQL COUNT query
    """

    # Columns classified by type
    multi_string_columns = {
        "diagnosis",
        "gender",
        "history_of_presenting_complaint",
        "investigation",
        "medication",
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

        # Determine if the filter is meant to exclude matching rows
        exclude = str(item.get("exclude", "")).strip().lower() == "true"

        # Numeric columns: use >= by default, < if exclude is True
        if column in numeric_columns:
            op = "<" if exclude else ">="
            conditions.append(f"{column} {op} {entries}")

        # Date columns: use = by default, != if exclude is True
        elif column in date_columns:
            op = "!=" if exclude else "="
            conditions.append(f"{column} {op} '{entries}'")

        # Single-value string columns (e.g., firstname, lastname)
        elif column in single_string_columns:
            val_safe = entries.replace("'", "''").lower()  # Escape single quotes
            op = "NOT LIKE" if exclude else "LIKE"
            conditions.append(f"LOWER({column}) {op} '%{val_safe}%'")

        # Multi-value string columns (e.g., diagnosis, gender)
        elif column in multi_string_columns:
            # Split comma-separated values and skip empty ones
            values = [v.strip() for v in entries.split(",") if v.strip()]
            if not values:
                continue
            sub_conditions = []
            for v in values:
                v_safe = v.replace("'", "''").lower()  # Escape single quotes
                op = "NOT LIKE" if exclude else "LIKE"
                sub_conditions.append(f"LOWER({column}) {op} '%{v_safe}%'")
            # Combine multiple values with OR inside parentheses
            conditions.append("(" + " OR ".join(sub_conditions) + ")")

    # Combine all conditions with AND
    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    return query
