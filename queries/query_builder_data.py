# Return datasets based on the queries provided


def generate_dataset_query(payload: list) -> str:
    """
    Generate a dynamic dataset SELECT query for v_records_safe from payload filters.
    'isSelected' is ignored — any column with 'entries' will be included.

    Args:
        payload (list): List of filter dictionaries
    Returns:
        str: Ready-to-run SELECT SQL query
    """
    select_columns = [
        "record_id",
        "visit_id",
        "database_id",
        "date_of_visit",
        "age_years",
        "gender",
        "diagnosis",
        "hpc",
        "investigation",
        "medication",
        "session_id",
    ]

    multi_string_columns = {
        "diagnosis",
        "gender",
        "history_of_presenting_complaint",
        "investigation",
        "medication",
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

    query = f"SELECT {', '.join(select_columns)} FROM v_records_safe"
    conditions = []

    for item in payload:
        column = item.get("column")
        if not column:
            continue

        # Get entries and skip if empty
        entries = str(item.get("entries", "")).strip()
        if not entries:
            continue

        # Determine if exclusion is requested
        exclude = str(item.get("exclude", "")).strip().lower() == "true"

        # Numeric columns
        if column in numeric_columns:
            op = "<" if exclude else ">="
            conditions.append(f"{column} {op} {entries}")

        # Date columns
        elif column in date_columns:
            op = "!=" if exclude else "="
            conditions.append(f"{column} {op} '{entries}'")

        # Single-value string columns
        elif column in single_string_columns:
            val_safe = entries.replace("'", "''").lower()
            op = "NOT LIKE" if exclude else "LIKE"
            conditions.append(f"LOWER({column}) {op} '%{val_safe}%'")

        # Multi-value string columns
        elif column in multi_string_columns:
            values = [v.strip() for v in entries.split(",") if v.strip()]
            if not values:
                continue
            sub_conditions = []
            for v in values:
                v_safe = v.replace("'", "''").lower()
                op = "NOT LIKE" if exclude else "LIKE"
                sub_conditions.append(f"LOWER({column}) {op} '%{v_safe}%'")
            conditions.append("(" + " OR ".join(sub_conditions) + ")")

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY database_id, date_of_visit;"

    return query
