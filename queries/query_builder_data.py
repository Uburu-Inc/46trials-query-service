# Return datasets based on the queries provided

# Map payload column names to view column names (long form -> short form)
COLUMN_ALIAS_MAP = {
    "history_of_presenting_complaint": "hpc",
    "administrative_details": "admin_details",
}


def _resolve_column(column: str) -> str:
    """Resolve payload column name to actual view column name."""
    return COLUMN_ALIAS_MAP.get(column, column)


def _parse_numeric_entry(value: str, is_exclude: bool) -> tuple[str, str]:
    """
    Parse a numeric filter value; allow operator prefix e.g. '< 18', '>= 18'.
    Returns (sql_operator, numeric_value). Defaults: include -> '>=', exclude -> '<'.
    """
    v = value.strip()
    if not v:
        return ("<" if is_exclude else ">=", "0")
    if v.startswith("<="):
        return ("<=", v[2:].strip())
    if v.startswith(">="):
        return (">=", v[2:].strip())
    if v.startswith("<"):
        return ("<", v[1:].strip())
    if v.startswith(">"):
        return (">", v[1:].strip())
    return ("<" if is_exclude else ">=", v)


def _build_select_clause(payload: list) -> list[str]:
    """
    Build the SELECT clause from payload: return columns that are queried (have entries or exclude).
    """
    seen = set()
    select_parts = []
    for item in payload:
        column = item.get("column")
        if not column:
            continue
        entries = str(item.get("entries", "")).strip()
        exclude = str(item.get("exclude", "")).strip()
        if not entries and not exclude:
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


def _add_conditions_for_value(
    conditions: list,
    db_column: str,
    column: str,
    value_str: str,
    is_exclude: bool,
    multi_string_columns: set,
    single_string_columns: set,
    numeric_columns: set,
    date_columns: set,
) -> None:
    """
    Add WHERE conditions for one value string (entries=include or exclude=exclude).
    """
    values = [v.strip() for v in value_str.split(",") if v.strip()]
    if not values:
        return

    if db_column in numeric_columns:
        for v in values:
            op, num_val = _parse_numeric_entry(v, is_exclude)
            conditions.append(f"{db_column} {op} {num_val}")

    elif db_column in date_columns:
        op = "!=" if is_exclude else "="
        for v in values:
            v_safe = v.replace("'", "''")
            conditions.append(f"{db_column} {op} '{v_safe}'")

    elif db_column in single_string_columns:
        op = "NOT LIKE" if is_exclude else "LIKE"
        for v in values:
            val_safe = v.replace("'", "''").lower()
            conditions.append(f"LOWER({db_column}) {op} '%{val_safe}%'")

    elif db_column in multi_string_columns:
        sub_conditions = []
        op = "NOT LIKE" if is_exclude else "LIKE"
        for v in values:
            v_safe = v.replace("'", "''").lower()
            sub_conditions.append(f"LOWER({db_column}) {op} '%{v_safe}%'")
        join_op = " AND " if is_exclude else " OR "
        conditions.append("(" + join_op.join(sub_conditions) + ")")


def generate_dataset_query(payload: list) -> str:
    """
    Generate a dynamic dataset SELECT query for v_records_safe from payload filters.

    entries = value(s) to INCLUDE (match). exclude = value(s) to EXCLUDE (not match).
    Either or both can be non-empty per item. Returns only columns that are queried.
    """
    multi_string_columns = {
        "diagnosis", "gender", "hpc", "investigation", "medication", "admin_details",
    }
    single_string_columns = set()  # firstname, middlename, lastname no longer used
    numeric_columns = {
        "age_years", "database_id", "record_id", "visit_id",
    }
    date_columns = {"date_of_visit"}

    select_parts = _build_select_clause(payload)
    if not select_parts:
        select_parts = ["record_id"]
    query = f"SELECT {', '.join(select_parts)} FROM v_records_safe"
    conditions = []

    for item in payload:
        column = item.get("column")
        if not column:
            continue

        entries_str = str(item.get("entries", "")).strip()
        exclude_str = str(item.get("exclude", "")).strip()
        if not entries_str and not exclude_str:
            continue

        db_column = _resolve_column(column)

        if entries_str:
            _add_conditions_for_value(
                conditions, db_column, column, entries_str, False,
                multi_string_columns, single_string_columns, numeric_columns, date_columns,
            )
        if exclude_str:
            _add_conditions_for_value(
                conditions, db_column, column, exclude_str, True,
                multi_string_columns, single_string_columns, numeric_columns, date_columns,
            )

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY database_id, date_of_visit;"
    return query
