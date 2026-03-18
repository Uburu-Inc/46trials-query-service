# Return counts based on the queries provided

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


def _add_conditions_for_value(
    conditions: list,
    db_column: str,
    column: str,
    value_str: str,
    is_exclude: bool,
    multi_string_columns: set,
    single_string_columns: set,
    exact_match_string_columns: set,
    numeric_columns: set,
    date_columns: set,
) -> None:
    """
    Add WHERE conditions for one value string (either entries=include or exclude=exclude).
    value_str: comma-separated values; is_exclude: True for exclude, False for include.
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

    elif db_column in exact_match_string_columns:
        # Exact match, value as-is (e.g. gender = 'Female' or gender = 'Male')
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


def _add_date_range_conditions(conditions: list, limit: dict | None) -> None:
    """
    Add visit_year >= startDate and visit_year <= endDate from limit if present.
    startDate/endDate are expected to be year strings like '1999', '2002'.
    """
    if not limit:
        return
    start_raw = (limit.get("startDate") or "").strip()
    end_raw = (limit.get("endDate") or "").strip()

    if start_raw:
        try:
            start_year = int(start_raw)
            conditions.append(f"visit_year >= {start_year}")
        except ValueError:
            pass
    if end_raw:
        try:
            end_year = int(end_raw)
            conditions.append(f"visit_year <= {end_year}")
        except ValueError:
            pass


def generate_count_query(payload: list, limit: dict | None = None) -> str:
    """
    Generate a dynamic COUNT query for v_records_safe from payload filters.

    entries = value(s) to INCLUDE (match): e.g. "sickle cell" -> LIKE '%sickle cell%'
    exclude = value(s) to EXCLUDE (not match): e.g. "emergency" -> NOT LIKE '%emergency%'

    limit (optional): { "startDate": \"1999\", \"endDate\": \"2002\", \"sampleSize\": n }
    startDate/endDate add a visit_year range; sampleSize is ignored for count.
    """
    multi_string_columns = {
        "diagnosis", "hpc", "investigation", "medication", "admin_details",
    }
    single_string_columns = set()  # firstname, middlename, lastname no longer used
    exact_match_string_columns = {"gender"}  # Male/Female, exact match, no lowercasing
    numeric_columns = {
        "age_years", "database_id", "record_id", "visit_id",
    }
    date_columns = {"date_of_visit"}

    query = "SELECT COUNT(DISTINCT visit_id) AS eligible_count FROM v_records_safe"
    conditions = []

    # First pass: all include (entries) conditions, in payload order
    for item in payload:
        column = item.get("column")
        if not column:
            continue
        entries_str = str(item.get("entries", "")).strip()
        if not entries_str:
            continue
        db_column = _resolve_column(column)
        _add_conditions_for_value(
            conditions, db_column, column, entries_str, False,
            multi_string_columns, single_string_columns, exact_match_string_columns,
            numeric_columns, date_columns,
        )

    # Second pass: all exclude conditions, in payload order
    for item in payload:
        column = item.get("column")
        if not column:
            continue
        exclude_str = str(item.get("exclude", "")).strip()
        if not exclude_str:
            continue
        db_column = _resolve_column(column)
        _add_conditions_for_value(
            conditions, db_column, column, exclude_str, True,
            multi_string_columns, single_string_columns, exact_match_string_columns,
            numeric_columns, date_columns,
        )

    _add_date_range_conditions(conditions, limit)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    return query
