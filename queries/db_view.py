# Generate view once query

def generate_db_view():
    return """
      DROP VIEW IF EXISTS v_records_safe;

      CREATE VIEW v_records_safe AS
      SELECT
        record_id,
        visit_id,
        database_id,
        session_id,
        source_pages,
        pages_merged,
        date_of_visit,
        diagnosis,
        history_of_presenting_complaint  AS hpc,
        investigation,
        medication,
        administrative_details           AS admin_details,

        -- Extract age from bio_data (e.g. "Age: 19 years")
        CASE
          WHEN INSTR(bio_data, 'Age: ') = 0 THEN NULL
          ELSE CAST(
            SUBSTR(
              bio_data,
              INSTR(bio_data, 'Age: ') + 5,
              INSTR(
                SUBSTR(bio_data, INSTR(bio_data, 'Age: ') + 5),
                char(10)
              ) - 1
            )
          AS INTEGER)
        END AS age_years,

        -- Extract gender from bio_data (e.g. "Gender: Female")
        CASE
          WHEN INSTR(LOWER(bio_data), 'gender: female') > 0 THEN 'Female'
          WHEN INSTR(LOWER(bio_data), 'gender: male')   > 0 THEN 'Male'
          ELSE 'Unknown'
        END AS gender

      FROM medical_records;
    """
