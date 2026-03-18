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

  -- Extract age from bio_data
  CASE
    WHEN INSTR(bio_data, 'Age: ') = 0 THEN NULL
    ELSE CAST(
      SUBSTR(
        bio_data,
        INSTR(bio_data, 'Age: ') + 5,
        INSTR(SUBSTR(bio_data, INSTR(bio_data, 'Age: ') + 5), char(10)) - 1
      )
    AS INTEGER)
  END AS age_years,

  -- Extract gender from bio_data
  CASE
    WHEN INSTR(LOWER(bio_data), 'gender: female') > 0 THEN 'Female'
    WHEN INSTR(LOWER(bio_data), 'gender: male')   > 0 THEN 'Male'
    ELSE 'Unknown'
  END AS gender,

  -- Extract visit_year cleanly
  -- Step 1: isolate first line only (before any char(10) newline)
  -- Step 2: extract what comes after the second slash
  -- Step 3: convert 2-digit years to 4-digit, handle 4-digit as-is
  CASE
    WHEN INSTR(date_of_visit, 'Visit Date: ') = 0 THEN NULL
    ELSE
      CASE
        WHEN CAST(
          SUBSTR(
            CASE
              WHEN INSTR(date_of_visit, char(10)) > 0
              THEN SUBSTR(date_of_visit, 1, INSTR(date_of_visit, char(10)) - 1)
              ELSE date_of_visit
            END,
            INSTR(date_of_visit, 'Visit Date: ') + 12 +
            INSTR(
              SUBSTR(
                CASE
                  WHEN INSTR(date_of_visit, char(10)) > 0
                  THEN SUBSTR(date_of_visit, 1, INSTR(date_of_visit, char(10)) - 1)
                  ELSE date_of_visit
                END,
                INSTR(date_of_visit, 'Visit Date: ') + 12
              ), '/'
            ) +
            INSTR(
              SUBSTR(
                CASE
                  WHEN INSTR(date_of_visit, char(10)) > 0
                  THEN SUBSTR(date_of_visit, 1, INSTR(date_of_visit, char(10)) - 1)
                  ELSE date_of_visit
                END,
                INSTR(date_of_visit, 'Visit Date: ') + 12 +
                INSTR(
                  SUBSTR(
                    CASE
                      WHEN INSTR(date_of_visit, char(10)) > 0
                      THEN SUBSTR(date_of_visit, 1, INSTR(date_of_visit, char(10)) - 1)
                      ELSE date_of_visit
                    END,
                    INSTR(date_of_visit, 'Visit Date: ') + 12
                  ), '/'
                )
              ), '/'
            ),
            4
          )
        AS INTEGER) > 1900
        THEN CAST(
          SUBSTR(
            CASE
              WHEN INSTR(date_of_visit, char(10)) > 0
              THEN SUBSTR(date_of_visit, 1, INSTR(date_of_visit, char(10)) - 1)
              ELSE date_of_visit
            END,
            INSTR(date_of_visit, 'Visit Date: ') + 12 +
            INSTR(SUBSTR(
              CASE
                WHEN INSTR(date_of_visit, char(10)) > 0
                THEN SUBSTR(date_of_visit, 1, INSTR(date_of_visit, char(10)) - 1)
                ELSE date_of_visit
              END,
              INSTR(date_of_visit, 'Visit Date: ') + 12), '/'
            ) +
            INSTR(SUBSTR(
              CASE
                WHEN INSTR(date_of_visit, char(10)) > 0
                THEN SUBSTR(date_of_visit, 1, INSTR(date_of_visit, char(10)) - 1)
                ELSE date_of_visit
              END,
              INSTR(date_of_visit, 'Visit Date: ') + 12 +
              INSTR(SUBSTR(
                CASE
                  WHEN INSTR(date_of_visit, char(10)) > 0
                  THEN SUBSTR(date_of_visit, 1, INSTR(date_of_visit, char(10)) - 1)
                  ELSE date_of_visit
                END,
                INSTR(date_of_visit, 'Visit Date: ') + 12), '/')
            ), '/'),
            4
          )
        AS INTEGER)
        ELSE
          CASE
            WHEN CAST(SUBSTR(
              CASE
                WHEN INSTR(date_of_visit, char(10)) > 0
                THEN SUBSTR(date_of_visit, 1, INSTR(date_of_visit, char(10)) - 1)
                ELSE date_of_visit
              END,
              INSTR(date_of_visit, 'Visit Date: ') + 12 +
              INSTR(SUBSTR(
                CASE
                  WHEN INSTR(date_of_visit, char(10)) > 0
                  THEN SUBSTR(date_of_visit, 1, INSTR(date_of_visit, char(10)) - 1)
                  ELSE date_of_visit
                END,
                INSTR(date_of_visit, 'Visit Date: ') + 12), '/'
              ) +
              INSTR(SUBSTR(
                CASE
                  WHEN INSTR(date_of_visit, char(10)) > 0
                  THEN SUBSTR(date_of_visit, 1, INSTR(date_of_visit, char(10)) - 1)
                  ELSE date_of_visit
                END,
                INSTR(date_of_visit, 'Visit Date: ') + 12 +
                INSTR(SUBSTR(
                  CASE
                    WHEN INSTR(date_of_visit, char(10)) > 0
                    THEN SUBSTR(date_of_visit, 1, INSTR(date_of_visit, char(10)) - 1)
                    ELSE date_of_visit
                  END,
                  INSTR(date_of_visit, 'Visit Date: ') + 12), '/')
              ), '/'),
              2
            ) AS INTEGER) >= 90
            THEN 1900 + CAST(SUBSTR(
              CASE
                WHEN INSTR(date_of_visit, char(10)) > 0
                THEN SUBSTR(date_of_visit, 1, INSTR(date_of_visit, char(10)) - 1)
                ELSE date_of_visit
              END,
              INSTR(date_of_visit, 'Visit Date: ') + 12 +
              INSTR(SUBSTR(
                CASE
                  WHEN INSTR(date_of_visit, char(10)) > 0
                  THEN SUBSTR(date_of_visit, 1, INSTR(date_of_visit, char(10)) - 1)
                  ELSE date_of_visit
                END,
                INSTR(date_of_visit, 'Visit Date: ') + 12), '/'
              ) +
              INSTR(SUBSTR(
                CASE
                  WHEN INSTR(date_of_visit, char(10)) > 0
                  THEN SUBSTR(date_of_visit, 1, INSTR(date_of_visit, char(10)) - 1)
                  ELSE date_of_visit
                END,
                INSTR(date_of_visit, 'Visit Date: ') + 12 +
                INSTR(SUBSTR(
                  CASE
                    WHEN INSTR(date_of_visit, char(10)) > 0
                    THEN SUBSTR(date_of_visit, 1, INSTR(date_of_visit, char(10)) - 1)
                    ELSE date_of_visit
                  END,
                  INSTR(date_of_visit, 'Visit Date: ') + 12), '/')
              ), '/'),
              2
            ) AS INTEGER)
            ELSE 2000 + CAST(SUBSTR(
              CASE
                WHEN INSTR(date_of_visit, char(10)) > 0
                THEN SUBSTR(date_of_visit, 1, INSTR(date_of_visit, char(10)) - 1)
                ELSE date_of_visit
              END,
              INSTR(date_of_visit, 'Visit Date: ') + 12 +
              INSTR(SUBSTR(
                CASE
                  WHEN INSTR(date_of_visit, char(10)) > 0
                  THEN SUBSTR(date_of_visit, 1, INSTR(date_of_visit, char(10)) - 1)
                  ELSE date_of_visit
                END,
                INSTR(date_of_visit, 'Visit Date: ') + 12), '/'
              ) +
              INSTR(SUBSTR(
                CASE
                  WHEN INSTR(date_of_visit, char(10)) > 0
                  THEN SUBSTR(date_of_visit, 1, INSTR(date_of_visit, char(10)) - 1)
                  ELSE date_of_visit
                END,
                INSTR(date_of_visit, 'Visit Date: ') + 12 +
                INSTR(SUBSTR(
                  CASE
                    WHEN INSTR(date_of_visit, char(10)) > 0
                    THEN SUBSTR(date_of_visit, 1, INSTR(date_of_visit, char(10)) - 1)
                    ELSE date_of_visit
                  END,
                  INSTR(date_of_visit, 'Visit Date: ') + 12), '/')
              ), '/'),
              2
            ) AS INTEGER)
          END
      END
  END AS visit_year

FROM medical_records;
"""
