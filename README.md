## Start the application
### uvicorn main:app --reload


## Data retrieval query (Use for later)

```sql
SELECT
  record_id,
  visit_id,
  database_id,
  date_of_visit,
  age_years,
  gender,
  diagnosis,
  hpc,
  investigation,
  medication,
  admin_details
FROM v_records_safe
WHERE age_years < 18
  AND LOWER(diagnosis) LIKE '%sickle cell%'
  AND LOWER(diagnosis) NOT LIKE '%malaria%'
  AND LOWER(hpc)       NOT LIKE '%malaria%'
  AND LOWER(admin_details) NOT LIKE '%emergency%'
ORDER BY age_years, database_id;
```
