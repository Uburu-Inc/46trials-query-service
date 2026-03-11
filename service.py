from db import query_db
from db_dict import db_files
from response import response
from queries.query_builder_count import generate_count_query
from queries.query_builder_data import generate_dataset_query

class Service:
    def __init__(self):
        pass
    
    # Fetch records via api endpoints
    def run_query_via_endpoint(self, db: str, query: str) -> dict:
        # Validate databse
        if db not in db_files:
            return response(404, False, "Database not found", None)
        
        if not query:
            return response(400, False, "Invalid query", None)
        
        db_file = db_files[db]["path"]

        records = query_db(db_file, query)
        return response(200, True, "Success", records)

    # Fetch records from database through the sql query generator
    def run_query(self, db: str, db_query_parameters: dict, query_type: str) -> dict:
        # Validate database
        if db not in db_files:
            return response(404, False, "Database not found", None)

        db_path = db_files[db]["path"]

        try:
            if query_type == "count":
                query = generate_count_query(db_query_parameters["query_value"])
                print(query, '<=== Count query')
                records = query_db(db_path, query)
                return response(200, True, "Success", records[0])
            elif query_type == "dataset":
                query = generate_dataset_query(db_query_parameters["query_value"])
                print(query, '<=== Dataset query')
                records = query_db(db_path, query)
                return response(200, True, "Success", records)
            else:
                return response(400, False, "Invalid query type", None)

        except Exception as e:
            return response(500, False, f"Database error: {str(e)}", None)

    def health_check(self) -> dict:
        return response(200, True, "Health check successful", None)
