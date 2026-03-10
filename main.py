from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

from service import Service
from response import response
from schema import validator

app = FastAPI()

# Only allow urls in in origin lists
origins = ["http://localhost:3800"]

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

service = Service()

class DBQuery(BaseModel):
    query_parameters: dict


class APIQuery(BaseModel):
    query: str

# Fetch record via query parameters
@app.post("/fetch-record/{platform}/{db}")
def fetch_records(platform: str, db: str, query_parameters: DBQuery):

    data = query_parameters.dict()

    # Cerberus validation
    if not validator.validate(data):
        raise HTTPException(status_code=400, detail=validator.errors)

    if platform not in ["46_trials", "one_record"]:
        return response(400, False, "Invalid platform")
    return service.run_query(
        db, data["query_parameters"], data["query_parameters"]["query_type"]
    )

# Fetch record via endpoint
@app.post("/fetch-record-via-endpoint/{db}")
def fetch_records_via_endpoint(db: str, query_parameters: APIQuery):
    return service.run_query_via_endpoint(db, query_parameters.query)

# Health check
@app.get("/health")
def health_check():
    return service.health_check()
