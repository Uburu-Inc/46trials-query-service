import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

db_files = {
    "00DB1": {
        "path": os.path.join(
            BASE_DIR,
            "databases",
            "Apex_Care_University_Hospital_DB.sqlite"
        ),
        "table": "medical_records"
    },
    "00DB2": {
        "path": os.path.join(
            BASE_DIR,
            "databases",
            "Crest_Medical_and_Research_Hospital_DB.sqlite"
        ),
        "table": "medical_records"
    },
    "00DB3": {
        "path": os.path.join(
            BASE_DIR,
            "databases",
            "Emerald_Care_Polytechnic_Hospital_DB.sqlite"
        ),
        "table": "medical_records"
    },
     "00DB4": {
        "path": os.path.join(
            BASE_DIR,
            "databases",
            "Golden_Eagle_Medical_Centre_DB.sqlite"
        ),
        "table": "medical_records"
    },
     "00DB5": {
        "path": os.path.join(
            BASE_DIR,
            "databases",
            "Harmony_Health_Institute_DB.sqlite"
        ),
        "table": "medical_records"
    },
     "00DB6": {
        "path": os.path.join(
            BASE_DIR,
            "databases",
            "Lakewood_Community_Hospital_DB.sqlite"
        ),
        "table": "medical_records"
    },
     "00DB7": {
        "path": os.path.join(
            BASE_DIR,
            "databases",
            "Meridian_Health_Centre_DB.sqlite"
        ),
        "table": "medical_records"
    },
     "00DB8": {
        "path": os.path.join(
            BASE_DIR,
            "databases",
            "Pinnacle_University_Teaching_Hospital_DB.sqlite"
        ),
        "table": "medical_records"
    },
     "00DB9": {
        "path": os.path.join(
            BASE_DIR,
            "databases",
            "Silverleaf_General_Hospital_DB.sqlite"
        ),
        "table": "medical_records"
    },
     "00DB10": {
        "path": os.path.join(
            BASE_DIR,
            "databases",
            "Sunrise_General_Hospital_DB.sqlite"
        ),
        "table": "medical_records"
    }
}
