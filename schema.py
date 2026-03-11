from cerberus import Validator

query_schema = {
    "query_parameters": {
        "type": "dict",
        "required": True,
        "schema": {
            "query_type": {
                "type": "string",
                "required": True,
                "empty": False,
                "allowed": ["count", "dataset"],
            },
            "query_value": {
                "type": "list",
                "required": True,
                "schema": {
                    "type": "dict",
                    "schema": {
                        "column": {"type": "string", "required": True, "empty": False},
                        "entries": {"type": "string", "required": True},
                        "exclude": {"type": "string", "required": True},
                        "id": {"type": "string", "required": True, "empty": False},
                        "isSelected": {"type": "boolean", "required": True},
                    },
                },
            },
        },
    }
}

validator = Validator(query_schema)
