

class DatabaseConfKey:
    HOST = "host"
    USER = "user"
    PORT = "port"
    PASSWORD = "password"
    DATABASE = "database"
    TIMEZONE = "timezone"


DATABASE_JSONSCHEMA = {
    "type": "object",
    "properties": {
        DatabaseConfKey.HOST: {"type": "string", "minLength": 1, "description": "Database host"},
        DatabaseConfKey.PORT: {"type": "integer", "minimum": 1, "description": "Database port"},
        DatabaseConfKey.USER: {"type": "string", "minLength": 1, "description": "Database user"},
        DatabaseConfKey.PASSWORD: {"type": "string", "minLength": 1, "description": "Database password"},
        DatabaseConfKey.DATABASE: {"type": "string", "minLength": 1, "description": "Database name"},
        DatabaseConfKey.TIMEZONE: {"type": "string", "minLength": 1, "description": "Predefined session timezone"},
    },
    "additionalProperties": False,
    "required": [DatabaseConfKey.HOST, DatabaseConfKey.PORT, DatabaseConfKey.DATABASE],
}


DATABASE_CONNECTIONS_JSONSCHEMA = {
    "type": "object",
    "additionalProperties": DATABASE_JSONSCHEMA,
    "description": "Dictionary of <database-connection-name>:<database-connection-properties>"
}
