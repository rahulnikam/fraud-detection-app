from sqlalchemy import create_engine
import json
import os
from urllib.parse import quote_plus

def get_db_connection():
    """
    Reads database config from JSON and returns a SQLAlchemy engine.
    """
    config_path = os.path.join(os.path.dirname(__file__), "../config/db_config.json")

    with open(config_path, "r") as file:
        config = json.load(file)

    # URL-encode password to handle special characters
    password = quote_plus(config['password'])

    # Create SQLAlchemy engine
    engine = create_engine(
        f"mysql+mysqlconnector://{config['user']}:{password}@{config['host']}/{config['database']}"
    )

    return engine
