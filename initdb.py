import os

from src.core.config import settings
from src.db.db import Database


def init_db(db):
    # Create database functions
    for file in os.listdir("src/db/functions"):
        if file.endswith(".sql"):
            with open(f"src/db/functions/{file}", "r") as f:
                db.perform(f.read())


if __name__ == "__main__":
    db = Database(settings.POSTGRES_DATABASE_URI)
    try:
        init_db(db)
        print("Database initialized.")
    except Exception as e:
        print(e)
        print("Database initialization failed.")
    finally:
        db.close()
