"""Compatibility entrypoint for versioned migrations."""
from dbd.storage.mysql.migrations import migrate

if __name__ == "__main__":
    migrate()
    print("Schema is current")
