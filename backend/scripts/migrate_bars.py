from app.db.session import engine
from sqlalchemy import text

def add_column_if_not_exists(table, column, type_def):
    with engine.connect() as conn:
        try:
            # Check if column exists
            # SQLite specific check
            result = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
            columns = [row[1] for row in result]
            if column not in columns:
                print(f"Adding column {column} to table {table}...")
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {type_def}"))
                conn.commit()
            else:
                print(f"Column {column} already exists in table {table}.")
        except Exception as e:
            print(f"Error checking/adding column: {e}")

if __name__ == "__main__":
    add_column_if_not_exists("weekly_bars", "adj_factor", "FLOAT")
    add_column_if_not_exists("monthly_bars", "adj_factor", "FLOAT")
