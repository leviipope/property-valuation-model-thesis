import sqlite3
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data/database.db" 

def replace_nulls():
    conn = get_connection()
    c = conn.cursor()

    tables = ["apartment_listings", "house_listings"]

    for table in tables:
        c.execute(f"PRAGMA table_info({table})")
        columns = [col[1 ] for col in c.fetchall()]

        for col in columns:
            c.execute(f'''
                UPDATE {table} 
                SET {col} = 'missing data' 
                WHERE {col} IS NULL
            ''')
        print(f"[INFO] NULL values replaced in table '{table}'")

    conn.commit()
    conn.close()

def get_all_ids(table_name, site):
    conn = get_connection()
    c = conn.cursor()
    c.execute(f'SELECT id FROM {table_name} WHERE site = ?', (site,))
    ids = [row['id'] for row in c.fetchall()]
    conn.close()
    return ids

def create_db():
    if not DB_PATH.exists():
        open(DB_PATH, 'w').close()

def get_connection():
    try:
        if not os.path.exists(DB_PATH):
            raise FileNotFoundError(f"Database file not found at {DB_PATH}")
        
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
         raise RuntimeError(f"\033[91m[DB ERROR] Failed to connect to database: {e}\033[0m")

def create_apartment_table():
    conn = get_connection()
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS apartment_listings (
            site TEXT,
            id TEXT UNIQUE NOT NULL,
            listing_url TEXT,
            price INT,
            size TEXT,
            rooms TEXT,
            bathrooms INT,
            condition TEXT,
            facade_condition TEXT,
            stairwell_condition TEXT,
            heating TEXT,
            year_built INT,
            legal_status TEXT,
            PRIMARY KEY (site, id)
        ) 
    ''')

def create_house_table():
    conn = get_connection()
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS house_listings (
            site TEXT,
            id TEXT UNIQUE NOT NULL,
            listing_url TEXT,
            price INT,
            size TEXT,
            property_size TEXT,
            rooms TEXT,
            bathrooms INT,
            condition TEXT,
            facade_condition TEXT,
            heating TEXT,
            year_built INT,
            legal_status TEXT,
            PRIMARY KEY (site, id)
        ) 
    ''')

    conn.commit()
    conn.close()

def clear_table(table_name):
    conn = get_connection()
    c = conn.cursor()
    c.execute(f'DELETE FROM {table_name}')
    conn.commit()
    conn.close()

def delete_table(table_name):
    conn = get_connection()
    c = conn.cursor()
    c.execute(f'DROP TABLE IF EXISTS {table_name}')
    conn.commit()
    conn.close()    

def delete_listing(table_name, id):
    conn = get_connection()
    c = conn.cursor()
    c.execute(f'DELETE FROM {table_name} WHERE id = ?', (id,))
    conn.commit()
    conn.close()

def main():
    clear_table("apartment_listings")
    clear_table("house_listings")

if __name__ == "__main__":
    main()