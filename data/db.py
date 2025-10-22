import sqlite3
import os
import pandas as pd
import sqlalchemy as sa
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data/raw/raw.db" 

def migrate_and_clean():
    conn = get_connection()

    apartments = pd.read_sql("SELECT * FROM apartment_listings", conn)
    houses = pd.read_sql("SELECT * FROM house_listings", conn)

    conn.close()

    apartments.drop(columns=['site', 'id', 'listing_url'], errors='ignore', inplace=True)
    houses.drop(columns=['site', 'id', 'listing_url'], errors='ignore', inplace=True)

    apartments.replace("missing data", pd.NA, inplace=True)
    houses.replace("missing data", pd.NA, inplace=True)

    for col in ['rooms', 'size', 'property_size', 'year_built', 'bathrooms']:
        houses[col] = pd.to_numeric(houses[col], errors='coerce')

    for col in ['rooms', 'size', 'year_built', 'bathrooms']:
        apartments[col] = pd.to_numeric(apartments[col], errors='coerce')

    def clean_table(df):
        numeric_cols = df.select_dtypes(include=["float64", "int64"]).columns
        categorical_cols = df.select_dtypes(include=["object"]).columns

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            if col == 'bathrooms':
                df[col] = df[col].fillna(df[col].mode()[0])
            elif col == 'year_built':
                df[col] = df[col].fillna(int(df[col].mean()))
            else:
                df[col] = df[col].fillna(df[col].median())

        for col in categorical_cols:
            df[col] = df[col].astype(str)
            df[col] = df[col].fillna('missing')

        df = pd.get_dummies(df, columns=categorical_cols, drop_first=True)

        return df
    
    apartments_clean = clean_table(apartments)
    houses_clean = clean_table(houses)

    NEW_DB_PATH = PROJECT_ROOT / "data/processed/processed.db"
    new_conn = sqlite3.connect(NEW_DB_PATH)

    apartments_clean.to_sql("apartment_listings_clean", new_conn, if_exists='replace', index=False)
    houses_clean.to_sql("house_listings_clean", new_conn, if_exists='replace', index=False)

    new_conn.close()

    print("[INFO] Data migration completed.")

def get_info():
    engine = sa.create_engine(f"sqlite:///{DB_PATH}")
    print(("Apartment Listings Table Info:"))
    df = pd.read_sql("SELECT * FROM apartment_listings", engine)
    print(df.info())
    print(df.head())
    print("\n\n" + "-"*50 + "\n")
    print("\nHouse Listings Table Info:")
    df = pd.read_sql("SELECT * FROM house_listings", engine)
    print(df.info())
    print(df.head())

def get_column_info(table_name):
    conn = get_connection()
    c = conn.cursor()

    c.execute(f"PRAGMA table_info({table_name})")
    columns = c.fetchall()

    for col in columns:
        print(f"Column: {col[1]}, Type: {col[2]}")
    
    conn.close()

def replace_nulls(): # deprecated
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
    migrate_and_clean()

if __name__ == "__main__":
    main()