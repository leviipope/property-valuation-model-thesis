import sqlite3
import os
import numpy as np
import pandas as pd
import sqlalchemy as sa
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data/raw/raw.db" 

# Migrate data from raw.db to processed.db with cleaning and feature engineering
def process_raw_data():
    conn = get_connection()
    apartments = pd.read_sql("SELECT * FROM apartment_listings", conn)
    houses = pd.read_sql("SELECT * FROM house_listings", conn)
    conn.close()

    apartments.drop(columns=['site', 'listing_url'], errors='ignore', inplace=True)
    houses.drop(columns=['site', 'listing_url'], errors='ignore', inplace=True)

    # Replace "missing data" with pd.NA
    apartments.replace("missing data", pd.NA, inplace=True)
    houses.replace("missing data", pd.NA, inplace=True)

    # Convert columns to numeric where applicable
    for col in ['rooms', 'size', 'property_size', 'year_built', 'bathrooms']:
        houses[col] = pd.to_numeric(houses[col], errors='coerce')
    for col in ['rooms', 'size', 'year_built', 'bathrooms']:
        apartments[col] = pd.to_numeric(apartments[col], errors='coerce')

    def clean_table(df):
        # Legal status cross validation
        mask_new = df['legal_status'].eq('new')
        df.loc[mask_new & df['year_built'].isna(), 'year_built'] = 2024
        df.loc[mask_new & df['condition'].isna(), 'condition'] = 'newly built'

        mask_newly_built = df['condition'].eq('newly built')
        df.loc[mask_newly_built & df['year_built'].isna(), 'year_built'] = 2024
        df.loc[mask_newly_built & df['legal_status'].isna(), 'legal_status'] = 'new'

        mask_year_new = df['year_built'].isin([2024, 2025, 2026])
        df.loc[mask_year_new & df['legal_status'].isna(), 'legal_status'] = 'new'
        df.loc[mask_year_new & df['condition'].isna(), 'condition'] = 'newly built'

        mask_used = df['legal_status'].isna() & ~df['year_built'].isin([2024, 2025, 2026])
        df.loc[mask_used, 'legal_status'] = 'used'

        numeric_cols = df.select_dtypes(include=["float64", "int64"]).columns
        categorical_cols = df.select_dtypes(include=["object"]).columns

        # Handle missing values
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            if col == 'bathrooms':
                df[col] = df[col].fillna(df[col].mode()[0])
            elif col == 'year_built':
                df[col] = df[col].fillna(int(df[col].median()))

        for col in categorical_cols:
            if col == 'facade_condition' or col == 'stairwell_condition':
                df[col] = df[col].fillna(df['condition'])
            elif col == 'heating':
                df[col] = df[col].fillna(df[col].mode()[0])
            else:
                df[col] = df[col].fillna('MISSING')
            df[col] = df[col].astype(str)

        return df
    
    apartments_clean = clean_table(apartments)
    houses_clean = clean_table(houses)

    def feature_engineering_apartment(df):
        df.insert(9, 'age_of_property', 2026 - df['year_built'])
        df['age_of_property'] = df['age_of_property'].clip(lower=0, upper=100).astype(int)
        df.insert(1, 'log_price', (df['price'].apply(lambda x: pd.NA if x <= 0 else x).apply(lambda x: pd.NA if pd.isna(x) else round(np.log(x), 2))))
        df.insert(3, 'log_size', (df['size'].apply(lambda x: pd.NA if x <= 0 else x).apply(lambda x: pd.NA if pd.isna(x) else round(np.log(x), 2))))
        df.insert(5, 'size_per_room', (df['size'] / df['rooms']).astype(int))
        df.insert(7, 'bathrooms_per_room', (df['bathrooms'] / df['rooms']).round(2))
        return df
    
    def feature_engineering_house(df):
        df.insert(9, 'age_of_property', 2026 - df['year_built'])
        df['age_of_property'] = df['age_of_property'].clip(lower=0, upper=100).astype(int)
        df.insert(1, 'log_price', (df['price'].apply(lambda x: pd.NA if x <= 0 else x).apply(lambda x: pd.NA if pd.isna(x) else round(np.log(x), 2))))
        df.insert(3, 'log_size', (df['size'].apply(lambda x: pd.NA if x <= 0 else x).apply(lambda x: pd.NA if pd.isna(x) else round(np.log(x), 2))))
        df.insert(4, 'log_property_size', (df['property_size'].apply(lambda x: pd.NA if x <= 0 else x).apply(lambda x: pd.NA if pd.isna(x) else round(np.log(x), 2))))
        df.insert(7, 'size_per_room', (df['size'] / df['rooms']).astype(int))
        df.insert(9, 'bathrooms_per_room', (df['bathrooms'] / df['rooms']).round(2))
        return df

    apartments_engineered = feature_engineering_apartment(apartments_clean)
    houses_engineered = feature_engineering_house(houses_clean)

    # No longer needed columns (after feature engineering)
    apartments_clean.drop(columns=['size', 'price'], inplace=True)
    houses_clean.drop(columns=['size', 'price', 'property_size'], inplace=True)

    new_conn = get_new_connection()
    apartments_engineered.to_sql("apartment_listings_processed", new_conn, if_exists='replace', index=False)
    houses_engineered.to_sql("house_listings_processed", new_conn, if_exists='replace', index=False)
    new_conn.close()

    print("[INFO] Data migration completed.")

def get_new_connection():
    DB_PATH = PROJECT_ROOT / "data/processed/processed.db"
    try:
        if not os.path.exists(DB_PATH):
            raise FileNotFoundError(f"Database file not found at {DB_PATH}")
        
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
         raise RuntimeError(f"\033[91m[DB ERROR] Failed to connect to database: {e}\033[0m")
    

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

def get_all_ids(table_name, site):
    conn = get_connection()
    c = conn.cursor()
    c.execute(f'SELECT id FROM {table_name} WHERE site = ?', (site,))
    ids = [row['id'] for row in c.fetchall()]
    conn.close()
    return ids

def count_listings_per_site(table_name):
    conn = get_connection()
    c = conn.cursor()
    c.execute(f'SELECT site, COUNT(*) as count FROM {table_name} GROUP BY site')
    counts = c.fetchall()
    conn.close()
    for row in counts:
        print(f"Site: {row['site']}, Listings Count: {row['count']}")

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

def delete_table(table_name, database):
    if database not in [1, 2]:
        database = int(input("Select database to delete from (1 - raw.db, 2 - processed.db): "))
    if database == 1:
        conn = get_connection()
        database_name = "raw"
    if database == 2:
        conn = get_new_connection()
        database_name = "processed"
    c = conn.cursor()
    c.execute(f'DROP TABLE IF EXISTS {table_name}')
    print(f"[INFO] Table '{table_name}' deleted from database {database_name}.")
    conn.commit()
    conn.close()    

def delete_listing(table_name, id):
    conn = get_connection()
    c = conn.cursor()
    c.execute(f'DELETE FROM {table_name} WHERE id = ?', (id,))
    conn.commit()
    conn.close()

def count_unique_values_per_site(table_name, column_name):
    conn = get_connection()
    c = conn.cursor()
    c.execute(f"SELECT site, {column_name}, COUNT(*) as count FROM {table_name} GROUP BY site, {column_name}")
    values = c.fetchall()
    conn.close()

    for site, value, count in values:
        print(f"Site: {site}, {column_name}: {value}, Count: {count}")

def count_unique_values(table_name, column_name):
    conn = get_connection()
    c = conn.cursor()
    c.execute(f"SELECT {column_name}, COUNT(*) as count FROM {table_name} GROUP BY {column_name} ORDER BY count DESC")
    values = c.fetchall()
    conn.close()

    for value, count in values:
        print(f"{column_name}: {value}, Count: {count}")

def replace_values():
    conn = get_connection()
    c = conn.cursor()
    table_name = "apartment_listings"
    column_name = "heating"
    try:
        # ("new value", "old_value"),
        updates = [
            #("other", "underfloor heating"),
            #("other", "individual metered central heating"),
        ]

        for new_val, old_val in updates:
            c.execute(
                f"UPDATE {table_name} SET {column_name} = ? WHERE lower(trim({column_name})) = lower(trim(?))",
                (new_val, old_val),
            )
            conn.commit()
            print(f"[INFO] Replaced '{old_val}' -> '{new_val}' ({c.rowcount} rows affected)")

    except sqlite3.Error as e:
        conn.rollback()
        print(f"[DB ERROR] Failed to update heating values: {e}")

    conn.close()

def main():
    # replace_values()
    table_name1 = "apartment_listings"
    table_name2 = "house_listings"
    column_name = "heating"
    print(f"\nCounting unique values in column '{column_name}' for table '{table_name1}':")
    count_unique_values(table_name1, column_name)
    print(f"\nCounting unique values in column '{column_name}' for table '{table_name2}':")
    count_unique_values(table_name2, column_name)

if __name__ == "__main__":
    main()