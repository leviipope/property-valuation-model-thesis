import pandas as pd
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from data.db import get_new_connection

def get_cities():
    capital_city = 'budapest'
    large_cities = ['debrecen', 'szeged', 'miskolc', 'győr', 'pécs']
    big_cities = ['nyíregyháza', 'kecskemét', 'székesfehérvár', 'szombathely', 'érd', 'szolnok', 'tatabánya', 'kaposvár', 'békéscsaba', 'veszprém', 'zalaegerszeg', 'eger', 'nagykanizsa']
    city_list = [capital_city] + large_cities + big_cities
    return capital_city, large_cities, big_cities, city_list

def categorize_city(city):
    capital_city, large_cities, big_cities, _ = get_cities()
    if city == capital_city:
        return 1
    elif city in large_cities:
        return 2
    elif city in big_cities:
        return 3
    else:
        return 4

def heating_types(table):
    if table == 'Apartment':
        conn = get_new_connection()
        query = "SELECT DISTINCT heating FROM apartment_listings_processed"
        df = pd.read_sql(query, conn)
        return df['heating'].tolist()
    elif table == 'House':
        conn = get_new_connection()
        query = "SELECT DISTINCT heating FROM house_listings_processed"
        df = pd.read_sql(query, conn)
        return df['heating'].tolist()