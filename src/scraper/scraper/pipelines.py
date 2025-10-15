# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import sys
import re
from itemadapter import ItemAdapter 
from pathlib import Path
from scrapy.utils.project import get_project_settings

sys.path.insert(0, str(Path(__file__).parents[3]))
from data.db import get_connection

class CleanDataPipeline:
    def convert_to_int(self, s):
        s = s.strip()
        if ',' in s and '.' not in s:
            if s.count(',') == 1 and s.split(',')[1].isdigit():
                s = s.split(',')[0]
            else:
                s = s.replace(',', '')
        try:
            return int(s)
        except ValueError:
            return int(float(s))

    def try_convert_to_int(self, value, item_id=None, field_name='field'):
        """Try to convert a value to int. Logs a warning if conversion fails."""
        if value == 'missing data':
            return 'missing data'
        else:
            try:
                return self.convert_to_int(value)
            except (ValueError, TypeError):
                print(f"\033[93m[WARNING] {field_name} conversion failed for item ID {item_id} value: {value}\033[0m")
                return None

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        print("\033[94mCleaning item...\033[0m")

        if spider.name == "jofogas_haz" or spider.name == "jofogas_lakas":
            for field in item.keys():
                if item[field] == "Hiányzó adat" or item[field] is None or item[field] == 'None' or item[field] == '':
                    item[field] = "missing data"

            if adapter.get('build_type'):
                build_type = adapter.get('build_type')
                if build_type == 'tégla':
                    adapter['build_type'] = 'brick'
                elif build_type == 'egyéb':
                    adapter['build_type'] = 'other'

            if adapter.get('condition'):
                condition = adapter.get('condition').lower()
                if condition == 'jó állapotú' or condition == 'felújított':
                    adapter['condition'] = 'good'
                elif condition == 'újszerű':
                    adapter['condition'] = 'excellent'
                elif condition == 'új építésű':
                    adapter['condition'] = 'newly built'
                elif condition == 'felújítandó':
                    adapter['condition'] = 'bad'

            if adapter.get('heating'):
                heating = adapter.get('heating').lower()
                if heating == 'gáz-cirkó':
                    adapter['heating'] = 'central gas heating'
                elif heating == 'hőszivattyú':
                    adapter['heating'] = 'heat pump'
                elif heating == 'egyéb':
                    adapter['heating'] = 'other'
                elif heating == 'távfűtés' or heating == 'távhő':
                    adapter['heating'] = 'district heating'
                elif heating == 'egyedi mérős központifűtés':
                    adapter['heating'] = 'individual metered central heating'
                elif heating == 'gáz konvektor':
                    adapter['heating'] = 'gas convector'

            if adapter.get('price'):
                price_str = adapter.get('price').replace('Ft', '').replace(' ', '').replace('\n', '')
                adapter['price'] = self.try_convert_to_int(price_str, item_id=adapter.get('id'), field_name='price')

            if adapter.get('property_size') and adapter.get('property_size') != "missing data":
                property_size_str = adapter.get('property_size').replace(' \xa0m2', '').replace(' ', '').replace('\n', '')
                adapter['property_size'] = self.try_convert_to_int(property_size_str, item_id=adapter.get('id'), field_name='property_size')
            
            if adapter.get('size'):
                size_str = adapter.get('size').replace('m²', '').replace(' ', '').replace('\n', '')
                adapter['size'] = self.try_convert_to_int(size_str, item_id=adapter.get('id'), field_name='size')

            if adapter.get('year_built') and adapter.get('year_built') != "missing data":
                year_built_str = adapter.get('year_built').replace(' ', '').replace('\n', '')
                adapter['year_built'] = self.try_convert_to_int(year_built_str, item_id=adapter.get('id'), field_name='year_built')

            if adapter.get('rooms'):
                rooms_str = adapter.get('rooms').replace(' ', '').replace('+', '').replace('szoba', '')
                adapter['rooms'] = self.try_convert_to_int(rooms_str, item_id=adapter.get('id'), field_name='rooms')

        if spider.name == "oc_haz" or spider.name == "oc_lakas":
            if adapter.get('bathrooms'):
                bathrooms_str = adapter.get('bathrooms')
                if bathrooms_str is None or bathrooms_str == 'None':
                    adapter['bathrooms'] = 'missing data'
                adapter['bathrooms'] = self.try_convert_to_int(bathrooms_str, item_id=adapter.get('id'), field_name='bathrooms')

            if adapter.get('build_type'):
                build_type = adapter.get('build_type').lower()
                if build_type == 'tégla':
                    adapter['build_type'] = 'brick'
                elif build_type == 'panel':
                    adapter['build_type'] = 'panel'
                elif build_type == 'csúsztatott zsalus':
                    adapter['build_type'] = 'slipform'
                elif build_type == 'vályog':
                    adapter['build_type'] = 'adobe'
            
            if adapter.get('condition'):
                condition = adapter.get('condition').lower()
                if condition == 'jó':
                    adapter['condition'] = 'good'
                elif condition == 'kiváló':
                    adapter['condition'] = 'excellent'
                elif condition == 'átlagos':
                    adapter['condition'] = 'average'
                elif condition == 'felújítandó' or condition == 'Átlagon aluli':
                    adapter['condition'] = 'bad'

            if adapter.get('facade_condition'):
                facade_condition = adapter.get('facade_condition').lower()
                if facade_condition == 'jó':
                    adapter['facade_condition'] = 'good'
                elif facade_condition == 'kiváló':
                    adapter['facade_condition'] = 'excellent'
                elif facade_condition == 'átlagos':
                    adapter['facade_condition'] = 'average'
                elif facade_condition == 'rossz':
                    adapter['facade_condition'] = 'bad'

            if adapter.get('stairwell_condition'):
                stairwell_condition = adapter.get('stairwell_condition').lower()
                if stairwell_condition is None:
                    adapter['stairwell_condition'] = 'missing data'
                else:
                    if stairwell_condition == 'jó':
                        adapter['stairwell_condition'] = 'good'
                    elif stairwell_condition == 'kiváló':
                        adapter['stairwell_condition'] = 'excellent'
                    elif stairwell_condition == 'átlagos':
                        adapter['stairwell_condition'] = 'average'

            if adapter.get('heating'):
                heating = adapter.get('heating').lower()
                if heating is None:
                    adapter['heating'] = 'missing data'
                else:
                    if heating == 'gáz cirkó':
                        adapter['heating'] = 'central gas heating'
                    elif heating == 'egyedi távfűtés':
                        adapter['heating'] = 'individual district heating'
                    elif heating == 'távfűtés':
                        adapter['heating'] = 'district heating'
                    elif heating == 'hőszivattyú':
                        adapter['heating'] = 'heat pump'
                    elif 'központi' in heating:
                        adapter['heating'] = 'central heating'
                    elif heating == 'gáz konvektor' or heating == 'gázkonvektor':
                        adapter['heating'] = 'gas convector'
                    elif heating == 'gázkazán' or heating == 'gáz kazán':
                        adapter['heating'] = 'gas boiler'
                    elif heating == 'megújuló':
                        adapter['heating'] = 'renewable'
                    else:
                        adapter['heating'] = 'missing data'

            if adapter.get('legal_status'):
                legal_status = adapter.get('legal_status').lower()
                if legal_status == 'használt':
                    adapter['legal_status'] = 'used'
                elif legal_status == 'új':
                    adapter['legal_status'] = 'new'

            if adapter.get('price'):
                price_str = adapter.get('price').replace('Ft', '').replace(' ', '')
                adapter['price'] = self.try_convert_to_int(price_str, item_id=adapter.get('id'), field_name='price')

            if adapter.get('rooms'):
                rooms_str = adapter.get('rooms').replace(' ', '').replace('szoba', '')
                adapter['rooms'] = self.try_convert_to_int(rooms_str, item_id=adapter.get('id'), field_name='rooms')

            if adapter.get('size'):
                size_str = adapter.get('size').replace('m²', '').replace(' ', '').replace(',', '.')
                adapter['size'] = self.try_convert_to_int(size_str, item_id=adapter.get('id'), field_name='size')

            if adapter.get('property_size'):
                property_size_str = adapter.get('property_size').replace('m²', '').replace(' ', '').replace('\xa0', '')
                adapter['property_size'] = self.try_convert_to_int(property_size_str, item_id=adapter.get('id'), field_name='property_size')

            if adapter.get('year_built'):
                year_built_str = adapter.get('year_built').replace(' ', '')
                adapter['year_built'] = self.try_convert_to_int(year_built_str, item_id=adapter.get('id'), field_name='year_built')

        if adapter.get('legal_status') == 'new':
            if adapter.get('year_built') == 'missing data':
                adapter['year_built'] = 2024
            if adapter.get('condition') == 'missing data':
                adapter['condition'] = 'newly built'

        if adapter.get('condition') == 'newly built':
            if adapter.get('year_built') == 'missing data':
                adapter['year_built'] = 2024
            if adapter.get('legal_status') == 'missing data':
                adapter['legal_status'] = 'new'

        if adapter.get('year_built') in [2024, 2025, 2026]:
            if adapter.get('condition') == 'missing data':
                adapter['condition'] = 'newly built'
            if adapter.get('legal_status') == 'missing data':
                adapter['legal_status'] = 'new'

        if spider.name == "oc_haz" or spider.name == "jofogas_haz":
            if adapter.get('condition') == 'newly built' and adapter.get('facade_condition') == 'missing data':
                adapter['facade_condition'] = 'excellent'

        if spider.name == "oc_lakas" or spider.name == "jofogas_lakas":
            if adapter.get('condition') == 'newly built':
                if adapter.get('facade_condition') == 'missing data':
                    adapter['facade_condition'] = 'excellent'
                if adapter.get('stairwell_condition') == 'missing data':
                    adapter['stairwell_condition'] = 'excellent'
                    
        print("\033[92mItem cleaned!\033[0m")

        return item

class SQLitePipeline:
    def __init__(self):
        settings = get_project_settings()
        self.enabled = settings.getbool('ENABLE_SQL_PIPELINE')
        if not self.enabled:
            print("\033[93m[SQL PIPELINE DISABLED — DEVELOPMENT MODE]\033[0m")
        else:
            print("\033[94mInitializing SQLitePipeline...\033[0m")
            self.conn = get_connection()
            self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        if not self.enabled:
            return item

        adapter = ItemAdapter(item)
        id = adapter.get('id')
        print(f"\033[94mProcessing item ID: {id} in SQL pipeline\033[0m")

        for key, value in adapter.items():
            if value in [None, 'None', '']:
                adapter[key] = 'missing data'

        if spider.name == "jofogas_lakas" or spider.name == "oc_lakas":
            columns, placeholders, values = [], [], []

            for field in adapter.keys():
                columns.append(field)
                placeholders.append('?')
                values.append(adapter.get(field))

            query = f"INSERT INTO apartment_listings ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"

            try:
                self.cursor.execute(query, values)
                self.conn.commit()
                print(f"\033[92mItem ID {id} inserted into apartment_listings table.\033[0m")
            except Exception as e:
                print(f"\033[91m[DB ERROR] Failed to insert item ID {id} into apartment_listings: {e}\033[0m")

        elif spider.name == "jofogas_haz" or spider.name == "oc_haz":
            columns, placeholders, values = [], [], []

            for field in adapter.keys():
                columns.append(field)
                placeholders.append('?')
                values.append(adapter.get(field))

            query = f"INSERT INTO house_listings ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"

            try:
                self.cursor.execute(query, values)
                self.conn.commit()
                print(f"\033[92mItem ID {id} inserted into house_listings table.\033[0m")
            except Exception as e:
                print(f"\033[91m[DB ERROR] Failed to insert item ID {id} into house_listings: {e}\033[0m")

        return item