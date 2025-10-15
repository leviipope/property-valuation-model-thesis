import os
import sys
from pathlib import Path
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

BASE_DIR = Path(__file__).resolve().parents[2]
SCRAPER_DIR = BASE_DIR / 'src' / 'scraper'
sys.path.append(str(SCRAPER_DIR))
os.environ.setdefault("SCRAPY_PROJECT", "scraper")
os.environ.setdefault("SCRAPY_SETTINGS_MODULE", "scraper.settings")

from scraper.spiders.jofogas_lakas import JofogasLakasSpider
from scraper.spiders.jofogas_haz import JofogasHazSpider
from scraper.spiders.oc_lakas import OcLakasSpider
from scraper.spiders.oc_haz import OcHazSpider

def start_all_spiders():
    process = CrawlerProcess(get_project_settings())

    print("\033[94m[INFO] Starting all spiders with pipelines active...\033[0m")

    process.crawl(JofogasLakasSpider)
    process.crawl(JofogasHazSpider)
    process.crawl(OcLakasSpider)
    process.crawl(OcHazSpider)

    process.start()

    print("\033[92m[INFO] All spiders finished!\033[0m")

if __name__ == "__main__":
    start_all_spiders()
