from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from scraper.spiders.jofogas_lakas import JofogasLakasSpider
from scraper.spiders.jofogas_haz import JofogasHazSpider
from scraper.spiders.oc_lakas import OcLakasSpider
from scraper.spiders.oc_haz import OcHazSpider

def start_all_spiders():
    process = CrawlerProcess(get_project_settings())

    process.crawl(JofogasLakasSpider)
    process.crawl(JofogasHazSpider)
    process.crawl(OcLakasSpider)
    process.crawl(OcHazSpider)

    process.start()

if __name__ == "__main__":
    start_all_spiders()
