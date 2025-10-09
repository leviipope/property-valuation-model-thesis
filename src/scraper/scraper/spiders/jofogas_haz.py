import sys
import scrapy
from scraper.items import ScraperItem
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[4]))
from data.db import get_all_ids

class JofogasLakasSpider(scrapy.Spider):
    name = "jofogas_haz"
    start_urls = ["https://ingatlan.jofogas.hu/hajdu-bihar/debrecen/haz?st=s"]

    def parse(self, response):
        listings = response.css("div.list-items > div")

        for listing in listings:
            id = listing.css("div::attr(id)").get()
            if "adverticum" in id:
                continue
            listing_url_scrape = listing.css('section > h3 > a::attr(href)').get()
            listing_url = response.urljoin(listing_url_scrape)

            ids = get_all_ids("house_listings", "jofogas")
            if id in ids:
                print(f"\033[93m[SKIPPED] Listing with ID {id} already exists in the database.\033[0m")
                continue

            yield response.follow(
                listing_url,
                callback=self.parse_listing,
                meta={
                    "id": id,
                    "listing_url": listing_url
                }
            )

            next_page = response.xpath('//ul/li/a[normalize-space(text())="\u203A"]/@href').get()

        if next_page:
            yield response.follow(next_page, self.parse)

    def parse_listing(self, response):
        id = response.meta.get('id')
        listing_url = response.meta.get('listing_url')

        price = response.css('h2[class="MuiTypography-root MuiTypography-h2 css-1ptqlsg"]::text').get()
        size = response.xpath(
                '//div[span[text()="Méret:"]]//h6/span/text()'
            ).get(default="Hiányzó adat")
        condition = response.xpath(
            '//div[span[normalize-space(text())="Állapot:"]]//h6/span/text()'
        ).get(default="Hiányzó adat")
        rooms = response.xpath(
                '//div[span[text()="Szobák száma:"]]//h6/span/text()'
            ).get(default="Hiányzó adat")
        heating = response.xpath(
                '//div[span[text()="Fűtés típusa:"]]//h6/span/text()'
            ).get(default="Hiányzó adat")
        year_built = response.xpath(
                '//div[span[text()="Építés éve:"]]//h6/span/text()'
            ).get(default="Hiányzó adat")
        property_size = response.xpath(
                '//div[span[text()="Kert mérete:"]]//h6/span/text()'
            ).get(default="Hiányzó adat")
        
        scraper_item = ScraperItem()

        scraper_item['site'] = "jofogas"
        scraper_item['id'] = id
        scraper_item['listing_url'] = listing_url
        scraper_item['price'] = price
        scraper_item['size'] = size
        scraper_item['condition'] = condition
        scraper_item['rooms'] = rooms
        scraper_item['heating'] = heating
        scraper_item['year_built'] = year_built
        scraper_item['property_size'] = property_size

        yield scraper_item