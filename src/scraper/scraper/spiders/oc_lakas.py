import sys
import scrapy
from scraper.items import ScraperItem
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[4]))
from data.db import get_all_ids

class OcLakasSpider(scrapy.Spider):
    name = "oc_lakas"
    allowed_domains = ["www.oc.hu"]
    start_urls = ["https://www.oc.hu/ingatlanok/lista/jelleg:lakas;ertekesites:elado;elhelyezkedes:debrecen?page=1-100000"]

    def parse(self, response):
        listings = response.css("div[class='row items_container js-host'] > div")

        for property in listings:
            property_sublink = property.css("div > a:first-of-type::attr(href)").get()
            listing_url = response.urljoin(property_sublink)
            id = property_sublink.split('/')[-1]

            ids = get_all_ids("apartment_listings", "oc")
            if id in ids:
                print(f"\033[93m[SKIPPED] Listing with ID {id} already exists in the database.\033[0m")
                continue

            is_new_apartment = True if property_sublink.split('/')[1] == "uj-lakas" else False
            if is_new_apartment:
                continue

            yield response.follow(
                listing_url,
                callback=self.parse_listing,
                meta = {
                    'id': id,
                    'listing_url': listing_url,
                    'is_new_apartment': is_new_apartment,
                }
            )

    def parse_listing(self, response):
        id = response.meta.get('id')
        listing_url = response.meta.get('listing_url')
        is_new_apartment = response.meta.get('is_new_apartment')

        if not is_new_apartment:
            price = response.css("h2.head-price::text").get()
            price = price.replace('\xa0', '')
            
            year_built = response.xpath('//div[@class="col data-label" and text()="Építés éve"]/following-sibling::div[@class="col data-value"][1]/text()').get()
            build_type = response.xpath('//div[@class="col data-label" and text()="Építési mód"]/following-sibling::div[@class="col data-value"][1]/text()').get()
            size = response.xpath('//div[@class="col data-label" and text()="Méret"]/following-sibling::div[@class="col data-value"][1]/text()').get()
            rooms = response.xpath('//ul[@class="head-main-params"]/li[contains(text(),"szoba")]/text()').get()
            condition = response.xpath('//div[@class="col data-label" and text()="Állapot"]/following-sibling::div[@class="col data-value"][1]/text()').get()
            heating = response.xpath('//div[@class="col data-label" and text()="Fűtés"]/following-sibling::div[@class="col data-value"][1]/text()').get()
            bathrooms = response.xpath('//div[@class="col data-label" and text()="Fürdőszobák száma"]/following-sibling::div[@class="col data-value"][1]/text()').get()
            stairwell_condition = response.xpath('//div[@class="col data-label" and text()="Lépcsőház állapota"]/following-sibling::div[@class="col data-value"][1]/text()').get()
            facade_condition = response.xpath('//div[@class="col data-label" and text()="Homlokzat állapota"]/following-sibling::div[@class="col data-value"][1]/text()').get()
            legal_status = response.xpath('//div[@class="col data-label" and text()="Jogi státusz"]/following-sibling::div[@class="col data-value"][1]/text()').get()

        scraper_item = ScraperItem()
        scraper_item["site"] = "oc"
        scraper_item["id"] = id
        scraper_item["listing_url"] = listing_url
        scraper_item["price"] = price
        scraper_item["year_built"] = year_built
        scraper_item["size"] = size
        scraper_item["condition"] = condition
        scraper_item["rooms"] = rooms
        scraper_item["heating"] = heating
        scraper_item["bathrooms"] = bathrooms
        scraper_item["stairwell_condition"] = stairwell_condition
        scraper_item["facade_condition"] = facade_condition
        scraper_item["legal_status"] = legal_status

        yield scraper_item