import sys
import scrapy
from scraper.items import ScraperItem
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[4]))
from data.db import get_all_ids

class OcHazSpider(scrapy.Spider):
    name = "oc_haz"
    allowed_domains = ["www.oc.hu"]
    start_urls = ["https://www.oc.hu/ingatlanok/lista/jelleg:haz;ertekesites:elado?page=1-50",
                  "https://www.oc.hu/ingatlanok/lista/jelleg:haz;ertekesites:elado?page=51-100",
                  "https://www.oc.hu/ingatlanok/lista/jelleg:haz;ertekesites:elado?page=101-150",
                  "https://www.oc.hu/ingatlanok/lista/jelleg:haz;ertekesites:elado?page=151-200",
                  "https://www.oc.hu/ingatlanok/lista/jelleg:haz;ertekesites:elado?page=201-250",
                  "https://www.oc.hu/ingatlanok/lista/jelleg:haz;ertekesites:elado?page=251-300",
                  "https://www.oc.hu/ingatlanok/lista/jelleg:haz;ertekesites:elado?page=301-350",
                  "https://www.oc.hu/ingatlanok/lista/jelleg:haz;ertekesites:elado?page=351-400",
                  "https://www.oc.hu/ingatlanok/lista/jelleg:haz;ertekesites:elado?page=401-450",
                  "https://www.oc.hu/ingatlanok/lista/jelleg:haz;ertekesites:elado?page=451-500",
                  "https://www.oc.hu/ingatlanok/lista/jelleg:haz;ertekesites:elado?page=501-550",
                  "https://www.oc.hu/ingatlanok/lista/jelleg:haz;ertekesites:elado?page=551-600",
                  "https://www.oc.hu/ingatlanok/lista/jelleg:haz;ertekesites:elado?page=601-650",                 
                  ]

    def parse(self, response):
        listings = response.css("div[class='row items_container js-host'] > div")

        for property in listings:
            property_sublink = property.css("div > a:first-of-type::attr(href)").get()
            listing_url = response.urljoin(property_sublink)
            id = property_sublink.split('/')[-1]

            ids = get_all_ids("house_listings", "oc")
            if id in ids:
                print(f"\033[93m[SKIPPED] Listing with ID {id} already exists in the database.\033[0m")
                continue

            is_new_house = True if property_sublink.split('/')[1] == "uj-lakas" else False
            english_site = True if property_sublink.split('/')[1] == "realestate" or property_sublink.split('/')[1] == "realestates" else False
            if is_new_house or english_site:
                continue

            yield response.follow(
                listing_url,
                callback=self.parse_listing,
                meta = {
                    'id': id,
                    'listing_url': listing_url,
                    'is_new_house': is_new_house
                }
            )

    def parse_listing(self, response):
        id = response.meta.get('id')
        listing_url = response.meta.get('listing_url')
        is_new_house = response.meta.get('is_new_house')

        if not is_new_house:
            price = response.css("h2.head-price::text").get()
            price = price.replace('\xa0', '')

            location = response.css("section.data-sheet-head > address.head-address::text").get(default="missing data")
            year_built = response.xpath('//div[@class="col data-label" and text()="Építés éve"]/following-sibling::div[@class="col data-value"][1]/text()').get(default="missing data")
            size = response.xpath('//div[@class="col data-label" and text()="Méret"]/following-sibling::div[@class="col data-value"][1]/text()').get(default="missing data")
            property_size = response.xpath('//div[@class="col data-label" and text()="Telek méret"]/following-sibling::div[@class="col data-value"][1]/text()').get(default="missing data")
            rooms = response.xpath('//ul[@class="head-main-params"]/li[contains(text(),"szoba")]/text()').get(default="missing data")
            condition = response.xpath('//div[@class="col data-label" and text()="Állapot"]/following-sibling::div[@class="col data-value"][1]/text()').get(default="missing data")
            facade_condition = response.xpath('//div[@class="col data-label" and text()="Homlokzat állapota"]/following-sibling::div[@class="col data-value"][1]/text()').get(default="missing data")
            heating = response.xpath('//div[@class="col data-label" and text()="Fűtés"]/following-sibling::div[@class="col data-value"][1]/text()').get(default="missing data")
            bathrooms = response.xpath('//div[@class="col data-label" and text()="Fürdőszobák száma"]/following-sibling::div[@class="col data-value"][1]/text()').get(default="missing data")
            legal_status = response.xpath('//div[@class="col data-label" and text()="Jogi státusz"]/following-sibling::div[@class="col data-value"][1]/text()').get(default="missing data")

        scraper_item = ScraperItem()
        scraper_item["site"] = "oc"
        scraper_item["id"] = id
        scraper_item["listing_url"] = listing_url
        scraper_item["price"] = price
        scraper_item["location"] = location
        scraper_item["year_built"] = year_built
        scraper_item["size"] = size
        scraper_item["property_size"] = property_size
        scraper_item["rooms"] = rooms
        scraper_item["condition"] = condition
        scraper_item["facade_condition"] = facade_condition
        scraper_item["heating"] = heating
        scraper_item["bathrooms"] = bathrooms
        scraper_item["legal_status"] = legal_status

        yield scraper_item