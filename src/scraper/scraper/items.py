# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ScraperItem(scrapy.Item):
    site = scrapy.Field()
    id = scrapy.Field()
    listing_url = scrapy.Field()
    price = scrapy.Field()
    year_built = scrapy.Field()
    size = scrapy.Field()
    property_size = scrapy.Field()
    rooms = scrapy.Field()
    condition = scrapy.Field()
    heating = scrapy.Field()
    bathrooms = scrapy.Field()
    stairwell_condition = scrapy.Field()
    facade_condition = scrapy.Field()
    legal_status = scrapy.Field()

    pass
