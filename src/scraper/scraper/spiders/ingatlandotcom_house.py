import scrapy


class IngatlandotcomHouseSpider(scrapy.Spider):
    name = "ingatlandotcom_house"
    allowed_domains = ["ingatlan.com"]
    start_urls = ["https://ingatlan.com/lista/elado+haz+debrecen"]

    def parse(self, response):
        listings = response.css("")


# iced_status = listing.css("div[class='uad-col uad-col-price'] div::attr(class)").get()
#img = listing.css("div[class='uad-col uad-col-image'] > a > img::attr(src)").get()