import scrapy


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

            # If id is present in DB, don't parse it.
            ids = [] # Call db
            if id in ids:
                continue

            is_new_apartment = True if property_sublink.split('/')[1] == "uj-lakas" else False
            if is_new_apartment:
                continue
            

            yield response.follow(
                listing_url,
                callback=self.parse_listing,
                meta = {
                    'id': id,
                    'is_new_apartment': is_new_apartment,
                }
        
            )

    def parse_listing(self, response):
        id = response.meta.get('id')
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
            ceiling_height = response.xpath('//div[@class="col data-label" and text()="Belmagasság"]/following-sibling::div[@class="col data-value"][1]/text()').get()
            stairwell_condition = response.xpath('//div[@class="col data-label" and text()="Lépcsőház állapota"]/following-sibling::div[@class="col data-value"][1]/text()').get()
            facade_condition = response.xpath('//div[@class="col data-label" and text()="Homlokzat állapota"]/following-sibling::div[@class="col data-value"][1]/text()').get()


        yield {
            "id": id,
            "price": price,
            "year_built": year_built,
            "build_type": build_type,
            "size": size,
            "condition": condition,
            "rooms": rooms,
            "heating": heating,
            "bathrooms": bathrooms,
            "ceiling_height": ceiling_height,
            "stairwell_condition": stairwell_condition,
            "facade_condition": facade_condition,
        }

