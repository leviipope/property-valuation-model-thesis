import scrapy


class JofogasLakasSpider(scrapy.Spider):
    name = "jofogas_haz"
    start_urls = ["https://ingatlan.jofogas.hu/hajdu-bihar/debrecen/haz?st=s"]

    def parse(self, response):
        listings = response.css("div.list-items > div")

        for listing in listings:
            id = listing.css("div::attr(id)").get()
            listing_url_scrape = listing.css('section > h3 > a::attr(href)').get()
            listing_url = response.urljoin(listing_url_scrape)

            # If id is present in DB, don't parse it.
            ids = [] # Call db
            if id in ids:
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
        build_type = response.xpath(
                '//div[span[text()="Ingatlan típusa:"]]//h6/span/text()'
            ).get(default="Hiányzó adat")
        property_size = response.xpath(
                '//div[span[text()="Kert mérete:"]]//h6/span/text()'
            ).get(default="Hiányzó adat")

        yield {
            "id": id,
            "listing_url": listing_url,
            "price": price,
            "size": size,
            "condition": condition,
            "rooms": rooms,
            "heating": heating,
            "year_built": year_built,
            "build_type": build_type,
            "property_size": property_size
        }