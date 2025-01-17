import os
import json
import re
from datetime import datetime
from urllib.parse import urljoin
from collections import OrderedDict

from scrapy import signals, Spider, Request


class FarfetchSpider(Spider):
    name = "FarFetch_New"
    current_dt = datetime.now().strftime("%d%m%Y%H%M")

    custom_settings = {
        "CONCURRENT_REQUESTS": 3,
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408],
        # Increase download timeout to handle slow proxies
        'DOWNLOAD_TIMEOUT': 70,

        "ZYTE_API_EXPERIMENTAL_COOKIES_ENABLED": True,
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
            "https": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
        },
        'DOWNLOADER_MIDDLEWARES': {
            "scrapy_zyte_api.ScrapyZyteAPIDownloaderMiddleware": 1000,
            "scrapy_poet.InjectionMiddleware": 543,
        },
        'REQUEST_FINGERPRINTER_CLASS': "scrapy_zyte_api.ScrapyZyteAPIRequestFingerprinter",
        'TWISTED_REACTOR': "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        'ZYTE_API_KEY': "f693db95c418475380b0e70954ed0911",
        "ZYTE_API_TRANSPARENT_MODE": True,
        # 'REQUESTS_PER_SESSION': 2,  # Define requests per session
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'cache-control': 'max-age=0',
        'priority': 'u=0, i',
        'sec-ch-ua-full-version-list': '"Google Chrome";v="131.0.6778.266", "Chromium";v="131.0.6778.266", "Not_A Brand";v="24.0.0.0"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    }

    fields = ['Source ID', 'Product URL', 'Product Title', 'Product ID', 'Category', 'Price', 'Discount',
                'Currency', 'Description', 'Main Image URL', 'Other Images URL', 'Colors', 'Variations',
                'Sizes', 'Other Details', 'Availability', 'Number of Items in Stock', 'Last Update', 'Creation Date'
            ]

    def __init__(self):
        super().__init__()
        self.current_category = ''
        self.category_item_found = 0
        self.category_item_scraped = 0
        self.categories_item_found = 0
        self.categories_item_scraped = 0
        self.categories = ['women', 'men', 'kids']
        self.count_categories = len(self.categories)
        self.current_records = []

        #files & Records
        os.makedirs('output', exist_ok=True)
        self.output_file_path = f'output/{self.name} Products Details {self.current_dt}.json'
        self.previous_scraped_records = self.read_write_json_file(key='previous_records')

        # Logs
        os.makedirs('logs', exist_ok=True)
        self.logs_filepath = f'logs/{self.name}_{self.current_dt}.txt'
        self.script_starting_datetime = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.write_logs(f'[INIT] Script started at {self.script_starting_datetime}')

        self.proxies = {
            "http": "http://scraperapi:075473ce6faba741191f47b04d4512f8@proxy-server.scraperapi.com:8001",
            "https": "https://scraperapi:075473ce6faba741191f47b04d4512f8@proxy-server.scraperapi.com:8001"
        }

    def parse_category_brands(self, response, **kwargs):
        if response.status !=200:
            return

        try:
            script_tag = response.css('script:contains("HYDRATION_STATE") ::text').re_first(
                r'designersByLetter(.*?)"context')
            script_tag = script_tag.replace('\\', '').replace('":{', '{').replace(']},', ']}')
            brand_dit = json.loads(script_tag)
        except json.JSONDecodeError as e:
            print(f'Json Error: {e} URL:{response.url}')
            return

        for alphabet_brands, brands in brand_dit.items():
            for brand in brands:
                brand_name = brand.get('name', '')
                brand_url = brand.get('href', '')
                url = f'https://www.farfetch.com{brand_url}'
                proxy_url = self.proxies['https']
                yield Request(url,
                              callback=self.parse_brand, dont_filter=True,
                              meta={"handle_httpstatus_all": True})

    def parse_brand(self, response):
        unaut_text = 'Unauthorized request, please make sure your API key is valid.'
        if unaut_text== response.text:
            print('Proxy Request make issues: URL:', response.url)
            return

        try:
            product_dict = json.loads(response.css('script:contains("ItemList") ::text').get(''))
        except json.JSONDecodeError as e:
            self.write_logs(f'Not any Brand Exist: URL{response.url}')
            product_dict = {}
            return

        products = product_dict.get('itemListElement', [])

        if products:
            self.category_item_found += len(products)
            self.categories_item_found += len(products)

        for product in products:
            prod_url = product.get('offers', {}).get('url', '')
            url = urljoin(response.url, prod_url)
            proxy_url = self.proxies['http'] if 'http:' in url else self.proxies['https']
            yield Request(url, callback=self.parse_product_detail,
                          meta={"handle_httpstatus_all": True, 'product':product})

    def parse_product_detail(self, response):
        try:
            info_dict = json.loads(response.css('script[type="application/ld+json"]:contains("Product") ::text').get(''))
            cat_dict = json.loads(response.css('script:contains("BreadcrumbList") ::text').get(''))
            cat_dict = cat_dict.get('itemListElement', [])
        except json.JSONDecodeError as e:
            print(f'Product Information error: {e}')
            info_dict = {}
            cat_dict = {}

        try:
            original_price = response.css('[data-component="PriceOriginal"] ::text').re_first(r'\d[\d,]*')
            original_price= original_price.replace(',', '') if original_price else 0
            current_price = info_dict.get('offers', {}).get('price', 0)
            no_items =  response.css('.ltr-knpsgl p::text').re_first(r'\d[\d,]*')
            no_items =  no_items.replace(',', '') if no_items else ''

            item = OrderedDict()
            item['Source ID'] = 'FarFetch'
            item['Product URL'] = response.url
            item['Brand'] = info_dict.get('brand', {}).get('name', '')
            item['Product Title'] = info_dict.get('name', '')
            item['Product ID'] = info_dict.get('productID', '')
            item['Category'] = ', '.join([cat.get('item', {}).get('name', '') for cat in cat_dict])
            item['Price'] = str(current_price)
            item['Discount'] = int(original_price) - int(current_price) if int(original_price)> int(current_price) else ''
            item['Currency'] = info_dict.get('offers', {}).get('priceCurrency', '')
            item['Description'] = self.get_description(response, tag='.exjav154 > div')
            item['Main Image URL'] = ''.join([img.get('contentUrl', '') for img in info_dict.get('image', [])][0:1])
            item['Other Images URL'] = [img.get('contentUrl', '') for img in info_dict.get('image', [])][1:5]
            item['Colors'] = info_dict.get('color', '')
            item['Variations'] = ''
            item['Sizes'] = ''
            item['Other Details'] = ''
            instock = info_dict.get('offers', {}).get('availability', '')
            item['Availability'] = 'In Stock' if instock and instock is not None else 'Out of Stock'
            item['Number of Items in Stock'] = int(no_items) if no_items and int(no_items)!=current_price else 0
            item['Last Update'] = ''
            item['Creation Date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            self.category_item_scraped += 1
            self.categories_item_scraped += 1
            self.read_write_json_file(record=item, key='scrape_record')
        except Exception as e:
            self.write_logs(f'Error parsing item:{e} URL:{response.url}')
            a=1

    # def get_description(self, response):
    def get_description(self,response, tag):
        text = []
        # tags = response.css('.exjav154 > div')
        tags = response.css(f'{tag}')
        # If tags list is empty, return an empty string
        if not tags:
            return ''

        for tag in tags:
            if 'data-component="Img"' in tag.get():
                continue
            tag_texts = tag.xpath('.//text()[not(ancestor::style)]').getall()
            tag_texts = '\n'.join(tag_texts)
            text.append(''.join(tag_texts))

        return '\n'.join(text)

    def read_write_json_file(self, record=None, key=None):
        if key=='scrape_record':
            self.current_records.append(record)
        if len(self.current_records)>=10 or key=='close_spider':
            try:
                with open(self.output_file_path, 'w', encoding='utf-8') as f:
                    json.dump(self.current_records, f, ensure_ascii=False, indent=4)
                    return
            except FileNotFoundError:
                return {}

    def write_logs(self, log_msg):
        with open(self.logs_filepath, mode='a', encoding='utf-8') as logs_file:
            logs_file.write(f'{log_msg}\n')
            print(log_msg)

    def close(spider, reason):
        spider.read_write_json_file(record=None, key='close_spider')

        # Log overall scraping statistics
        spider.write_logs(f"\n--- Scraping Summary ---")
        spider.write_logs(f"Total Products Available on Website: {spider.categories_item_found}")
        spider.write_logs(f"Total Products Successfully Scraped: {spider.categories_item_scraped}")

        # Log script execution times
        spider.write_logs(f"\n--- Script Execution Times ---")
        spider.write_logs(f"Script Start Time: {spider.script_starting_datetime}")
        spider.write_logs(f"Script End Time: {datetime.now().strftime('%d-%m-%Y %H:%M:%S')}")
        spider.write_logs(f"Reason for Closure: {reason}")

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(FarfetchSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider

    def spider_idle(self):
        if  self.current_category:
            self.write_logs(f"Total Products Available on Category: {self.current_category} Are:{self.category_item_found}")
            self.write_logs(f"Total Products Scraped on Category: {self.current_category} Are:{self.category_item_scraped}")
            self.category_item_found = 0
            self.category_item_scraped = 0

        if self.categories:
            # Log progress
            self.write_logs(f"\n\n{len(self.categories)}/{self.count_categories} Categories left to Scrape\n\n")

            self.current_category = self.categories.pop()
            self.write_logs(f"\n\n{ self.current_category.title()} Category is Starting to Scrape\n\n")

            url = f'https://www.farfetch.com/sa/designers/{ self.current_category}'
            # self.crawler.engine.crawl(Request(url, callback=self.parse_category_brands,
            #                                   dont_filter=True, meta={"handle_httpstatus_all": True}))

            # Pass the spider instance (self) explicitly
            self.crawler.engine.crawl(
                Request(url, callback=self.parse_category_brands, dont_filter=True,
                        meta={"handle_httpstatus_all": True}),
                spider=self
            )