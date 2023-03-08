import scrapy

class MySpider(scrapy.Spider):
    name = 'myspider'
    start_urls = ['https://www.example.com']

    def parse(self, response):
        # This method is called when the spider receives a response for the request
        # made to the start_urls.

        # You can capture the response body as text
        response_text = response.text
        print(response_text)

        # You can also capture the response headers
        response_headers = response.headers
        print(response_headers)


console command:
scrapy runspider myspider.py
