import json
from bs4 import BeautifulSoup
import scrapy

class KabumScrapper(scrapy.Spider):
    name = 'kabum_cpu_base_scrapper'
    domain = "https://www.kabum.com.br"

    code_cpu = [
        520369,
        320799,
        102248,
        426261,
        283718,
        497575,
        283719,
        497573,
        521362,
        496109,
        382745,
        384627,
        174762,
        469132
    ]

    start_urls = []

    for code in code_cpu:
        start_urls.append("https://www.kabum.com.br/produto/" + str(code))

    def parse(self, response: scrapy.http.HtmlResponse):
        # Extraindo o conteúdo do script com dados
        soup = BeautifulSoup(response.body, 'lxml')
        pc = soup.find('script', {'id': '__NEXT_DATA__', 'type': 'application/json'})

        if pc:
            # Convertendo o JSON extraído para um dicionário Python
            pc_data = json.loads(pc.string)

            # Acessando os detalhes do produto (ajustar conforme a estrutura JSON)
            product_info = pc_data['props']['pageProps']['data']['productCatalog']
            product_info = json.loads(product_info)

            # Extraindo informações específicas como nome, preço, etc.
            code = product_info['code']
            name = product_info['name']
            old_price = product_info['price']
            price = product_info['priceMarketplace']
            max_installment = product_info['maxInstallment']

            if price == 0:
                price = old_price

            yield {
                "code": code,
                "name": name,
                "old_price": old_price,
                "price": price,
                "max_installment": max_installment
            }
