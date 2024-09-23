import requests
import json
import pandas as pd

class PichauBase():
    # URL da API que você quer acessar
    url = "https://www.pichau.com.br/api/catalog"

    sku_list = [
        'PCM-Pichau-Gamer-50695',
        '100-100000927BOX',
        'BX8071514400',
        'BX8071514100',
        '100-100001488BOX',
        '100-100001503WOF',
        'BX8071512400F-BR',
        'CMK16GX4M2D3000C16',
        'CMW16GX4M2D3600C18W',
        'CMT16GX4M2D3600C18W',
        'GV-N4060WF2OC-8GD',
        'GV-N406TWF2OC-8GD',
        'VCG40608DFXPB1-O',
        '912-V515-098'
    ]

    body_list = []

    for sku in sku_list:
        body_item = "{\"operationName\": \"productDetail\",\"variables\": {\"sku\":\"" + sku + "}, \"query\": \"query productDetail($sku: String) {\n  productDetail: products(filter: {sku: {eq: $sku}}) {\n    items {\n      __typename\n      sku\n      name\n      only_x_left_in_stock\n      stock_status\n      special_price\n      mysales_promotion {\n        expire_at\n        price_discount\n        price_promotional\n        promotion_name\n        promotion_url\n        qty_available\n        qty_sold\n        __typename\n      }\n      pichauUlBenchmarkProduct {\n        overallScore\n        scoreCPU\n        scoreGPU\n        games {\n          fullHdFps\n          medium4k\n          quadHdFps\n          title\n          ultra1080p\n          ultra4k\n          __typename\n        }\n        __typename\n      }\n      pichau_prices {\n        avista\n        avista_discount\n        avista_method\n        base_price\n        final_price\n        max_installments\n        min_installment_price\n        __typename\n      }\n      price_range {\n        __typename\n      }\n      ... on BundleProduct {\n        dynamic_sku\n        dynamic_price\n        dynamic_weight\n        price_view\n        ship_bundle_items\n        options: items {\n          option_id\n          title\n          required\n          type\n          position\n          sku\n          value: options {\n            id\n            uid\n            quantity\n            position\n            is_default\n            price\n            price_type\n            can_change_quantity\n            title: label\n            product {\n              id\n              name\n              sku\n              url_key\n              stock_status\n              slots_memoria\n              portas_sata\n              image {\n                url\n                url_listing\n                path\n                label\n                __typename\n              }\n              __typename\n            }\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n    }\n    __typename\n  }\n}\n\"}"
        body_list.append(body_item)

    headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'max-age=0',
    'cookie': 'CookieConsent=true; cf_clearance=n3Pc4QgTlaH22Qv36yVZq0XlQHJD1rawp6j8ODzBfRU-1727040730-1.2.1.1-Bj0CSJ8UEbP6JzRCIkNA4t7i5FowdkahNhvCYcZs6uQH5Gk2OvRIAD6NUm3mCTFLcRzsj0eG8FHtUNpu.yoXchsn5cn3ojIbta.VA2Wy8hnOac0jWPTNj3NOz5VAPG0vrsfi4sJxMkc.80lGBQtYXBbV57SEV2mcDIVEyWLe5fdP9UozX5dEiW_KBcMSBqUHTh6sZB0WE.Jd6VRzgvM.c6TWW5rpSN9vcYR3kOnQm4o9WZtHc74AXeiYtKGREsDfBa9l5foK4AeJoJUVnqAyIqnUdPoTqOHWxe1DxmuwB.t5Ygy0pYFZ8HuV6bMIZ0Kl5apjqExGTLvdOhnkPmNKAYs3C.g9hlZk_e9Yy_n5gXMHXuTqYSpLDowHxEzNjMHi.8k2DIaNQBcRy49Mkqb2lD6WfhqznKcqUpn4AJchhyk; __cf_bm=vPirwtSfBvQI549glI3VVu92Twsji8DXLA.o0VmK1cM-1727043665-1.0.1.1-ZXGSBGCEKn5pPUUK8x8pIEZFk7w9ZxvKtP0uqRuswG1s.qRGjmkN3cXSRKyBNlIIpB_0zXiEtJGPIoLaPo9tsQ',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Not)A;Brand";v="99", "Opera GX";v="113", "Chromium";v="127"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 OPR/113.0.0.0',
    'Referer': 'https://www.pichau.com.br',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 OPR/113.0.0.0',
    'Origin': 'https://www.pichau.com.br',
    'authorization': '',
    'content-type': 'application/json',
    'origin': 'https://www.pichau.com.br',
    'referer': 'https://www.pichau.com.br/pc-gamer-pichau-arquetu-amd-ryzen-5-5600-geforce-rtx-4060-8gb-16gb-ddr4-ssd-480gb-50695',
    'vendor': 'Pichau',
    'cachettl': '76800',
    'if-none-match': 'W/"24a-qxS3F0FyEAdXCooDWpyX/IVq6Vg"',
    'pichaucachekey': 'ratings-review',
    'if-modified-since': 'Sat, 21 Sep 2024 09:56:23 GMT',
    'Accept': '*/*',
    'Service-Worker': 'script',
    'pragma': 'no-cache'
    }

    @classmethod
    def make_requests(cls):
        for body in cls.body_list:
            payload = body
            response = requests.request("POST", cls.url, headers=cls.headers, data=payload)

            if response.status_code == 200:
                print(response)
                data_df = pd.DataFrame(response)
                data_df.to_json(orient='records')
            else:
                print(f"Erro na requisição: {response.status_code}.")

if __name__ == "__main__":
    PichauBase.make_requests()
