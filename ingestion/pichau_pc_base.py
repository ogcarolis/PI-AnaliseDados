import json
import pandas as pd
import undetected_chromedriver as uc
import time
import random

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
    
    def __init__(self):
        self.driver = None


    def setup_session(self):
        # Initialize undetected-chromedriver
        options = uc.ChromeOptions()
        #options.add_argument('--headless')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-webgl')
        options.add_argument('--disable-webrtc')
        options.add_argument('--disable-gpu')
        
        self.driver = uc.Chrome(options=options)
        
        self.driver.set_window_position(0, 0)
        # Visit the website to get cookies
        self.driver.get('https://www.pichau.com.br')
        time.sleep(random.uniform(4, 7))  # Wait for everything to load

        # Get cookies from browser and add to session
        # cookies = self.driver.get_cookies()
        # for cookie in cookies:
        #     self.session.cookies.set(
        #         name=cookie['name'], 
        #         value=cookie['value'], 
        #         domain=cookie.get('domain', ''),
        #         path=cookie.get('path', '/')
        #     )
        
        self.headers = {
            'Accept': '*/*',
            'User-Agent': self.driver.execute_script('return navigator.userAgent'),
            'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'authorization': '',
            'priority': 'u=1, i',
            'sec-ch-ua': '"Not)A;Brand";v="99", "Opera GX";v="113", "Chromium";v="127"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'vendor': 'Pichau',
            'Content-Type': 'application/json',
            "Accept-Encoding": "gzip, deflate"
        }
        
        print(self.headers)
    
    def make_requests(self):
        results = []
        
        for sku in self.sku_list:
            body = {
                "operationName": "productDetail",
                "variables": {"sku": sku },
                "query": """query productDetail($sku: String) {
                    productDetail: products(filter: {sku: {eq: $sku}}) {
                        items {
                            __typename
                            sku
                            name
                            only_x_left_in_stock
                            stock_status
                            special_price
                            mysales_promotion {
                                expire_at
                                price_discount
                                price_promotional
                                promotion_name
                                promotion_url
                                qty_available
                                qty_sold
                                __typename
                            }
                            pichau_prices {
                                avista
                                avista_discount
                                avista_method
                                base_price
                                final_price
                                max_installments
                                min_installment_price
                                __typename
                            }
                            price_range {
                                __typename
                            }
                            ... on BundleProduct {
                                dynamic_sku
                                dynamic_price
                                dynamic_weight
                                price_view
                                ship_bundle_items
                                options: items {
                                    option_id
                                    title
                                    required
                                    type
                                    position
                                    sku
                                    value: options {
                                        id
                                        uid
                                        quantity
                                        position
                                        is_default
                                        price
                                        price_type
                                        can_change_quantity
                                        title: label
                                        product {
                                            id
                                            name
                                            sku
                                            url_key
                                            stock_status
                                            slots_memoria
                                            portas_sata
                                            image {
                                                url
                                                url_listing
                                                path
                                                label
                                                __typename
                                            }
                                        __typename
                                        }
                                        __typename
                                    }
                                    __typename
                                }
                                __typename
                            }
                        }
                        __typename
                    }
                }"""
            }

            try:
                
                time.sleep(random.uniform(3, 7))
                
                # response = self.session.post(
                #     self.url,
                #     headers=self.headers,
                #     json=body
                # )
                
                script = f"""
                    return fetch('{self.url}', {{
                        'method': 'POST',
                        'headers': {self.headers},
                        'body': JSON.stringify({json.dumps(body)}),
                        "referrerPolicy": "strict-origin-when-cross-origin",
                        "mode": "cors",
                        "credentials": "include"
                    }}).then(response => response.json())
                    .then(data => data);
                """
                
                result = self.driver.execute_script(script)
                
                results.append(result['data']['productDetail']['items'][0])
                
                # if response.status_code == 200:
                #     data = response.json()
                #     results.append(data)
                #     print(f"Successfully retrieved data for SKU: {sku}")
                # else:
                #     print(f"Error for SKU {sku}: Status code {response.status_code}")
                print(f"Successfully retrieved data for SKU: {sku}")
                
                time.sleep(random.uniform(3, 6))  # Small delay between requests
                
            except Exception as e:
                print(f"Error processing SKU {sku}: {str(e)}")
        
        # Save results
        if results:
            with open('pichau_products.json', 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
        
        return results
    
    # @classmethod
    # def make_requests(cls):
    #     for body in cls.body_list:
    #         payload = body
    #         response = requests.request("POST", cls.url, headers=cls.headers, data=payload)

    #         if response.status_code == 200:
    #             print(response)
    #             data_df = pd.DataFrame(response)
    #             data_df.to_json(orient='records')
    #         else:
    #             print(f"Erro na requisição: {response.status_code}.")
                
    def run(self):
        try:
            print("Setting up session with browser...")
            self.setup_session()
            print("Making API requests...")
            return self.make_requests()
        finally:
            if self.driver:
                self.driver.quit()

if __name__ == "__main__":
    scraper = PichauBase()
    result = scraper.run()
    print(result)
