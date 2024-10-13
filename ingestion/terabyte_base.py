import json
import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
import time
import random

class TerabyteBase():
    # URL da API que você quer acessar
    domain = "https://www.terabyteshop.com.br"
    
    urls= []

    pages = [
        '/produto/20843/pc-gamer-custo-beneficio-do-ano-2024-amd-ryzen-5-5600x-nvidia-geforce-rtx-3060-16gb-2x8gb-ddr4-ssd-512gb',
        '/produto/21631/placa-de-video-inno3d-geforce-rtx-3060-twin-x2-12gb-gddr6-dlss-ray-tracing-n30602-12d6-119032ah'
    ]
    
    def __init__(self):
        self.driver = None
        self.urls = map(lambda page: self.domain + page, self.pages)


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
        self.driver.get(self.domain)
        time.sleep(random.uniform(4, 7))  # Wait for everything to load
    
    def init_scraper(self):
        results = []
        
        for url in self.urls:

            try:
                
                time.sleep(random.uniform(3, 7))
                
                result = self.scrap_page(url)
                
                results.append(result)

                print(f"Successfully retrieved data for URL: {url}")
                
                time.sleep(random.uniform(3, 6))  # Small delay between requests
                
            except Exception as e:
                print(f"Error processing URL {url}: {str(e)}")
        
        # Save results
        if results:
            with open('terabyte_products.json', 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
        
        return results
    
    def scrap_page(self, page):
        self.driver.get(page)
        time.sleep(random.uniform(4, 7))
        
        infos = self.driver.find_element(By.CSS_SELECTOR, ".info-det-prod > .info-prod.info-price")
        preco_total = infos.find_element(By.CSS_SELECTOR, ".pull-left > p > del").text
        preco_avista = infos.find_element(By.CSS_SELECTOR, "#valVista").text
        preco_parcelado = infos.find_element(By.CSS_SELECTOR, "#valParc").text
        preco_parcelado_vezes = infos.find_element(By.CSS_SELECTOR, "#nParc").text
        
        produto = {
            'url': page,
            'preco_total': preco_total,
            'preco_avista': preco_avista,
            'preco_parcelado': preco_parcelado,
            'preco_parcelado_vezes': preco_parcelado_vezes
        }
        
        componentes = self.driver.find_elements(By.CSS_SELECTOR, ".custom-pc-container > #containerpers > .cardPc:not(.hidden)");
        
        pc_componentes = []
        
        for componente in componentes:
            
            nome_prod = componente.find_element(By.CSS_SELECTOR, "#prodDesc").text
            preco_avista_pod = componente.find_element(By.CSS_SELECTOR, ".cardPC_preco_av").text
            preco_parcelado_pod = componente.find_element(By.CSS_SELECTOR, ".cardPC_preco_parc").text
            
            pc_componentes.append({
                'nome': nome_prod, 
                'preco_avista': preco_avista_pod, 
                'preco_parcelado': preco_parcelado_pod
            })
            
        if len(pc_componentes) > 0:
            produto['componentes'] = pc_componentes
        
        return produto
                
    def run(self):
        try:
            print("Setting up session with browser...")
            self.setup_session()
            print("Making API requests...")
            return self.init_scraper()
        finally:
            if self.driver:
                self.driver.quit()

if __name__ == "__main__":
    scraper = TerabyteBase()
    result = scraper.run()
    print(result)
