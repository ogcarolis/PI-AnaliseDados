import json
import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
import time
import random
from datetime import datetime

class TerabyteBase():
    # URL da API que você quer acessar
    domain = "https://www.terabyteshop.com.br"
    
    urls= []

    pages = [
        '/produto/25235/placa-de-video-gigabyte-nvidia-geforce-rtx-4060-windforce-oc-8gbgddr6-dlss-ray-tracing-gv-n4060wf2oc-8gd',
        '/produto/20843/pc-gamer-custo-beneficio-do-ano-2024-amd-ryzen-5-5600x-nvidia-geforce-rtx-3060-16gb-2x8gb-ddr4-ssd-512gb',
        '/produto/21631/placa-de-video-inno3d-geforce-rtx-3060-twin-x2-12gb-gddr6-dlss-ray-tracing-n30602-12d6-119032ah',
        '/produto/20788/processador-amd-ryzen-5-5600-35ghz-44ghz-turbo-6-cores-12-threads-cooler-wraith-stealth-am4-100-100000927box',
        '/produto/12330/memoria-ddr4-geil-evo-potenza-8gb-3000mhz-black-gapb48gb3000c16asc',
        '/produto/26887/ssd-wd-green-sn350-500gb-m2-nvme-leitura-2400mbs-e-gravacao-1500mbs-wds500g2g0c',
        '/produto/28945/ssd-crucial-bx500-500gb-sata-iii-leitura-550mbs-gravacao-500mbs-ct500bx500ssd1',
        '/produto/27842/placa-de-video-gigabyte-nvidia-geforce-rtx-4060-ti-windforce-8gb-gddr6-dlss-ray-tracing-gv-n406twf2-8gd',
        '/produto/24814/fonte-duex-600fse-600w-80-plus-bronze-pfc-ativo-dx-600fse',
        '/produto/25374/placa-de-video-msi-nvidia-geforce-rtx-4060-ti-ventus-2x-black-oc-8gb-gddr6-dlss-ray-tracing-912-v515-024',
        '/produto/25474/placa-de-video-pny-nvidia-geforce-rtx-4060-xlr8-gaming-verto-epic-x-rgb-8gb-gddr6-dlss-ray-tracing-vcg40608tfxxpb1',
        '/produto/26103/memoria-ddr4-xpg-gammix-d10-16gb-3200mhz-cl16-black-ax4u320016g16a-sb10',
        '/produto/16830/memoria-ddr4-geil-orion-rgb-edicao-amd-16gb-3000mhz-gray-gaosg416gb3000c16asc',
        '/produto/20072/processador-intel-core-i5-12400f-25ghz-44ghz-turbo-12-geracao-6-cores-12-threads-lga-1700-bx8071512400f',
        '/produto/27329/processador-amd-ryzen-7-5700x3d-30ghz-41ghz-turbo-8-cores-16-threads-am4-sem-cooler-100-100001503wof',
        '/produto/27314/processador-amd-ryzen-5-5600gt-36ghz-46ghz-turbo-6-cores-12-threads-cooler-wraith-stealth-am4-100-100001488box',
        '/produto/26271/processador-intel-core-i5-14600k-35-ghz-53ghz-turbo-14-geracao-14-cores-20-threads-lga-1700-bx8071514600k'
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
        time.sleep(random.uniform(3, 7))  # Wait for everything to load
    
    def init_scraper(self):
        results = []
        
        for url in self.urls:

            try:
                
                time.sleep(random.uniform(3, 7))
                
                result = self.scrap_page(url)
                
                results.append(result)

                print(f"Successfully retrieved data for URL: {url}")
                
            except Exception as e:
                print(f"Error processing URL {url}: {str(e)}")
        
        # Save results
        if results:
            with open(f'terabyte_{datetime.today().strftime('%Y-%m-%d')}.json', 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
        
        return results
    
    def scrap_page(self, page):
        self.driver.get(page)
        time.sleep(random.uniform(4, 7))
        
        nome = self.driver.find_element(By.CSS_SELECTOR, ".AreaInfvlrpdt > h1.tit-prod").text
        
        try:
            infos = self.driver.find_element(By.CSS_SELECTOR, ".info-det-prod > .info-prod.info-price")
            preco_total = infos.find_element(By.CSS_SELECTOR, ".pull-left > p > del").text
            preco_avista = infos.find_element(By.CSS_SELECTOR, "#valVista").text
            preco_parcelado = infos.find_element(By.CSS_SELECTOR, "#valParc").text
            preco_parcelado_vezes = infos.find_element(By.CSS_SELECTOR, "#nParc").text 
        
            produto = {
                'url': page,
                'nome': nome,
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
        except:
            return {
                'url': page,
                'nome': nome,
                'preco_total': 0,
                'preco_avista': 0,
                'preco_parcelado': 0,
                'preco_parcelado_vezes': 0
            }
                
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
