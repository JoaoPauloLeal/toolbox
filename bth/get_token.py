from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options as ChromeOptions
import time
import json

"""ATUALMENTE FUNCIONA SOMENTE PARA CONVENIOS, QUANDO IMPLEMENTADO UMA SEGUNDA AUTOMAÇÃO SERA IMPLEMENTADA UMA 
SEPARAÇÃO """


def get_token_access(params_exec, *args, **kwargs):
    token = None
    try:
        options = ChromeOptions()
        options.add_argument("--headless")

        capabilities = DesiredCapabilities.CHROME
        capabilities["goog:loggingPrefs"] = {"performance": "ALL"}

        driver = webdriver.Chrome(
            r"packages/convenios_cloud_sybase/drivers/chromedriver.exe",
            desired_capabilities=capabilities, options=options
        )
        driver.get("https://cloud.betha.com.br/")
        username = driver.find_element_by_id("login:iUsuarios")
        password = driver.find_element_by_id("login:senha")
        username.send_keys(params_exec['user_login'])
        password.send_keys(params_exec['user_pass'])
        driver.find_element_by_xpath("//a[contains(@onclick,'login:btAcessar')]").click()
        time.sleep(10)
        driver.find_element_by_link_text(params_exec['nome_entidade']).click()
        time.sleep(8)
        for item in driver.find_elements_by_class_name('PLA'):
            if item.text == 'CONVÊNIOS':
                item.click()
                break
        time.sleep(10)
        driver_log = driver.get_log('performance')
        events = [process_driver_log_entry(entry) for entry in driver_log]
        events = [event for event in events if 'Network.response' in event['method']]
        count = 0
        for item in events:
            if item['method'] == 'Network.responseReceived':
                if str(item['params']['response']['url']) == 'https://convenios.cloud.betha.com.br/common/geral/geral.html':
                    token = {
                        'authorization': item['params']['response']['requestHeaders']['authorization'],
                        'user-access': item['params']['response']['requestHeaders']['user-access']
                    }
                count += 1
    except Exception as error:
        print(f'Erro ao consultar token". {error}')
    finally:
        return token


def process_driver_log_entry(entry):
    response = json.loads(entry['message'])['message']
    return response
