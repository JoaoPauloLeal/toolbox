from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options as ChromeOptions
import time
import json


def iniciar_processo_busca(params_exec, *args, **kwargs):
    options = ChromeOptions()
    options.add_argument("--headless")
    capabilities = DesiredCapabilities.CHROME
    capabilities["goog:loggingPrefs"] = {"performance": "ALL"}
    driver = webdriver.Chrome(
        r"packages/tools/arqjob/drivers/chromedriver.exe",
        desired_capabilities=capabilities, options=options
    )
    driver.get("https://cloud.betha.com.br/")
    username = driver.find_element_by_id("login:iUsuarios")
    password = driver.find_element_by_id("login:senha")
    username.send_keys(params_exec['user_login'])
    password.send_keys(params_exec['user_pass'])
    driver.find_element_by_xpath("//a[contains(@onclick,'login:btAcessar')]").click()
    time.sleep(10)
    driver.find_element_by_link_text(params_exec['entidade']).click()
    time.sleep(8)
    for item in driver.find_elements_by_class_name('PLA'):
        if item.text == 'CONTABILIDADE':
            item.click()
            break
    time.sleep(10)
    driver_log = driver.get_log('performance')
    events = [process_driver_log_entry(entry) for entry in driver_log]
    events = [event for event in events if 'Network.response' in event['method']]
    count = 0
    access_token = 'Não encontrado'
    for item in events:
        if item['method'] == 'Network.responseReceived':
            if 'https://plataforma-licencas.betha.cloud/licenses/v0.1/api-suite/entidade/' in str(item['params']['response']['url']):
                result = str(item['params']['response']['url']).index('access_token=')
                access_token = str(item['params']['response']['url'])[result+13:len(str(item['params']['response']['url']))]
                count += 1

    print(f"Bearer {access_token}")


def process_driver_log_entry(entry):
    response = json.loads(entry['message'])['message']
    return response
