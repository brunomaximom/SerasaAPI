from datetime import timedelta
from flask import Flask, request, jsonify
from selenium.webdriver import Chrome
from selenium.webdriver.common.keys import Keys
import time, pandas, redis

def common(driver, xpath, flag, region):
    """Função de eventos em comum usadas pelo crawler

    Args:
        driver (selenium.webdriver.chrome.webdriver.WebDriver): driver do selenium para Chromium ou Chrome
        xpath (string): xpath do elemento HTML que se deseja operar
        flag (string): define qual evento deve ser realizado
        region (string): grava qual país o usuário quer buscar
    """
    time.sleep(6)       # necessário para garantir que todos os eventos tenham sido realizados adequadamente
    elem = driver.find_element_by_xpath(xpath)
    
    # caso nenhuma região seja passada, manter United States
    if region == None or region == "":
        region = "United States"        # United States é o padrão
    
    # escolher o evento adequado
    if flag == 'click':
        elem.click()
    if flag == 'send_keys':
        elem.send_keys(region)

def crawler(region):
    """ Essa função realiza todos os eventos para obtenção dos dados da tabela
        do site https://finance.yahoo.com/screener/new. Por fim ela salva os 
        dados num servidor Redis local e retorna um json desses dados.
        
    Args:
        region (string): grava qual país o usuário quer buscar

    Returns:
        json: Todos os dados encontrados no site no país escolhido
    """
    driver = Chrome()
    driver.get("https://finance.yahoo.com/screener/new")
    driver.maximize_window()
    conn = redis.StrictRedis(host='localhost', decode_responses=True)
    
    while conn.ttl("SerasaAPI") >= 0:
        print("Cache cheia")
    
    # Remover United States ou mantê-lo caso nenhuma região seja passada
    common(driver, "//button[@class='Bd(0) Pb(8px) Pt(6px) Px(10px) M(0) D(ib) C($primaryColor) filterItem:h_C($primaryColor) Fz(s)']", 'click', region)
    
    # Abrir a seleção de região
    common(driver, "//div[@class='D(ib) Pt(6px) Pb(7px) Pstart(6px) Pend(7px) C($tertiaryColor) Fz(s) Cur(p)']", 'click', region)
    
    # Digitar a região específica
    common(driver, "//input[@class='Bd(0) H(28px) Bgc($lv3BgColor) C($primaryColor) W(100%) Fz(s) Pstart(28px)']", 'send_keys', region)
    
    # Clicar na região escolhida
    common(driver, "//label[@class='Ta(c) Pos(r) Va(tb) Pend(10px)']", 'click', region)
    
    # Clicar no botão Find Stocks
    common(driver, "//button[@class='Bgc($linkColor) C(white) Fw(500) Px(20px) Py(9px) Bdrs(3px) Bd(0) Fz(s) D(ib) Whs(nw) Miw(110px) Bgc($linkActiveColor):h']", 'click', region)
    
    # Garantir que a nova página seja totalmente carregada
    time.sleep(5)
    
    # Obter o número em estoque para saber a quantidade de páginas a iterar
    elem = driver.find_element_by_xpath("//span[@class='Mstart(15px) Fw(500) Fz(s)']")
    elem = elem.find_element_by_tag_name("span").get_attribute('innerHTML')
    nstocks = elem.split(' ')[2]        # Número no estoque
    
    offset = 0
    current_url = driver.current_url
    
    # Criar o JSON a partir do dataframe e salva na cache
    # Os dataframes são criados para cada página da tabela, por isso o loop
    while offset <= int(nstocks):
        driver.get(current_url+"?offset="+str(offset)+"&count=250")     # recarregar a página com os parâmetros adequaddos
        dataframes = pandas.read_html(driver.current_url)
        dataframe_values = dataframes[0][['Symbol', 'Name', 'Price (Intraday)']]
        dataframe_keys = list(dataframes[0]['Symbol'])
        json_list = dataframe_values.to_json(orient='records', lines=True).splitlines()    #lista de cada elemento contido no json
        json_final = {el:0 for el in dataframe_keys}        # criar o JSON com as chaves certas e valores zerados
        json_final.update(zip(json_final, json_list))       # atualiza os valores para os valores certos
        offset += 250
        conn.hmset("SerasaAPI", json_final)     # gravar cada página da tabela no Redis
    
    conn.expire("SerasaAPI", timedelta(seconds=193))    # expirar os dados importados a cada 3m13s
    json_final = conn.hgetall("SerasaAPI")      # recuperar todos os dados do Redis para retorná-los juntos
    
    # garantindo que todas as variáveis criadas e não mais necessárias sejam liberadas
    del dataframes
    del dataframe_values
    del dataframe_keys
    
    return jsonify(json_final)


app = Flask(__name__)
app.config["DEBUG"] = True

@app.route('/', methods=['GET'])
def home():
    return "<h1>Prova técnica Serasa Experian</h1><p>Adicione o endpoint /stocks na URL</p>"


@app.route('/stocks', methods=['GET'])
def api():
    return crawler(request.args.get('region'))

app.run()