import pandas as pd
import requests
from datetime import datetime
import calendar
import schedule
import time

# Funcao para converter colunas para tipo numerico
def converter_colunas_numerico(df, colunas):
    for coluna in colunas:
        df[coluna] = pd.to_numeric(df[coluna], errors='coerce')
    return df

# Funcao para converter colunas para tipo data
def converter_colunas_data(df, colunas):
    for coluna in colunas:
        df[coluna] = pd.to_datetime(df[coluna], errors='coerce').dt.strftime('%d/%m/%Y')
    return df

# Apikey
apikey = "APIKEY"

# Cria dataframes para os dados
dataframes_notas = []

# Inicializa a variavel de controle da pagina
page = 1

# Obtem o ultimo dia do mes atual
hoje = datetime.now().date()

# Obtem o primeiro dia do mes anterior
primeiro_dia_mes_anterior = datetime(hoje.year, hoje.month - 1, 1).strftime("%d/%m/%Y")

# Obtem o ultimo dia do mes atual
ultimo_dia_mes_atual = calendar.monthrange(hoje.year, hoje.month)[1]
ultimo_dia_mes_atual = datetime(hoje.year, hoje.month, ultimo_dia_mes_atual).strftime("%d/%m/%Y")

def processar_dados ():

    # Chama as variaveis para dentro da funcao
    global page, dataframes_notas

    # Obtem dados das notas fiscais de todas as paginas
    while True:
        # Usando API do ERP bling como exemplo - Pegando as notas fiscais
        url = f"https://bling.com.br/Api/v2/notasfiscais/page={page}/json/"

        # Usando como parametro a chave API (APIKEY) e o filtro, que seria o mes anterior e o mes atual
        params = {
            "apikey": apikey,
            "filters": f"dataEmissao[{primeiro_dia_mes_anterior} TO {ultimo_dia_mes_atual}]"
        }

        response = requests.get(url, params=params)
        data = response.json()

        # Verifica se a resposta contem dados
        if "retorno" in data and "notasfiscais" in data["retorno"]:
            notasfiscais = data["retorno"]["notasfiscais"]
            df_notas = pd.json_normalize(notasfiscais)

            # Adiciona o dataframe das notas fiscais a lista de dataframes
            dataframes_notas.append(df_notas)

            # Incrementa a variavel de controle da pagina
            page += 1
        else:
            break
    # Concatena os dataframes das notas fiscais em um unico dataframe
    df_notas = pd.concat(dataframes_notas)

    # Excluir as colunas das notas fiscais
    excluir_coluna_notas = ["coluna_1"]
    df_notas.drop(columns=excluir_coluna_notas, inplace=True)

    # Altera colunas das notas fiscais para tipo numerico
    conv_colunas_numerico_notas = ["coluna_2"]
    df_notas = converter_colunas_numerico(df_notas, conv_colunas_numerico_notas)

    # Altera colunas das notas fiscais para tipo data
    conv_colunas_data_notas = ["coluna_3"]
    df_notas = converter_colunas_data(df_notas, conv_colunas_data_notas)

    # Salvar o dataframe como um arquivo XLSX
    nome_arquivo = "arquivo.xlsx"
    diretorio_arquivo = r"C:/Users/..."
    df_notas.to_excel(diretorio_arquivo + nome_arquivo, index=False)

# Agendar a execução da função buscar_e_processar_dados a cada 30 minutos
schedule.every(2).minutes.do(processar_dados)

# Executar o agendador continuamente
while True:
    schedule.run_pending()
    time.sleep(120)