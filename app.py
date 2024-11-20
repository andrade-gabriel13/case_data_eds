import pandas as pd
import requests
import matplotlib.pyplot as plt
from collections import Counter
from datetime import datetime



# Código para criação de novo schema e tabela problema 1
def create_table_stg_prontuario():
    return """ 
        CREATE SCHEMA IF NOT EXISTS stg_prontuario;

        CREATE TABLE stg_prontuario.PACIENTE (
            id INT PRIMARY KEY,
            nome VARCHAR(255),
            dt_nascimento DATE,
            cpf INT,
            nome_mae VARCHAR(255),
            dt_atualizacao TIMESTAMP
        );
    """

# codigo para copiar os dados problema 2
def insert_data_table_stg_prontuario():
    return """ 
        INSERT INTO stg_prontuario.PACIENTE (id, nome, dt_nascimento, cpf, nome_mae, dt_atualizacao)
        SELECT id, nome, dt_nascimento, cpf, nome_mae, dt_atualizacao FROM stg_hospital_a.PACIENTE
        UNION ALL
        SELECT id, nome, dt_nascimento, cpf, nome_mae, dt_atualizacao FROM stg_hospital_b.PACIENTE
        UNION ALL
        SELECT id, nome, dt_nascimento, cpf, nome_mae, dt_atualizacao FROM stg_hospital_c.PACIENTE;
    """


# Comando para dados duplicados problema 3
def filter_data_table_stg_prontuario():
    return """
        SELECT 
            cpf,
            dt_nascimento,
            STRING_AGG(nome, ', ') AS nomes_variados,
            COUNT(*) AS total_registros,
            MIN(dt_atualizacao) AS primeira_atualizacao,
            MAX(dt_atualizacao) AS ultima_atualizacao
        FROM 
            stg_prontuario.PACIENTE
        GROUP BY 
            cpf, dt_nascimento
        HAVING 
            COUNT(*) > 1;
    """

# coletar a linha com a data mais recente 4 
def filter_data_table_paciente():
    return """
        WITH CTE_Pacientes AS (
            SELECT 
                id,
                nome,
                dt_nascimento,
                cpf,
                nome_mae,
                dt_atualizacao,
                ROW_NUMBER() OVER (
                    PARTITION BY cpf, dt_nascimento, nome_mae 
                    ORDER BY dt_atualizacao DESC
                ) AS rn
            FROM PACIENTE
        )
        SELECT 
            id,
            nome,
            dt_nascimento,
            cpf,
            nome_mae,
            dt_atualizacao
        FROM CTE_Pacientes
        WHERE rn = 1;
    """

# problema 5 - criação das tabelas
def create_table_rl_procedimento_cid_layout():
    return """
        CREATE TABLE rl_procedimento_cid_layout (
            CO_PROCEDIMENTO VARCHAR2(10),
            CO_CID VARCHAR2(4),
            ST_PRINCIPAL CHAR(1),
            DT_COMPETENCIA CHAR(6)
        );
    """
def create_table_rl_procedimento_cid_layout():
    return """
        CREATE TABLE rl_procedimento_cid_layout (
            CO_PROCEDIMENTO VARCHAR2(10),
            CO_CID VARCHAR2(4),
            ST_PRINCIPAL CHAR(1),
            DT_COMPETENCIA CHAR(6)
        );
    """

def create_table_tb_cid_layout():
    return """
        CREATE TABLE tb_cid_layout (
            CO_CID VARCHAR2(4),
            NO_CID VARCHAR2(100),
            TP_AGRAVO CHAR(1),
            TP_SEXO CHAR(1),
            TP_ESTADIO CHAR(1),
            VL_CAMPOS_IRRADIADOS NUMBER(4)
        );
    """

def create_table_tb_forma_organizacao_layout():
    return """
        CREATE TABLE tb_forma_organizacao_layout (
            CO_GRUPO VARCHAR2(2),
            CO_SUB_GRUPO VARCHAR2(2),
            CO_FORMA_ORGANIZACAO VARCHAR2(2),
            NO_FORMA_ORGANIZACAO VARCHAR2(100),
            DT_COMPETENCIA CHAR(6)
        );
    """

def create_table_tb_grupo_layout():
    return """
        CREATE TABLE tb_grupo_layout (
            CO_GRUPO VARCHAR2(2),
            NO_GRUPO VARCHAR2(100),
            DT_COMPETENCIA CHAR(6)
        );
    """

def create_table_tb_procedimento_layout():
    return """
        CREATE TABLE tb_procedimento_layout (
            CO_PROCEDIMENTO VARCHAR2(10),
            NO_PROCEDIMENTO VARCHAR2(250),
            TP_COMPLEXIDADE VARCHAR2(1),
            TP_SEXO VARCHAR2(1),
            QT_MAXIMA_EXECUCAO NUMBER(4),
            QT_DIAS_PERMANENCIA NUMBER(4),
            QT_PONTOS NUMBER(4),
            VL_IDADE_MINIMA NUMBER(4),
            VL_IDADE_MAXIMA NUMBER(4),
            VL_SH NUMBER(10),
            VL_SA NUMBER(10),
            VL_SP NUMBER(10),
            CO_FINANCIAMENTO VARCHAR2(2),
            CO_RUBRICA VARCHAR2(6),
            QT_TEMPO_PERMANENCIA NUMBER(4),
            DT_COMPETENCIA CHAR(6)
        );
    """

def create_table_tb_sub_grupo_layout():
    return """
        CREATE TABLE tb_sub_grupo_layout (
            CO_GRUPO VARCHAR2(2),
            CO_SUB_GRUPO VARCHAR2(2),
            NO_SUB_GRUPO VARCHAR2(100),
            DT_COMPETENCIA CHAR(6)
        );
    """




# problema 5 


def processar_rl_procedimento_cid_txt():
    """
    Processa um arquivo .txt no formato rl_procedimento_cid e salva como CSV.

    Args:
        arquivo_entrada (str): Caminho do arquivo .txt com os registros compactados.
        arquivo_saida (str): Caminho para salvar o arquivo CSV processado.

    Returns:
        pd.DataFrame: DataFrame contendo os dados processados.
    """

    # Exemplo de uso
    arquivo_entrada = './sigtap-simplificado/rl_procedimento_cid.txt'
    arquivo_saida = './formated/rl_procedimento_cid.csv'

    colunas = ['CO_PROCEDIMENTO', 'CO_CID', 'ST_PRINCIPAL', 'DT_COMPETENCIA']
    registros_processados = []

    with open(arquivo_entrada, 'r') as arquivo:
        for linha in arquivo:
            linha = linha.strip()  # Remove espaços e quebras de linha
            try:
                # Extraindo os campos com base nos índices
                CO_PROCEDIMENTO = linha[0:10]
                CO_CID = linha[10:14]
                ST_PRINCIPAL = linha[14:15]
                DT_COMPETENCIA = linha[15:21]
                
                # Adicionando o registro ao resultado
                registros_processados.append({
                    'CO_PROCEDIMENTO': CO_PROCEDIMENTO.strip(),
                    'CO_CID': CO_CID.strip(),
                    'ST_PRINCIPAL': ST_PRINCIPAL.strip(),
                    'DT_COMPETENCIA': DT_COMPETENCIA.strip()
                })
            except IndexError as e:
                print(f"Erro ao processar linha: '{linha}' - {e}")
    
    # Convertendo para DataFrame
    df = pd.DataFrame(registros_processados, columns=colunas)

    # Salvando o DataFrame como CSV
    df.to_csv(arquivo_saida, index=False, sep=',')
    print(f"Arquivo CSV salvo em: {arquivo_saida}")
    print(df.head())

    return df

def processar_tb_cid_txt():
    """
    Processa um arquivo .txt no formato tb_cid e salva como CSV.

    Args:
        Nenhum (usa caminhos de entrada e saída fixos).

    Returns:
        pd.DataFrame: DataFrame contendo os dados processados.
    """
    # Definição dos caminhos dos arquivos
    arquivo_entrada = './sigtap-simplificado/tb_cid.txt'
    arquivo_saida = './formated/tb_cid.csv'

    # Layout das colunas conforme especificado
    colunas = ['CO_CID', 'NO_CID', 'TP_AGRAVO', 'TP_SEXO', 'TP_ESTADIO', 'VL_CAMPOS_IRRADIADOS']
    registros_processados = []

    # Tenta abrir o arquivo com codificação ISO-8859-1 para lidar com caracteres especiais
    with open(arquivo_entrada, 'r', encoding='ISO-8859-1') as arquivo:
        for linha in arquivo:
            linha = linha.rstrip('\n')  # Remove espaços e quebras de linha
            
            try:
                # Extraindo os campos com base nos índices fornecidos
                CO_CID = linha[0:4].strip()
                NO_CID = linha[4:104].strip()
                TP_AGRAVO = linha[104:105].strip()
                TP_SEXO = linha[105:106].strip()
                TP_ESTADIO = linha[106:107].strip()
                VL_CAMPOS_IRRADIADOS = linha[107:111].strip()

                # Adicionando o registro ao resultado
                registros_processados.append({
                    'CO_CID': CO_CID,
                    'NO_CID': NO_CID,
                    'TP_AGRAVO': TP_AGRAVO,
                    'TP_SEXO': TP_SEXO,
                    'TP_ESTADIO': TP_ESTADIO,
                    'VL_CAMPOS_IRRADIADOS': VL_CAMPOS_IRRADIADOS
                })
            except IndexError as e:
                print(f"Erro ao processar linha: '{linha}' - {e}")
    
    # Convertendo para DataFrame
    df = pd.DataFrame(registros_processados, columns=colunas)

    # Salvando o DataFrame como CSV
    df.to_csv(arquivo_saida, index=False, sep=',', encoding='utf-8')
    print(f"Arquivo CSV salvo em: {arquivo_saida}")
    print(df.head())

    return df


def processar_dados_arquivo():
    """
    Processa os dados do arquivo fornecido com base no layout da tabela
    e salva o resultado como CSV.

    Returns:
        pd.DataFrame: DataFrame com os dados processados.
    """
    # Configuração do layout do arquivo
    layout = [
        {"coluna": "CO_GRUPO", "inicio": 0, "fim": 2, "tipo": "VARCHAR2"},
        {"coluna": "CO_SUB_GRUPO", "inicio": 2, "fim": 4, "tipo": "VARCHAR2"},
        {"coluna": "CO_FORMA_ORGANIZACAO", "inicio": 4, "fim": 6, "tipo": "VARCHAR2"},
        {"coluna": "NO_FORMA_ORGANIZACAO", "inicio": 6, "fim": 106, "tipo": "VARCHAR2"},
        {"coluna": "DT_COMPETENCIA", "inicio": 106, "fim": 112, "tipo": "CHAR"}
    ]

    # Caminhos dos arquivos
    arquivo_entrada = './sigtap-simplificado/tb_forma_organizacao.txt'
    arquivo_saida = './formated/tb_forma_organizacao_layout.csv'

    # Lista para armazenar os registros processados
    registros_processados = []

    # Abrindo o arquivo de entrada
    with open(arquivo_entrada, 'r', encoding='ISO-8859-1') as arquivo:
        for linha in arquivo:
            linha = linha.rstrip('\n')  # Remove quebra de linha
            registro = {}
            for campo in layout:
                # Extraindo o valor com base nos índices de início e fim
                registro[campo["coluna"]] = linha[campo["inicio"]:campo["fim"]].strip()
            registros_processados.append(registro)

    # Convertendo para DataFrame
    df = pd.DataFrame(registros_processados)

    # Salvando o DataFrame como CSV
    df.to_csv(arquivo_saida, index=False, encoding='utf-8', sep=',')
    print(f"Arquivo CSV salvo em: {arquivo_saida}")
    print(df.head())

    return df


def processar_tb_grupo_txt():
    """
    Processa um arquivo .txt no formato tb_grupo e salva como CSV.

    Args:
        Nenhum.

    Returns:
        pd.DataFrame: DataFrame contendo os dados processados.
    """

    # Exemplo de uso
    arquivo_entrada = './sigtap-simplificado/tb_grupo.txt'
    arquivo_saida = './formated/tb_grupo.csv'

    # Definição das colunas e seus tamanhos
    colunas = ['CO_GRUPO', 'NO_GRUPO', 'DT_COMPETENCIA']
    registros_processados = []

    # Abrindo o arquivo com o encoding adequado
    with open(arquivo_entrada, 'r', encoding='latin1') as arquivo:
        for linha in arquivo:
            linha = linha.strip()  # Remove espaços e quebras de linha
            try:
                # Extraindo os campos com base nos índices
                CO_GRUPO = linha[0:2]
                NO_GRUPO = linha[2:102]
                DT_COMPETENCIA = linha[102:108]
                
                # Adicionando o registro ao resultado
                registros_processados.append({
                    'CO_GRUPO': CO_GRUPO.strip(),
                    'NO_GRUPO': NO_GRUPO.strip(),
                    'DT_COMPETENCIA': DT_COMPETENCIA.strip()
                })
            except IndexError as e:
                print(f"Erro ao processar linha: '{linha}' - {e}")
    
    # Convertendo para DataFrame
    df = pd.DataFrame(registros_processados, columns=colunas)

    # Salvando o DataFrame como CSV
    df.to_csv(arquivo_saida, index=False, sep=',', encoding='utf-8')
    print(f"Arquivo CSV salvo em: {arquivo_saida}")
    print(df.head())

    return df


def processar_tb_procedimento_txt():
    """
    Processa um arquivo .txt no formato tb_procedimento e salva como CSV.

    Args:
        Nenhum.

    Returns:
        pd.DataFrame: DataFrame contendo os dados processados.
    """

    # Arquivo de entrada e saída
    arquivo_entrada = './sigtap-simplificado/tb_procedimento.txt'
    arquivo_saida = './formated/tb_procedimento.csv'

    # Definição das colunas e seus tamanhos
    colunas = [
        "CO_PROCEDIMENTO", "NO_PROCEDIMENTO", "TP_COMPLEXIDADE", "TP_SEXO",
        "QT_MAXIMA_EXECUCAO", "QT_DIAS_PERMANENCIA", "QT_PONTOS", "VL_IDADE_MINIMA",
        "VL_IDADE_MAXIMA", "VL_SH", "VL_SA", "VL_SP", "CO_FINANCIAMENTO", 
        "CO_RUBRICA", "QT_TEMPO_PERMANENCIA", "DT_COMPETENCIA"
    ]
    registros_processados = []

    # Abrindo o arquivo de entrada com o encoding adequado
    with open(arquivo_entrada, 'r', encoding='latin1') as arquivo:
        for linha in arquivo:
            linha = linha.strip()  # Remove espaços e quebras de linha
            try:
                # Extraindo os campos com base nos índices
                CO_PROCEDIMENTO = linha[0:10].strip()
                NO_PROCEDIMENTO = linha[10:260].strip()
                TP_COMPLEXIDADE = linha[260:261].strip()
                TP_SEXO = linha[261:262].strip()
                QT_MAXIMA_EXECUCAO = linha[262:266].strip()
                QT_DIAS_PERMANENCIA = linha[266:270].strip()
                QT_PONTOS = linha[270:274].strip()
                VL_IDADE_MINIMA = linha[274:278].strip()
                VL_IDADE_MAXIMA = linha[278:282].strip()
                VL_SH = linha[282:292].strip()
                VL_SA = linha[292:302].strip()
                VL_SP = linha[302:312].strip()
                CO_FINANCIAMENTO = linha[312:314].strip()
                CO_RUBRICA = linha[314:320].strip()
                QT_TEMPO_PERMANENCIA = linha[320:324].strip()
                DT_COMPETENCIA = linha[324:330].strip()

                # Adicionando o registro ao resultado
                registros_processados.append({
                    "CO_PROCEDIMENTO": CO_PROCEDIMENTO,
                    "NO_PROCEDIMENTO": NO_PROCEDIMENTO,
                    "TP_COMPLEXIDADE": TP_COMPLEXIDADE,
                    "TP_SEXO": TP_SEXO,
                    "QT_MAXIMA_EXECUCAO": QT_MAXIMA_EXECUCAO,
                    "QT_DIAS_PERMANENCIA": QT_DIAS_PERMANENCIA,
                    "QT_PONTOS": QT_PONTOS,
                    "VL_IDADE_MINIMA": VL_IDADE_MINIMA,
                    "VL_IDADE_MAXIMA": VL_IDADE_MAXIMA,
                    "VL_SH": VL_SH,
                    "VL_SA": VL_SA,
                    "VL_SP": VL_SP,
                    "CO_FINANCIAMENTO": CO_FINANCIAMENTO,
                    "CO_RUBRICA": CO_RUBRICA,
                    "QT_TEMPO_PERMANENCIA": QT_TEMPO_PERMANENCIA,
                    "DT_COMPETENCIA": DT_COMPETENCIA
                })
            except IndexError as e:
                print(f"Erro ao processar linha: '{linha}' - {e}")
    
    # Convertendo para DataFrame
    df = pd.DataFrame(registros_processados, columns=colunas)

    # Salvando o DataFrame como CSV
    df.to_csv(arquivo_saida, index=False, sep=',', encoding='utf-8')
    print(f"Arquivo CSV salvo em: {arquivo_saida}")
    print(df.head())

    return df


def processar_tb_sub_grupo_txt():
    """
    Processa um arquivo .txt no formato tb_sub_grupo e salva como CSV.

    Args:
        Nenhum.

    Returns:
        pd.DataFrame: DataFrame contendo os dados processados.
    """

    # Arquivo de entrada e saída
    arquivo_entrada = './sigtap-simplificado/tb_sub_grupo.txt'
    arquivo_saida = './formated/tb_sub_grupo.csv'

    # Definição das colunas e seus tamanhos
    colunas = ["CO_GRUPO", "CO_SUB_GRUPO", "NO_SUB_GRUPO", "DT_COMPETENCIA"]
    registros_processados = []

    # Abrindo o arquivo de entrada com o encoding adequado
    with open(arquivo_entrada, 'r', encoding='latin1') as arquivo:
        for linha in arquivo:
            linha = linha.strip()  # Remove espaços e quebras de linha
            try:
                # Extraindo os campos com base nos índices definidos
                CO_GRUPO = linha[0:2].strip()
                CO_SUB_GRUPO = linha[2:4].strip()
                NO_SUB_GRUPO = linha[4:104].strip()
                DT_COMPETENCIA = linha[104:110].strip()

                # Adicionando o registro ao resultado
                registros_processados.append({
                    "CO_GRUPO": CO_GRUPO,
                    "CO_SUB_GRUPO": CO_SUB_GRUPO,
                    "NO_SUB_GRUPO": NO_SUB_GRUPO,
                    "DT_COMPETENCIA": DT_COMPETENCIA
                })
            except IndexError as e:
                print(f"Erro ao processar linha: '{linha}' - {e}")
    
    # Convertendo para DataFrame
    df = pd.DataFrame(registros_processados, columns=colunas)

    # Salvando o DataFrame como CSV
    df.to_csv(arquivo_saida, index=False, sep=',', encoding='utf-8')
    print(f"Arquivo CSV salvo em: {arquivo_saida}")
    print(df.head())

    return df


def request_and_convert_to_dataframe():
    # Dados base da URL e parâmetros
    base_url = "https://api.open-meteo.com/v1/forecast"
    latitude = -22.9068
    longitude = -43.1729
    hourly = "surface_pressure"
    timezone = "America/Sao_Paulo"

    # Montando a URL manualmente
    url = (
        f"{base_url}?"
        f"latitude={latitude}&"
        f"longitude={longitude}&"
        f"hourly={hourly}&"
        f"timezone={timezone.replace('/', '%2F')}"  # Codifica o '/'
    )
    
    # Realizando a requisição
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()  # Obtém o JSON da resposta
        
        # Acessa os dados 'hourly'
        hourly_data = data.get("hourly", {})
        time = hourly_data.get("time", [])
        pressure = hourly_data.get("surface_pressure", [])
        
        # Cria o DataFrame
        df = pd.DataFrame({
            "time": time,
            "surface_pressure": pressure
        })
        print(df)
        return df
    else:
        raise Exception(f"Erro na requisição: {response.status_code}")



def verificar_viabilidade(prescricao, estoque):
    # Contando a frequência de cada medicamento nas sequências
    prescricao_counter = Counter(prescricao)
    estoque_counter = Counter(estoque)
    
    # Verificando se para cada medicamento prescrito, a quantidade no estoque é suficiente
    for medicamento, quantidade_prescrita in prescricao_counter.items():
        if estoque_counter[medicamento] < quantidade_prescrita:
            return False
    return True

# # Exemplos de uso
# print(verificar_viabilidade("a", "b"))  # Saída: False
# print(verificar_viabilidade("aa", "b"))  # Saída: False
# print(verificar_viabilidade("aa", "aab"))  # Saída: True
# print(verificar_viabilidade("aba", "cbaa"))  # Saída: True



def visualizar_atendimentos_por_dia(lista_de_datas_exemp):

    # Exemplo de uso
    lista_de_datas_exemp = [
        "2024-11-01", "2024-11-01", "2024-11-02", "2024-11-02", "2024-11-02", 
        "2024-11-03", "2024-11-04", "2024-11-04", "2024-11-04", "2024-11-04", 
        "2024-11-05"
    ]
    # Convertendo as datas para o formato datetime para garantir a ordenação correta
    datas_convertidas = [datetime.strptime(data, "%Y-%m-%d") for data in lista_de_datas_exemp]
    
    # Contando a quantidade de atendimentos por data
    contagem = Counter(datas_convertidas)
    
    # Preparando os dados para o gráfico
    datas = list(contagem.keys())
    atendimentos = list(contagem.values())
    
    # Ordenando as datas para exibir no gráfico
    datas.sort()
    atendimentos_ordenados = [contagem[date] for date in datas]
    
    # Plotando o gráfico
    plt.figure(figsize=(10, 6))
    plt.plot(datas, atendimentos_ordenados, marker='o', color='b', linestyle='-', markersize=8)
    plt.title('Quantidade de Atendimentos Médicos por Dia')
    plt.xlabel('Data')
    plt.ylabel('Quantidade de Atendimentos')
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.show()



# visualizar_atendimentos_por_dia(lista_de_datas_exemp)




# problema 8 medicamentos tabela


def get_media_prescricoes_query():
    return """
        SELECT AVG(qtd_prescricoes) AS media_prescricoes
        FROM (
            SELECT 
                a.id AS atendimento_id,
                COUNT(ap.id_prescricao) AS qtd_prescricoes
            FROM 
                ATENDIMENTO a
            JOIN 
                ATENDIMENTO_PRESCRICAO ap ON a.id = ap.id_atend
            WHERE 
                a.tp_atend = 'U'
            GROUP BY 
                a.id
        ) subquery;
    """


# problema 7 

# Tabela de Atendimentos Médicos
def create_table_stg_atendimentos():
    return """
        CREATE TABLE stg_atendimentos (
            atendimento_id SERIAL PRIMARY KEY,
            paciente_id BIGINT,
            data_atendimento TIMESTAMP NOT NULL,
            local_atendimento VARCHAR(255),
            medico_responsavel VARCHAR(255),
            observacoes TEXT
        );
    """
# problema 7 Tabela de Exames Solicitados 
def create_table_stg_exames():
    return """
        CREATE TABLE stg_exames (
            exame_id SERIAL PRIMARY KEY,
            atendimento_id BIGINT NOT NULL,
            tipo_exame VARCHAR(255) NOT NULL,
            data_solicitacao TIMESTAMP NOT NULL,
            status_exame VARCHAR(50),
            resultado TEXT,
            FOREIGN KEY (atendimento_id) REFERENCES stg_atendimentos(atendimento_id)
        );
    """

