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


# problema 6 

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



# problema 9

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


# problema 10
# visualizar_atendimentos_por_dia(lista_de_datas_exemp)

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


