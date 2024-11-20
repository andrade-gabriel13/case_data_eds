
-- Código para criação de novo schema e tabela problema 1
CREATE SCHEMA IF NOT EXISTS stg_prontuario;

CREATE TABLE stg_prontuario.PACIENTE (
    id INT PRIMARY KEY,
    nome VARCHAR(255),
    dt_nascimento DATE,
    cpf INT,
    nome_mae VARCHAR(255),
    dt_atualizacao TIMESTAMP
);


-- codigo para copiar os dados problema 2
INSERT INTO stg_prontuario.PACIENTE (id, nome, dt_nascimento, cpf, nome_mae, dt_atualizacao)
SELECT id, nome, dt_nascimento, cpf, nome_mae, dt_atualizacao FROM stg_hospital_a.PACIENTE
UNION ALL
SELECT id, nome, dt_nascimento, cpf, nome_mae, dt_atualizacao FROM stg_hospital_b.PACIENTE
UNION ALL
SELECT id, nome, dt_nascimento, cpf, nome_mae, dt_atualizacao FROM stg_hospital_c.PACIENTE;


-- Comando para dados duplicados problema 3

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

-- coletar a linha com a data mais recente 4 

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

-- problema 5 - criação das tabelas

CREATE TABLE rl_procedimento_cid_layout (
    CO_PROCEDIMENTO VARCHAR2(10),
    CO_CID VARCHAR2(4),
    ST_PRINCIPAL CHAR(1),
    DT_COMPETENCIA CHAR(6)
);

CREATE TABLE tb_cid_layout (
    CO_CID VARCHAR2(4),
    NO_CID VARCHAR2(100),
    TP_AGRAVO CHAR(1),
    TP_SEXO CHAR(1),
    TP_ESTADIO CHAR(1),
    VL_CAMPOS_IRRADIADOS NUMBER(4)
);

CREATE TABLE tb_forma_organizacao_layout (
    CO_GRUPO VARCHAR2(2),
    CO_SUB_GRUPO VARCHAR2(2),
    CO_FORMA_ORGANIZACAO VARCHAR2(2),
    NO_FORMA_ORGANIZACAO VARCHAR2(100),
    DT_COMPETENCIA CHAR(6)
);

CREATE TABLE tb_grupo_layout (
    CO_GRUPO VARCHAR2(2),
    NO_GRUPO VARCHAR2(100),
    DT_COMPETENCIA CHAR(6)
);

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

CREATE TABLE tb_sub_grupo_layout (
    CO_GRUPO VARCHAR2(2),
    CO_SUB_GRUPO VARCHAR2(2),
    NO_SUB_GRUPO VARCHAR2(100),
    DT_COMPETENCIA CHAR(6)
);





-- problema 7 

-- Tabela de Atendimentos Médicos
CREATE TABLE stg_atendimentos (
    atendimento_id SERIAL PRIMARY KEY,
    paciente_id BIGINT,
    data_atendimento TIMESTAMP NOT NULL,
    local_atendimento VARCHAR(255),
    medico_responsavel VARCHAR(255),
    observacoes TEXT
);

-- problema 7 Tabela de Exames Solicitados 
CREATE TABLE stg_exames (
    exame_id SERIAL PRIMARY KEY,
    atendimento_id BIGINT NOT NULL,
    tipo_exame VARCHAR(255) NOT NULL,
    data_solicitacao TIMESTAMP NOT NULL,
    status_exame VARCHAR(50),
    resultado TEXT,
    FOREIGN KEY (atendimento_id) REFERENCES stg_atendimentos(atendimento_id)
);


-- problema 8 medicamentos tabela

SELECT 
    AVG(qtd_prescricoes) AS media_prescricoes
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


-- problema 9 
