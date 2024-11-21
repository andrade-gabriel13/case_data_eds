
# Teste Técnico para Engenheiro de Dados

## Responsável
**Gabriel Andrade**

## Descrição
Este projeto consiste em resolver um conjunto de problemas técnicos relacionados a Engenharia de Dados, conforme descrito no documento recebido. Foram implementadas soluções que envolvem manipulação de dados, criação de esquemas e tabelas em SQL, limpeza de dados duplicados, consumo de APIs externas, e análise de dados com visualizações gráficas.

O repositório foi organizado para facilitar a navegação e a compreensão das soluções, com os arquivos de entrada, scripts de solução e arquivos de saída devidamente estruturados.

```bash
.
├── formated
│   ├── rl_procedimento_cid.csv
│   ├── tb_cid.csv
│   ├── tb_forma_organizacao_layout.csv
│   ├── tb_grupo.csv
│   ├── tb_procedimento.csv
│   └── tb_sub_grupo.csv
├── Readme.md
├── sigtap-simplificado
│   ├── DATASUS-Tabela-de-Procedimentos-Lay-out.xls
│   ├── rl_procedimento_cid_layout.txt
│   ├── rl_procedimento_cid.txt
│   ├── sigtap-simplificado.zip
│   ├── tb_cid_layout.txt
│   ├── tb_cid.txt
│   ├── tb_forma_organizacao_layout.txt
│   ├── tb_forma_organizacao.txt
│   ├── tb_grupo_layout.txt
│   ├── tb_grupo.txt
│   ├── tb_procedimento_layout.txt
│   ├── tb_procedimento.txt
│   ├── tb_sub_grupo_layout.txt
│   └── tb_sub_grupo.txt
└── src
    ├── app_problema_5.py
    └── app.py
```

- **formated/**: Contém os arquivos processados do problema 5.
- **sigtap-simplificado/**: Contém os arquivos de entrada originais disponibilizados.
- **src/**: Scripts de solução para os problemas descritos.

## Soluções Implementadas

### Problema 1: Criação de Esquema e Tabela
Criado um script SQL que define o esquema `stg_prontuario` e a tabela `PACIENTE`.

### Problema 2: Copiar Dados de Tabelas
Foi implementado um comando SQL para unir e inserir dados de diferentes tabelas (`stg_hospital_a`, `stg_hospital_b`, `stg_hospital_c`) na tabela `PACIENTE` do esquema `stg_prontuario`.

### Problema 3: Identificar Dados Duplicados
Um comando SQL foi desenvolvido para identificar registros duplicados com base no CPF e data de nascimento, exibindo variações de nomes e intervalos de atualização.

### Problema 4: Selecionar Registros Mais Recentes
Usando uma CTE e a função `ROW_NUMBER()`, foi possível filtrar os registros mais recentes de pacientes com base na data de atualização.

### Problema 5: Manipulação de Arquivos SIGTAP
Processamento dos arquivos do SIGTAP, com:
- Criação de tabelas específicas para cada arquivo fornecido.
- Importação e formatação dos dados para geração de arquivos CSV.

### Problema 6: Consumo de API Externa
Uma API foi utilizada para coletar dados meteorológicos de pressão atmosférica em horários específicos. Os dados foram convertidos para um DataFrame com o auxílio da biblioteca Pandas.

### Problema 7: Modelagem de Tabelas de Atendimentos e Exames
Criados esquemas SQL para representar tabelas de atendimentos médicos e exames solicitados, com chaves primárias e relacionamentos definidos.

### Problema 8: Média de Prescrições por Atendimento
Desenvolvido um comando SQL para calcular a média de prescrições feitas para atendimentos do tipo `U`.

### Problema 9: Validação de Estoque de Medicamentos
Criada uma função Python que verifica se o estoque é suficiente para atender uma prescrição com base nas frequências de medicamentos.

### Problema 10: Visualização de Atendimentos
Usando Matplotlib, foi gerado um gráfico que exibe a quantidade de atendimentos médicos por dia com base em uma lista de datas fornecida.

## Pré-requisitos

### Linguagem Python:
- Pandas
- Matplotlib
- Requests

### Banco de Dados:
- SQL compatível com PostgreSQL para execução dos scripts.

### Ambiente:
- Python 3.8 ou superior
- Banco de dados PostgreSQL instalado e configurado.

## Melhorias Futuras
- **Testes Automatizados**: Implementar testes unitários para validação de funções e scripts.
- **Dockerização**: Criar um ambiente Docker para facilitar a configuração do banco de dados e dependências Python.
- **Documentação**: Adicionar diagramas de esquema das tabelas para melhor visualização.

## Contato
Gabriel Andrade  
**Email:** gabriel_andrade_souza@yahoo.com
