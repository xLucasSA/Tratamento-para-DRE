import pandas as pd
from datetime import datetime
from os import getenv
from dotenv import load_dotenv

load_dotenv()
async def gerarFolhasComCategorias(df):
    df = pd.concat(df)
    
    rateioBase = pd.read_excel(getenv("RATEIO_FUNCIONARIOS"), sheet_name="Planilha2")
    
    rateioBase.columns = rateioBase.iloc[0]
    rateioBase = rateioBase.drop(rateioBase.index[0])

    rateioBase = rateioBase.dropna(subset=['Coluna1'])
    rateioBase.columns = ['Funcionário', 'Função', 'Operação', 'Unidade']

    rateioBase['Operação'] = rateioBase['Operação'].replace('Odontologia', 'Odonto')

    funcao = rateioBase[['Funcionário', 'Função']]
    
    df = pd.merge(df, funcao, how='left', on='Funcionário')

    df['Valor'] = df['Valor'].apply(lambda x: -1 * x)

    dfFerias = df.copy()
    dfSalarios = df.copy()
    dfFgts = df.copy()
    dfDecimo = df.copy()
    dfInss = df.copy()
    
    dfSalarios['Categoria'] = 'Salários'

    dfFerias['Categoria'] = 'Férias'
    dfFerias['Valor'] = dfFerias['Valor'].apply(lambda x: x*0.12/0.92)
     
    dfFgts['Categoria'] = 'FGTS e Rescisão'
    dfFgts['Valor'] = dfFgts['Valor'].apply(lambda x: x*0.12/0.92)
    
    dfDecimo['Categoria'] = 'Décimo Terceiro'
    dfDecimo['Valor'] = dfDecimo['Valor'].apply(lambda x: x*0.083/0.92)

    dfInss['Categoria'] = 'INSS'
    dfInss['Valor'] = dfInss['Valor'].apply(lambda x: x*0.2/0.92)
    dfInss = dfInss[dfInss['Operação'] != 'Cartão']
    dfInss = dfInss[dfInss['Data'].astype('datetime64[ns]') > datetime(2022, 12, 31)]

    return {
        "folhaSalario": dfSalarios,
        "folhaFerias": dfFerias,
        "folhaFgts": dfFgts,
        "folhaDecimo": dfDecimo,
        "folhaInss": dfInss
    }
    