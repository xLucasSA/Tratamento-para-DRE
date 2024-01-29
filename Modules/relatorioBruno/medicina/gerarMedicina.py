import pandas as pd
from utils.corrigeData import corrigirData

async def tratarMedicinaBruno(unidade: str, df: pd.DataFrame):
    df = df[['DataPagamento', 'Categoria', 'Descricao', 'ValorPago']]
    df = df.rename(columns={'DataPagamento': 'Data', 'ValorPago': 'Valor', 'Descricao': 'Descrição'})

    df['Unidade'] = unidade
    df['Operação'] = 'Medicina'
    
    dfTrataMes = df.copy()

    filtro = ["COFINS", "CSLL", "IR Pessoa Jurídica", "ISS", "PIS", "Simples Nacional"]
    
    dfTrataMes = dfTrataMes[dfTrataMes['Categoria'].isin(filtro)]
    df = df[~df['Categoria'].isin(filtro)]

    dfTrataMes = dfTrataMes[dfTrataMes['Descrição'].apply(lambda x: str(x).find("Repasse de") == -1)]
    dfTrataMes = dfTrataMes[dfTrataMes['Data'] > pd.Timestamp('01-31-2023')]
    dfTrataMes['Data'] = dfTrataMes['Data'].apply(lambda x: x + pd.DateOffset(months=-1))

    df = pd.concat([df, dfTrataMes])

    filtro = ["13º Salário", "Adiantamento Salarial", "FGTS", "Folha Mensal - Salário", "Férias Funcionários", "INSS", "Repasses", "Férias", "Rescisões"]
    df = df[~df['Categoria'].isin(filtro)]

    df['Data'] = await corrigirData(df['Data'])
    df['Categoria'] = df['Categoria'].apply(lambda x: x.upper().strip())
    df['Descrição'] = df['Descrição'].apply(lambda x: x.title())

    return df

async def retrocederMesEmTodos(unidade: str, df: pd.DataFrame):
    df = df[['DataPagamento', 'Categoria', 'Descricao', 'ValorPago']]
    df = df.rename(columns={'DataPagamento': 'Data', 'ValorPago': 'Valor', 'Descricao': 'Descrição'})

    df['Unidade'] = unidade
    df['Operação'] = 'Medicina'

    df = df[df['Descrição'].apply(lambda x: str(x).find("Repasse de") == -1)]

    df['Data'] = df['Data'].apply(lambda x: x + pd.DateOffset(months=-1))
    df['Data'] = await corrigirData(df['Data'])

    df['Categoria'] = df['Categoria'].apply(lambda x: x.upper().strip())
    df['Descrição'] = df['Descrição'].apply(lambda x: x.title())

    return df