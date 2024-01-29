import pandas as pd
from utils.corrigeData import corrigirData

async def tratarCartaoBruno(unidade, df, semFolha=True):
    df = pd.DataFrame(df)

    df = df[['DataPagamento', 'Categoria', 'Descricao', 'ValorPago']]
    df = df.rename(columns={'DataPagamento': 'Data', 'ValorPago': 'Valor', 'Descricao': 'Descrição'})

    df['Unidade'] = unidade
    df['Operação'] = 'Cartão'

    df['Categoria'] = df['Categoria'].apply(lambda x: x.upper().strip())
    df['Descrição'] = df['Descrição'].apply(lambda x: x.title().strip())

    df = df[~df['Categoria'].str.contains('CSLL', regex=True)]
    df = df[~df['Categoria'].str.contains('IRPJ', regex=True)]

    if semFolha:
        filtro = ["13º SALÁRIO", "13º  SALÁRIO", "COMISSOES", "COMISSÕES CLINICA", "FGTS", "FÉRIAS", "INSS", "RESCISOES CONTRATUAIS", "SALÁRIOS"]
        df = df[~df['Categoria'].isin(filtro)]

    df['Data'] = await corrigirData(df['Data'])

    return df