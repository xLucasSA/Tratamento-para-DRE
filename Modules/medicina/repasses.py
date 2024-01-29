import pandas as pd
from utils.corrigeData import corrigirData

async def gerarRepasses(unidade, caminho, dfMedicos):
    df = pd.read_excel(caminho)
    
    filtro = ["Repasse de Consultas", "Repasse de Exames", "Repasse De Procedimentos", "Repasse de Procedimentos", "Repasses", "repasses"]
    df = df[df['Plano de Contas'].isin(filtro)]

    df = df[['Data', 'Conta', 'Origem', 'Valor']]
    df['Unidade'] = unidade
    df['Categoria'] = 'REPASSES'
    df['Operação'] = 'Medicina'

    df['Conta'] = df['Conta'].apply(lambda x: x.upper().strip())

    df['Valor'] = df['Valor'].apply(lambda x: x.strip().replace('.', '').replace(',', '.'))
    df['Valor'] = pd.to_numeric(df['Valor'])
    df['Valor'] = df['Valor'] * -1
    
    df = pd.merge(df,dfMedicos, on='Conta', how='left')
    df = df[~df['Especialidade'].isna()]

    df = df[[column for column in df.columns if column != 'Conta']]
    df = df.rename(columns={'Especialidade': 'Descrição'})
    df = df.drop(columns=['Origem'])

    df['Data'] = await corrigirData(df['Data'])

    return df
