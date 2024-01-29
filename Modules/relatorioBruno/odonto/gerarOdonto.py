import pandas as pd
import datetime
from utils.corrigeData import corrigirData

def avaliarDatasARetornar(row: pd.Series):
    if (row['Conta'] in ['Repasses', 'Protético']) and (row['Data'].day <= 15):
        return row['Data'] + pd.DateOffset(months=-1)
    else:
        return row['Data']
    
def base(unidade:str, df:pd.DataFrame, planoContas:pd.DataFrame):
    df = df[['DataPagamento', 'Categoria', 'Descricao', 'ValorPago']]
    df = df.rename(columns={'DataPagamento': 'Data', 'ValorPago': 'Valor', 'Descricao': 'Descrição'})
    planoContas = planoContas.rename(columns={'Descrição': 'Categoria'})

    planoContas = planoContas[~planoContas['Categoria'].isna()]

    df['Unidade'] = unidade
    df['Operação'] = 'Odonto'
    
    df = df[df['Valor'] < 0]

    df['Categoria'] = df['Categoria'].apply(lambda x: str(x).upper().strip())

    planoContas['Categoria'] = planoContas['Categoria'].apply(lambda x: str(x).upper().strip())
    planoContas = planoContas.drop_duplicates(subset=['Categoria'])

    df = pd.merge(df, planoContas, on='Categoria', how='left')

    df['Data'] = df.apply(avaliarDatasARetornar, axis=1)
    df['Descrição'] = df['Descrição'].apply(lambda x: x.title())
    df['Grupo'] = df['Grupo'].replace({'O - Variação Patrimonial': 'O - Movimentações'})

    return df


async def tratarOdontoBruno(unidade:str, df:pd.DataFrame, planoContas:pd.DataFrame):
    df = base(unidade, df, planoContas)

    dfTratarOutrasDatas = df.copy()
    filtro = ["COFINS", "CSLL", "INSS", "IR Pessoa Jurídica", "IRPJ", "ISSQN", "PIS", "SIMPLES NACIONAL"]

    dfTratarOutrasDatas = dfTratarOutrasDatas[dfTratarOutrasDatas['Categoria'].isin(filtro)]
    df = df[~df['Categoria'].isin(filtro)]

    dfTratarOutrasDatas['Data'] = dfTratarOutrasDatas['Data'].apply(lambda x: x + pd.DateOffset(months=-1))
    dfTratarOutrasDatas = dfTratarOutrasDatas[dfTratarOutrasDatas['Data'].dt.date > datetime.date(2022, 12, 31)]

    df = pd.concat([df, dfTratarOutrasDatas])
    df['Data'] = await corrigirData(df['Data'])

    filtro = ["13º SALARIO", "COMISSÕES", "FGTS", "FÉRIAS", "INSS", "SALÁRIO - DIARISTAS", "SALÁRIO - GESTOR DE CLÍNICA", "SALÁRIO - RECEPCIONISTAS, TSB´S, ASB´S (FIXAS"]

    df = df[~df['Categoria'].isin(filtro)]

    return df

async def retrocederMesEmTodos(unidade: str, df:pd.DataFrame, planoContas: pd.DataFrame):
    df = base(unidade, df, planoContas)

    df['Data'] = df['Data'].apply(lambda x: x + pd.DateOffset(months=-1))
    df['Data'] = await corrigirData(df['Data'])

    return df