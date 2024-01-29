import pandas as pd
from os import getenv
from utils.corrigeData import corrigirData
from dotenv import load_dotenv

load_dotenv()
async def gerarFolhaPlanoContas(df):    
    dfPlanoContas = pd.read_excel(getenv("PLANO_CONTAS"), sheet_name='data')

    dfPlanoContas = dfPlanoContas.rename(columns={'Conta': 'Categoria'})

    dfPlanoContas = dfPlanoContas[['Categoria', 'Subgrupo', 'Grupo']]
    dfPlanoContas = dfPlanoContas.drop_duplicates()

    dfPlanoContas['Categoria'] = dfPlanoContas['Categoria'].replace('FGTS e rescisão', 'FGTS e Rescisão')

    for chave, dataFrame in df.items():
        newDf = pd.merge(dataFrame, dfPlanoContas, on='Categoria', how='left')

        df[chave] = newDf

    dfCompleto = await adicionarRateio(df)

    dfCompleto = dfCompleto.rename(columns={'Funcionário': 'Descrição'})
    dfCompleto = dfCompleto[[column for column in dfCompleto.columns if column not in ['Código', 'CPF', 'Função']]]
    dfCompleto['Conta'] = dfCompleto['Categoria']
    
    return dfCompleto


async def adicionarRateio(df: dict):
    dfRateio = pd.read_excel(getenv("RATEIO_FUNCIONARIOS"), sheet_name='Call Centter')
    
    dfSalarios = df['folhaSalario']
    dfSalarios = dfSalarios.loc[:, ~dfSalarios.columns.isin(['Função'])]

    dfRateio = dfRateio.rename(columns={'Call centter': 'Função'})
    dfRateio = dfRateio[dfRateio['Função'] != 'Operacional']

    dfCompleto = pd.merge(dfRateio, dfSalarios, how='left', on='Funcionário')
    dfSalarios = dfSalarios[~dfSalarios.isin(dfCompleto.to_dict(orient='list')).all(axis=1)]
    
    dfRateioGeral = dfCompleto.copy()
    dfRateioGeral = dfRateioGeral[dfRateioGeral['Função'] == 'Geral']
    dfRateioGeral['Valor'] = dfRateioGeral['Valor'].apply(lambda x: x/6)

    
    dfRateioOutros = dfCompleto.copy()
    dfRateioOutros = dfRateioOutros[dfRateioOutros['Função'] != 'Geral']
    dfRateioOutros['Valor'] = dfRateioOutros['Valor'].apply(lambda x: x/2)
    
    dfRateioGeral = completarDfGeral(dfRateioGeral)
    dfRateioOutros = completarDfOutros(dfRateioOutros)

    df['folhaSalario'] = pd.merge(dfSalarios,dfRateio, how='left', on='Funcionário')
    df['folhaRateio'] = pd.concat([dfRateioGeral, dfRateioOutros])

    df = pd.concat(df.values())

    df['Data'] = await corrigirData(df['Data'])
    
    return df

def completarDfGeral(df):
    listaDfGeral = []
    listaOperacao = ['Medicina', 'Odonto', 'Cartão']
    listaUnidade = ['São Luís', 'Ribamar']
    
    for operacao in listaOperacao:
        for unidade in listaUnidade:
            dfModificado = df.copy()
            dfModificado['Unidade'] = unidade
            dfModificado['Operação'] = operacao
            
            listaDfGeral.append(dfModificado)

    dfGeral = pd.concat(listaDfGeral)
    return dfGeral

def completarDfOutros(df):
    listaDfOutros = []
    listaUnidade = ['São Luís', 'Ribamar']
    
    for unidade in listaUnidade:
        dfModificado = df.copy()
        dfModificado['Unidade'] = unidade
        
        listaDfOutros.append(dfModificado)

    dfOutros = pd.concat(listaDfOutros)
    return dfOutros