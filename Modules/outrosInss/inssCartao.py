import pandas as pd
from utils.corrigeData import corrigirData

async def inserirInssCartao(df: pd.DataFrame) -> pd.DataFrame:
    dfInssCartao = df.copy()
    dfInssCartao = dfInssCartao[dfInssCartao['Categoria'] == 'INSS']
    dfInssCartao['Data'] = dfInssCartao['Data'] + pd.DateOffset(months=-1)
    dfInssCartao['Data'] = await corrigirData(dfInssCartao['Data'])


    dfInssCartao['Subgrupo'] = 'Despesa com funcionários'
    dfInssCartao['Grupo'] = 'F - Despesa Operacional'
    dfInssCartao['Conta'] = 'INSS'

    return dfInssCartao