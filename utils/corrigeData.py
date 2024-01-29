import pandas as pd

async def corrigirData(coluna: pd.Series):
    coluna = coluna.apply(lambda x: pd.Timestamp(str(x)).date())
    
    return coluna