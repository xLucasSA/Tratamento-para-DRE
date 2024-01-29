import pandas as pd
import datetime

async def inserirIrpjCsllMedOdonto(df: pd.DataFrame):
    dfCopia = df.copy()

    dfCopia = dfCopia[dfCopia['Data'] < datetime.date(2023, 1, 1)]
    dfCopia['Valor'] = dfCopia['Valor'] * -0.02
    
    dfCopia = dfCopia[dfCopia['Categoria'] != 'REPASSES']
    
    dfCopia['Categoria'] = 'IRPJ e CSLL'
    


    return pd.concat([df, dfCopia])