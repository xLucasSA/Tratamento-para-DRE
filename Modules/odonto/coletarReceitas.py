import pandas as pd

async def gerarRecetiasOdonto(df: pd.DataFrame):

    df['Descrição'] = df['Descrição'].apply(lambda x: str(x).title())
    df['Categoria'] = df['Descrição'].apply(lambda x: str(x).upper())
    df['Valor'] = df['Valor']*1.025
    df['Data'] = df['Data'].dt.date

    return df