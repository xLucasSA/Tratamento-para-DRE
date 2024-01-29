import pandas as pd
import datetime

async def inserirInss2022MedOdonto(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df['Categoria'] == 'INSS']
    df = df[df['Data'] < datetime.date(2023, 1, 1)]

    df['Subgrupo'] = 'Despesa com funcionários'
    df['Grupo'] = 'F - Despesa Operacional'
    df['Conta'] = 'INSS'

    return df