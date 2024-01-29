import pandas as pd

async def gerarirpjECsll(dicionarioServicos: dict):
    df = pd.concat(dicionarioServicos.values())

    df['Valor'] = df['Valor'].apply(lambda x: x * -0.105)
    df['Categoria'] = "IRPJ e CSLL"

    return df