import os
import pandas as pd
from utils.corrigeData import corrigirData

async def gerarProducao(unidade: str, caminho: str):
    dfs = []
    
    for path, _, arquivos in os.walk(caminho):
        for arquivo in arquivos:
            df = pd.read_excel(os.path.join(path, arquivo))

            df = df[['Procedimento', 'Valor', 'Faturado', 'Data Execução']]
            df = df.rename(columns={'Procedimento': 'Descrição', 'Data Execução': 'Data'})

            df = df[df['Faturado'] == 'Faturado']
            df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')
            df = df.dropna(axis=0, how='any')
            df['Valor'] = df['Valor'].apply(lambda x: x*1.03)

            df['Unidade'] = unidade
            df['Operação'] = 'Medicina'
            
            df = df.drop(columns='Faturado')
            df['Categoria'] = df['Descrição'].apply(lambda x: str(x).upper())

            df['Data'] = await corrigirData(df['Data'])

            dfs.append(df)
        
    
    return pd.concat(dfs)