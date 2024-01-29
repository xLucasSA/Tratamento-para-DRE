import os
import pandas as pd
from utils.corrigeData import corrigirData

async def gerarServicosUnidade(unidade: str, caminho: str):
    dfs = []
    for path, _, arquivos in os.walk(caminho):
        for arquivo in arquivos:
            df = pd.read_excel(os.path.join(path, arquivo))

            df["Unidade"] = unidade
            df["Operação"] = "Cartão"

            df["Descrição"] = df["FormaRecebimento"]
            df = df.rename(columns={"FormaRecebimento": "Categoria"})

            df = df[["Valor", "Categoria", "Descrição", "Data", "Operação", "Unidade"]]

            itensARemover = ("CAR - Crédito de Antecipação de Receita", "DAR - Débito de Adiatamento de Receita", "SLR - Saldo Extra Líquido a Receber", "TAC - Valor Taxa de adesão Concessionária (NF2)", "VAM - VALOR ARRECADAO MENSAL (NF1)", "VIR - Valor Itens Refaturados", "DIRETO NO CARTAO - CORPORATIVO")
            df = df[~df["Categoria"].isin(itensARemover)]
            
            df['Valor'] = df['Valor'].apply(lambda x: float(str(x).replace('.', '').replace(',', '.')))
            df['Data'] = await corrigirData(df['Data'])

            dfs.append(df)
    
    return pd.concat(dfs)