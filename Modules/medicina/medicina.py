import pandas as pd
import asyncio
from os import getenv
from dotenv import load_dotenv
from .produçãoMedicina import gerarProducao
from .repasses import gerarRepasses
from utils.insereOutrosTributos import inserirIrpjCsllMedOdonto

load_dotenv()
def gerarMedicina():
    print("Iniciando tratamento de MEDICINA...")
    
    async def main():
        medicinaRib = asyncio.create_task(gerarProducao("Ribamar", getenv("PRODUCAO_RIB")))
        medicinaSlz = asyncio.create_task(gerarProducao("São Luís", getenv("PRODUCAO_SLZ")))
        
        dfMedicos: pd.DataFrame = pd.read_excel(getenv("MEDICOS"), sheet_name='Planilha2')

        dfMedicos = dfMedicos.iloc[:, lambda df: [2,3]]
        dfMedicos = dfMedicos.dropna(axis=0, how='any')
        dfMedicos.columns = ['Conta', 'Especialidade']
        dfMedicos = dfMedicos.iloc[1:].reset_index(drop=True)
        dfMedicos['Conta'] = dfMedicos['Conta'].apply(lambda x: x.upper().strip())

        repassesRib = asyncio.create_task(gerarRepasses("Ribamar", getenv("REPASSES_RIB"), dfMedicos))
        repassesSlz = asyncio.create_task(gerarRepasses("São Luís", getenv("REPASSES_SLZ"), dfMedicos))

        dfProducao = await asyncio.gather(medicinaRib, medicinaSlz)
        dfProducao = pd.concat(dfProducao)

        dfRepasses = await asyncio.gather(repassesRib, repassesSlz)
        dfRepasses = pd.concat(dfRepasses)

        medicinaCompleto = pd.concat([dfProducao, dfRepasses])

        medicinaCompleto = await inserirIrpjCsllMedOdonto(medicinaCompleto)
        
        return {
            'Produção Medicina': medicinaCompleto
        }

    resultado = asyncio.run(main())
    print("MEDICINA finalizado!")

    return resultado