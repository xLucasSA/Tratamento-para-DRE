import pandas as pd
import asyncio
from os import getenv
from dotenv import load_dotenv
from .coletarReceitas import gerarRecetiasOdonto
from utils.insereOutrosTributos import inserirIrpjCsllMedOdonto

load_dotenv()
def gerarOdonto():
    print("Iniciando tratamento de ODONTO...")
    
    async def main():
        receitasOdonto = pd.read_excel(getenv("CENTRO_CUSTOS"))

        df = await gerarRecetiasOdonto(receitasOdonto)

        df = await inserirIrpjCsllMedOdonto(df)

        return {
            'Receitas Odonto': df
        }
    
    resultado = asyncio.run(main())
    print("Finalizado ODONTO!")
    
    return resultado