import pandas as pd
import asyncio
from os import getenv
from dotenv import load_dotenv
from .cartaoSemFolhaPagamento import tratarCartaoBruno

load_dotenv()
def gerarCartaoBruno():
    print("Tratando CARTÃO Planilha Bruno...")

    async def main():
        cartaoRibSemFolha = asyncio.create_task(tratarCartaoBruno('Ribamar', pd.read_excel(getenv("RELATORIO_BRUNO"), sheet_name='Cartao RIB')))
        cartaoSlzSemFolha = asyncio.create_task(tratarCartaoBruno('São Luís', pd.read_excel(getenv("RELATORIO_BRUNO"), sheet_name='Cartao SLZ')))

        cartaoRibComFolha = asyncio.create_task(tratarCartaoBruno('Ribamar', pd.read_excel(getenv("RELATORIO_BRUNO"), sheet_name='Cartao RIB'), semFolha=False))
        cartaoSlzComFolha = asyncio.create_task(tratarCartaoBruno('São Luís', pd.read_excel(getenv("RELATORIO_BRUNO"), sheet_name='Cartao SLZ'), semFolha=False))
        
        dfComFolha = pd.concat(await asyncio.gather(cartaoRibComFolha, cartaoSlzComFolha))
        dfSemFolha = pd.concat(await asyncio.gather(cartaoRibSemFolha, cartaoSlzSemFolha))

        return {
            'Bruno Cartão sem Folha PGTO': dfSemFolha,
            'Bruno Cartão com Folha PGTO': dfComFolha
        }

    resultado = asyncio.run(main())
    print("Finalizado CARTÃO Planilha Bruno!")

    return resultado