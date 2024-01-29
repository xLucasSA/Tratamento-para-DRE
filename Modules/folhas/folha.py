import asyncio
from .gerarFolhas import gerarCsvs, tratarCsvs
from .gerarFolhasCategorias import gerarFolhasComCategorias
from .gerarFolhasPlanoContas import gerarFolhaPlanoContas
from os import getenv
from dotenv import load_dotenv

load_dotenv()
def folhaPagamentos():
    print("Iniciando tratamento de FOLHAS DE PAGAMENTO...")
    async def main():
        print("Verificando PDF's das folhas de pagamento...")

        await gerarCsvs(getenv("ORIGEM_FOLHA_LIQUIDA"), getenv("ORIGEM_CSV"))
        print("Verificação finalizada de PDF's das folhas de pagamento!")
    
        folhaBase = await tratarCsvs(getenv("ORIGEM_CSV"))
        
        folhasComCategorias = await gerarFolhasComCategorias(folhaBase)

        folhasPlanoRateio = await gerarFolhaPlanoContas(folhasComCategorias)

        return {
            'Folhas de Pagamento': folhasPlanoRateio
        }


    resultado = asyncio.run(main())
    print("FOLHAS DE PAGAMENTO finalizado!")
    return resultado

