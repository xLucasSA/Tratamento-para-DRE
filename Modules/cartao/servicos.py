from os import getenv
from dotenv import load_dotenv
import asyncio
from .servicosCartao import gerarServicosUnidade
from .cartoesIrpjECsll import gerarirpjECsll
import pandas as pd

load_dotenv()
def gerarServicosCartao():
    print("Iniciando tratamento de SERVIÇOS...")
    async def main():
        print("Coletando SERVIÇOS DOS CARTÕES...")
        servicosRiamar = asyncio.create_task(gerarServicosUnidade("Ribamar", getenv("SERVICOS_CARTAO_RIB")))
        servicosSlz = asyncio.create_task(gerarServicosUnidade("São Luíz", getenv("SERVICOS_CARTAO_SLZ")))

        servicos = {}
        servicos["Ribamar"] = await servicosRiamar
        servicos["São Luís"] = await servicosSlz

        print("Gerando IRPJ e CSLL de SERVIÇOS DOS CARTÕES...")
        irpjECsll = gerarirpjECsll(servicos)
        servicos["IRPJ e CSLL"] = await irpjECsll

        servicosCompleto = pd.concat(servicos.values())

        return {
            'Serviços Cartão': servicosCompleto
        }

    resultado = asyncio.run(main())
    print("SERVIÇOS finalizado!")
    
    return resultado
    
