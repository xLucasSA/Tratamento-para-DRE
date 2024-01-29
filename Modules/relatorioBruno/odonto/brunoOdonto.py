import pandas as pd
import asyncio
from os import getenv
from dotenv import load_dotenv
from .gerarOdonto import tratarOdontoBruno, retrocederMesEmTodos

load_dotenv()
def gerarOdontoBruno():
    print("Tratando ODONTO Planilha Bruno...")

    async def main():
        relatorioRib = pd.read_excel(getenv("RELATORIO_BRUNO"), sheet_name='Odonto RIB')
        relatorioSlz = pd.read_excel(getenv("RELATORIO_BRUNO"), sheet_name='Odonto SLZ')

        planoContas = pd.read_excel(getenv("PLANO_CONTAS"), sheet_name='data')

        odontoRib = asyncio.create_task(tratarOdontoBruno('Ribamar', relatorioRib, planoContas))
        odontoSlz = asyncio.create_task(tratarOdontoBruno('São Luís', relatorioSlz, planoContas))

        odontoRibRetrocedeTudo = asyncio.create_task(retrocederMesEmTodos('Ribamar', relatorioRib, planoContas))
        odontoSlzRetrocedeTudo = asyncio.create_task(retrocederMesEmTodos('São Luís', relatorioSlz, planoContas))

        odontoCompleto = pd.concat(await asyncio.gather(odontoRib, odontoSlz))
        odontoCompletoRetrocedeTudo = pd.concat(await asyncio.gather(odontoRibRetrocedeTudo, odontoSlzRetrocedeTudo))

        return {
            'Bruno Odonto com Filtro': odontoCompleto,
            'Bruno Odonto sem Filtro': odontoCompletoRetrocedeTudo
        }

    resultado = asyncio.run(main())
    print("Finalizado ODONTO Planilha Bruno!")

    return resultado