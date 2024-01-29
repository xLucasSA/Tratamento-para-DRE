import pandas as pd
import asyncio
from os import getenv
from dotenv import load_dotenv
from .gerarMedicina import tratarMedicinaBruno, retrocederMesEmTodos

load_dotenv()
def gerarMedicinaBruno():
    print("Tratando MEDICINA Planilha Bruno...")

    async def main():
        relatorioRib = pd.read_excel(getenv("RELATORIO_BRUNO"), sheet_name='Medicina RIB')
        relatorioSlz = pd.read_excel(getenv("RELATORIO_BRUNO"), sheet_name='Medicina SLZ')

        medicinaRib = asyncio.create_task(tratarMedicinaBruno('Ribamar', relatorioRib))
        medicinaSlz = asyncio.create_task(tratarMedicinaBruno('São Luís', relatorioSlz))

        medicinaRibTodosRetrocedidos = asyncio.create_task(retrocederMesEmTodos('Ribamar', relatorioRib)) 
        medicinaSlzTodosRetrocedidos = asyncio.create_task(retrocederMesEmTodos('São Luís', relatorioSlz)) 

        medicinaCompleto = pd.concat(await asyncio.gather(medicinaRib, medicinaSlz))
        medicinaCompletoTodosRetrocedidos = pd.concat(await asyncio.gather(medicinaRibTodosRetrocedidos, medicinaSlzTodosRetrocedidos))
        
        return {
            'Bruno Medicina com Filtro': medicinaCompleto,
            'Bruno Medicina sem Filtro': medicinaCompletoTodosRetrocedidos
        }

    resultado = asyncio.run(main())
    print("Finalizado MEDICINA Planilha Bruno!")

    return resultado