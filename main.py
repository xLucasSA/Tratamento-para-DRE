from multiprocessing import Pool
import pandas as pd

from Modules.folhas.folha import folhaPagamentos
from Modules.cartao.servicos import gerarServicosCartao
from Modules.medicina.medicina import gerarMedicina
from Modules.relatorioBruno.cartao.brunoCartao import gerarCartaoBruno
from Modules.relatorioBruno.medicina.brunoMedicina import gerarMedicinaBruno
from Modules.relatorioBruno.odonto.brunoOdonto import gerarOdontoBruno
from Modules.odonto.gerarOdonto import gerarOdonto
from Modules.outrosInss.inss import outrosInss
from Modules.resultados.calculaResultados import gerarResultadosDre
from utils.mesclaPlanoContas import mesclarPlanoDeContas

def call_function(func):
    return func()

tasks = [folhaPagamentos, gerarMedicinaBruno, gerarOdontoBruno, gerarCartaoBruno, gerarServicosCartao, gerarMedicina, gerarOdonto]
#[folhaPagamentos, gerarServicosCartao, gerarMedicina, gerarCartaoBruno, gerarMedicinaBruno, gerarOdontoBruno, gerarOdonto]
tratamentos: list[dict] = []

if __name__ == "__main__":
    with Pool() as pool:
        tratamentos = pool.map(call_function, tasks)

        medInss2022 = tratamentos[1].pop('Bruno Medicina sem Filtro')
        odontoInss2022 = tratamentos[2].pop('Bruno Odonto sem Filtro')
        cartaoInss = tratamentos[3].pop('Bruno Cartão com Folha PGTO')
        folha = tratamentos[0]['Folhas de Pagamento']

        tratamentos[0]['Folhas de Pagamento'] = outrosInss(medInss2022, odontoInss2022, cartaoInss, folha)
        
        print('Iniciando geração de dados para tabela do DRE...')
        dfDre = pd.concat(pool.map(mesclarPlanoDeContas, tratamentos))

        dfDreFinal = gerarResultadosDre(dfDre)

        print(dfDreFinal)   

        #TODO:
        #- Criar banco de dados com docker compose
        #- Salvar em banco de dados e conectar no bi
        #- Repassar os itens vistos para ver se não faltou algum tratamento 