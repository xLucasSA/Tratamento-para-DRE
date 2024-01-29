import pandas as pd
from dotenv import load_dotenv
from os import getenv

load_dotenv()
def tratarPlanoContas(df: pd.DataFrame):
    df = df.rename(columns={'Descrição': 'Categoria'})

    df = df[~df['Categoria'].isna()]
    df['Categoria'] = df['Categoria'].apply(lambda x: str(x).upper().strip())
    df = df.drop_duplicates(subset=['Categoria'])

    df['Conta'] = df['Conta'].apply(lambda x: str(x).title())
    df['Subgrupo'] = df['Subgrupo'].apply(lambda x: str(x).title())

    df['Grupo'] = df['Grupo'].replace({'O - Variação Patrimonial': 'O - Movimentações'})

    return df

def mesclarPlanoDeContas(relatorios: dict) -> pd.DataFrame:
    df: pd.DataFrame = [relatorio for relatorio in relatorios.values()][0]
    
    df['Categoria'] = df['Categoria'].apply(lambda x: str(x).upper().strip())
    df['Descrição'] = df['Descrição'].apply(lambda x: str(x).title().strip())

    if ('Folhas de Pagamento' in relatorios.keys() or 'Bruno Odonto com Filtro' in relatorios.keys()):
        return df
    
    if ('Bruno Cartão sem Folha PGTO' in relatorios.keys() or 'Bruno Medicina com Filtro' in relatorios.keys()):
        df = df[df['Valor'] < 0]

        filtro = ["13º  SALARIO", "13º SALÁRIO", "ADIANTAMENTO SALARIAL", "FERIAS EMPREGADOS", "FGTS", "FGTS PAGO PELO CARTAO", "FOLHA MENSAL - SALÁRIO", "FÉRIAS", "FÉRIAS FUNCIONÁRIOS", "INSS", "INSS DO REPASSE DO EMPREGADO", "INSS PAGO PELO CARTAO", "RESCISOES CONTRATUAIS", "RESCISÕES", "RESCISÕES CONTRATUAIS (ACORDOS", "SALARIOS LIQUIDOS", "SALÁRIO - DIARISTAS", "SALÁRIO - RECEPCIONISTAS, TSB´S, ASB´S (FIXAS", "COMISSOES"]
        df = df[~df['Categoria'].isin(filtro)]
    
    planoContas = pd.read_excel(getenv("PLANO_CONTAS"), sheet_name='data')
    planoContas = tratarPlanoContas(planoContas)  

    df = df.merge(planoContas, how='left', on='Categoria')
    
    return df