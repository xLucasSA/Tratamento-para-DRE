import pandas as pd
from multiprocessing import Pool

colunasNaoAgrupadas = ['Descrição', 'Categoria', 'Conta']

def gerarReceitaLiquida(df: pd.DataFrame):
    df = df[df['Grupo'].isin(['A -  Receita Operacional', 'B -  Redutor Da Receita'])]
    df = df.groupby(['Data', 'Grupo', 'Subgrupo', 'Operação', 'Unidade'])['Valor'].sum().reset_index()

    df['Grupo'] = 'C - Receita Líquida'
    
    for coluna in colunasNaoAgrupadas:
        df[coluna] = None

    return df

def gerarLucroBruto(df: pd.DataFrame):
    df = df[df['Grupo'].isin(['A -  Receita Operacional', 'B -  Redutor Da Receita', 'D -  Csp'])]
    df = df.groupby(['Data', 'Grupo', 'Subgrupo', 'Operação', 'Unidade'])['Valor'].sum().reset_index()

    df['Grupo'] = 'E - Lucro Bruto'

    for coluna in colunasNaoAgrupadas:
        df[coluna] = None

    return df

def gerarEbitida(df: pd.DataFrame):
    df = df[df['Grupo'].isin(['A -  Receita Operacional', 'B -  Redutor Da Receita', 'D -  Csp', 'F - Despesa Operacional'])]
    df = df.groupby(['Data', 'Grupo', 'Subgrupo', 'Operação', 'Unidade'])['Valor'].sum().reset_index()

    df['Grupo'] = 'G - EBITIDA'

    for coluna in colunasNaoAgrupadas:
        df[coluna] = None

    return df

def gerarEbit(df: pd.DataFrame):
    df = df[df['Grupo'].isin([
        'A -  Receita Operacional', 
        'B -  Redutor Da Receita', 
        'D -  Csp', 'F - Despesa Operacional', 
        'H - Amortização E Depreciação'
    ])]
    
    df = df.groupby(['Data', 'Grupo', 'Subgrupo', 'Operação', 'Unidade'])['Valor'].sum().reset_index()
    df['Grupo'] = 'I - EBIT'

    for coluna in colunasNaoAgrupadas:
        df[coluna] = None
    
    return df

def gerarLucroLiquido(df: pd.DataFrame):
    df = df[df['Grupo'].isin([
        'A -  Receita Operacional', 
        'B -  Redutor Da Receita', 
        'D -  Csp', 'F - Despesa Operacional', 
        'H - Amortização E Depreciação' , 
        'K - Despesa Não Operacional'
    ])]

    df = df.groupby(['Data', 'Grupo', 'Subgrupo', 'Operação', 'Unidade'])['Valor'].sum().reset_index()
    df['Grupo'] = 'L - Lucro Líquido'

    for coluna in colunasNaoAgrupadas:
        df[coluna] = None

    return df

def gerarLucroADistribuir(df: pd.DataFrame):
    df = df[df['Grupo'].isin([
        'A -  Receita Operacional', 
        'B -  Redutor Da Receita', 
        'D -  Csp', 'F - Despesa Operacional', 
        'H - Amortização E Depreciação' , 
        'K - Despesa Não Operacional',
        'M - Tributos Sobre Lucro',
        'N - Investimentos'
    ])]

    df = df.groupby(['Data', 'Grupo', 'Subgrupo', 'Operação', 'Unidade'])['Valor'].sum().reset_index()
    df['Grupo'] = 'P - Lucro a Distribuir'

    for coluna in colunasNaoAgrupadas:
        df[coluna] = None

    return df

def worker(func, dfDre):
    return func(dfDre)

def gerarResultadosDre(dfDre: pd.DataFrame):
    tasks = [gerarReceitaLiquida, gerarLucroBruto, gerarEbitida, gerarEbit, gerarLucroLiquido, gerarLucroADistribuir]

    resultados = []
    with Pool() as pool:
        resultados = pd.concat(pool.starmap(worker, [(func, dfDre) for func in tasks]))

    dfDreFinal = pd.concat([dfDre, resultados])

    return dfDreFinal