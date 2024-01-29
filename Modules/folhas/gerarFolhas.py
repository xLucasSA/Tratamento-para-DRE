import tabula, os, asyncio
import pandas as pd

async def gerarCsvs(origemPDF, saidaCSV):
    async def gerarCsvModelo(subPasta):
        for pasta in subpastas:
            for caminho, _, arquivos in os.walk(os.path.join(origemPDF, pasta)):
                if caminho == os.path.join(origemPDF, subPasta):
                    arquivosNoDiretorio = os.listdir(os.path.join(saidaCSV, subPasta))
                    
                    for arquivo in arquivos:
                        nomeArquivoAtual = '.'.join(arquivo.split('.')[0:2])+'.csv'

                        if ( nomeArquivoAtual not in arquivosNoDiretorio ):
                            saidaFinal = os.path.join(saidaCSV, subPasta, nomeArquivoAtual)
                            tabula.convert_into(os.path.join(caminho, arquivo), pages="all", area=(0,0,5000,5000), output_path=saidaFinal)
    
    subpastas = os.listdir(origemPDF)[:3]
    await asyncio.gather(
        gerarCsvModelo('Modelo 1'),
        gerarCsvModelo('Modelo 2'),
        gerarCsvModelo('Modelo 3'),
    )
    

async def tratarCsvs(origem):
    dfs = []
    for caminho, _, arquivos in os.walk(origem):
        if caminho == os.path.join(origem, "Modelo 1"):
            for arquivo in arquivos:
                df = pd.read_csv(os.path.join(caminho, arquivo), sep=',')

                novasColunas = list(df.iloc[4]) + ['Data', 'Unidade', 'Operação']

                data = df.iloc[2,0].split(' ')[1]
                df['Data'] = '01/'+data
                
                unidade = df.columns[0].split(': ')[1]
                df['Unidade'] = 'Ribamar' if ( unidade.find("SAO JOSE") != -1 ) else 'São Luís'
                
                df['Operação'] = 'Medicina' if ( arquivo.find("AMS") != -1 ) else 'Cartão'
                
                df.columns = novasColunas
                df = df.loc[5:]
                
                df = df.dropna(how='all', axis=1)
                colunasRemover = [coluna for coluna in df.columns if 'NAN' in str(coluna).upper()]
                df = df.drop(columns=colunasRemover)
                df = df.dropna(how='any', axis=0)
                df = df[~df['CPF'].astype(str).str.contains("Contribuintes", na=False)]

                df[['Código', 'Funcionário']] = df['Código Nome do empregado'].str.extract('(\d+)\s*(.*)', expand=True)
                df = df.drop(['Código Nome do empregado'], axis=1)
                
                df = df[['Código', 'Funcionário'] + [coluna for coluna in df.columns if coluna != 'Código' and coluna != 'Funcionário']]

                substituicoes = {'R\$ ': '', '\.': '', ',': '.'}
                df['Valor'] = df['Valor'].replace(substituicoes, regex=True).astype('float')

                df['Data'] = pd.to_datetime(df['Data'], dayfirst=True).dt.strftime('%d/%m/%Y')
                

                dfs.append(df)  
        
        elif caminho == os.path.join(origem, "Modelo 2"):
            for arquivo in arquivos:
                df = pd.read_csv(os.path.join(caminho, arquivo), sep=',')

                nomeArquivo = str(arquivo).split(' ')
                df['Data'] = '01/' + '/'.join(nomeArquivo[2].split('.')[0:2])
                
                unidade = nomeArquivo[1]
                df['Unidade'] = 'Ribamar' if ( unidade == 'SJR' ) else 'São Luís'
                
                df['Operação'] = 'Medicina' if ( nomeArquivo[0] == 'AMS' ) else 'Cartão'

                df = df.dropna(how='all', axis=1)
                df = df.dropna(how='any', axis=0)

            
                if df.columns[0].find("Relação de líquido") != -1:
                    df.columns = df.iloc[0]
                    df = df.iloc[1:]
                    df.reset_index(drop=True, inplace=True)
                    
                    if df.columns[0].find("Valor") != -1:
                        df[['Código', 'Funcionário', 'Valor']] = df['CódigoNome do colaborador Valor'].str.extract(r'(\d+)([a-zA-Z\s]+) ([\d\.,]+)', expand=True)
                        df = df.drop(['CódigoNome do colaborador Valor','Ban', 'Agência Conta Tipo'], axis=1)

                    else:
                        df[['Código', 'Funcionário']] = df['CódigoNome do colaborador'].str.extract(r'(\d+)([a-zA-Z\s]+)', expand=True)
                        df = df.drop(['CódigoNome do colaborador','Ban', 'Agência Conta Tipo'], axis=1)

                    colunasOrdenadas = ['Código', 'Funcionário', 'Valor', 'CPF']
                    df = df[colunasOrdenadas + [coluna for coluna in df.columns if coluna not in colunasOrdenadas]]
                    
                    df.columns = ['Código', 'Funcionário', 'Valor', 'CPF', 'Data', 'Unidade', 'Operação']
                else:
                    df.loc[-1] = df.columns.tolist()
                    df.index = df.index + 1
                    df = df.sort_index()
                    df.reset_index(drop=True, inplace=True)
                    
                    df.columns = ['Funcionário', 'Valor', 'CPF', 'Data', 'Unidade', 'Operação']
                    df['Código'] = None
                    df.at[0, 'Data'] = '01/' + '/'.join(nomeArquivo[2].split('.')[0:2])
                    df.at[0, 'Unidade'] = 'Ribamar' if ( unidade == 'SJR' ) else 'São Luís'
                    df.at[0, 'Operação'] = 'Medicina' if ( nomeArquivo[0] == 'AMS' ) else 'Cartão'
                    
                substituicoes = {'R\$ ': '', '\.': '', ',': '.'}
                df['Valor'] = df['Valor'].replace(substituicoes, regex=True).astype('float')

                df['Data'] = pd.to_datetime(df['Data'], dayfirst=True).dt.strftime('%d/%m/%Y')

                dfs.append(df)

        elif caminho == os.path.join(origem, "Modelo 3"):
            for arquivo in arquivos:
                df = pd.read_csv(os.path.join(caminho, arquivo), sep=',')

                df = df.dropna(how='all', axis=1)
                df = df.dropna(how='any', axis=0)

                df.columns = df.iloc[0]
                df = df.iloc[1:]
                df.reset_index(inplace=True, drop=True)

                df['Código'] = df['Func Chapeira Dados Bancários'].str.extract(r'^(\d+)(?:\s|$)')
                df['Funcionário'] = df['Nome'].str.extract(r'([A-Za-zÀ-ÿ\s]+)$')
                df = df.drop(['Func Chapeira Dados Bancários', 'Nome', 'Pagamento'], axis=1)

                df['CPF'] = None

                nomeArquivo = str(arquivo).split(' ')
                df['Data'] = '01/' + '/'.join(nomeArquivo[2].split('.')[0:2])
                
                unidade = nomeArquivo[1]
                df['Unidade'] = 'Ribamar' if ( unidade == 'SJR' ) else 'São Luís'
                
                df['Operação'] = 'Medicina' if ( nomeArquivo[0] == 'AMS' ) else 'Cartão'

                df['Valor'] = df['Valor'].replace(substituicoes, regex=True).astype('float')

                dfs.append(df)
        
    return dfs