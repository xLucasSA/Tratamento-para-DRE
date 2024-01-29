import pandas as pd
import asyncio
from .inssMedOdonto import inserirInss2022MedOdonto
from .inssCartao import inserirInssCartao

def outrosInss(dfMed: pd.DataFrame, dfOdonto: pd.DataFrame, dfCartao: pd.DataFrame, dfFolha: pd.DataFrame) -> pd.DataFrame:
    print('Icluindo outros INSS nas Folhas de Pagamento...')
    async def main():
        medInss2022 = inserirInss2022MedOdonto(dfMed)
        odontoInss2022 = inserirInss2022MedOdonto(dfOdonto)
        inssCartao = inserirInssCartao(dfCartao)

        inssFaltanes = pd.concat(await asyncio.gather(medInss2022, odontoInss2022, inssCartao))

        dfFolhaComInss = pd.concat([dfFolha, inssFaltanes])
        return dfFolhaComInss

    resultado = asyncio.run(main())

    print('Inclusão de outros INSS finalizado!')
    return resultado