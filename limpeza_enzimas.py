"""
Processador de Dados Enzimáticos (CUPP)
---------------------------------------

Este módulo é responsável pela limpeza e filtragem de arquivos TSV brutos
gerados por análises de peptídeos (CUPP).

Objetivo:
    Remover metadados do cabeçalho e filtrar o dataset para manter apenas
    as enzimas marcadas como significativas ('!').

Entrada esperada:
    Arquivo .tsv com 4 linhas de metadados iniciais.

Saída:
    Arquivo .tsv limpo contendo apenas as linhas relevantes.
"""

import pandas as pd
import os
from typing import Optional

# --- Constantes de Configuração ---
LINHAS_METADADOS_PARA_PULAR = 4
COLUNA_INDICADORA_SIGNIFICANCIA = '#Significant'
MARCADOR_ENZIMA_SIGNIFICATIVA = '!'

def carregar_dados_enzimaticos(caminho_arquivo: str) -> Optional[pd.DataFrame]:
    try:
        return pd.read_csv(
            caminho_arquivo,
            sep='\t',
            header=LINHAS_METADADOS_PARA_PULAR
        )
    except FileNotFoundError:
        print(f"Erro: O arquivo '{caminho_arquivo}' não foi encontrado.")
        return None
    except Exception as erro:
        print(f"Erro inesperado ao ler arquivo: {erro}")
        return None

def filtrar_apenas_significativas(dataset: pd.DataFrame) -> pd.DataFrame:
    if COLUNA_INDICADORA_SIGNIFICANCIA not in dataset.columns:
        raise ValueError(f"A coluna '{COLUNA_INDICADORA_SIGNIFICANCIA}' não existe no arquivo.")

    mascara_significancia = dataset[COLUNA_INDICADORA_SIGNIFICANCIA].astype(str).str.contains(
        MARCADOR_ENZIMA_SIGNIFICATIVA,
        na=False,
        regex=False
    )

    return dataset[mascara_significancia]

def salvar_dataset_processado(dataset: pd.DataFrame, caminho_destino: str) -> None:
    try:
        dataset.to_csv(caminho_destino, sep='\t', index=False)
        print(f"Processamento concluído. Arquivo gerado: {caminho_destino}")
    except Exception as erro:
        print(f"Erro ao salvar o arquivo final: {erro}")

def executar_pipeline_de_limpeza() -> None:
    arquivo_origem = 'Rickiella_edulis_cupp_GHs.tsv'
    arquivo_destino = 'Rickiella_edulis_significant_only.tsv'

    if not os.path.exists(arquivo_origem):
        print(f"Abortando: Arquivo de entrada '{arquivo_origem}' não localizado.")
        return

    print("Iniciando processamento de enzimas...")

    dataset_bruto = carregar_dados_enzimaticos(arquivo_origem)

    if dataset_bruto is not None:
        try:
            dataset_limpo = filtrar_apenas_significativas(dataset_bruto)
            salvar_dataset_processado(dataset_limpo, arquivo_destino)
        except ValueError as erro_validacao:
            print(f"Erro de validação nos dados: {erro_validacao}")

if __name__ == "__main__":
    executar_pipeline_de_limpeza()