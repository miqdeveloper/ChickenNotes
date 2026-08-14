import re
from glob import glob
import pandas as pd
from datetime import datetime

output_csv = r"output"
saida_txt = r"saida_text"

csv_files = glob(f"{saida_txt}/*.csv")

new_dataFrame = pd.DataFrame()

def get_date_now():
    date_now = datetime.now()
    d = str(date_now.strftime("""_%d_%m_%Y"""))
    return d


def get_list_csv(*args: list):
    files_csv = [glob(f"{file_csv}/*.csv") for file_csv in args]
    return files_csv


def get_id(*args):
    """
    Function to get the ID from the CSV files in the specified directory.
    It reads each CSV file, extracts the 'ID' column, and returns a list of IDs.
    """
    files = args
    ids = []
    for file_ in files:
        file_f = glob(f"{file_}/*.csv")
        for csv_file in file_f:
            df = pd.read_csv(csv_file)
            if "id_file" in df.columns:
                id_s = df["id_file"].tolist()
                id_s = [
                    re.sub(r"(?i)\.pdf$", "", str(i)).strip() for i in id_s
                ]  # Convert IDs to strings
                ids.extend(id_s)
            if "filename" in df.columns:
                id_ss = df["filename"].tolist()
                id_ss = [
                    re.sub(r"(?i)\.pdf$", "", str(i)).strip() for i in id_ss
                ]  # Convert IDs to strings
                ids.extend(id_ss)

    ids = list(dict.fromkeys(ids))  # Remove duplicates mantendo a ordem

    return ids


ids_list = get_id(output_csv, saida_txt)


def extrair_linha(
    df: pd.DataFrame, regex_busca: str, regex_extracao: str, coluna: str = "content"
):
    arr_temp = []
    if coluna not in df.columns:
        return []

    # Define a coluna de ID
    if "id_file" in df.columns:
        col_id = "id_file"
    elif "filename" in df.columns:
        col_id = "filename"
    else:
        return []

    mask = df[coluna].str.contains(regex_busca, case=False, regex=True, na=False)

    if not mask.any():
        return []  # lista vazia quando não encontra nada

    resultados = []

    for idx, row in df.loc[mask].iterrows():
        id_s = re.sub(r"(?i)\.pdf$", "", str(row[col_id])).strip()

        extraido = (
            pd.Series([row[coluna]])
            .str.extract(regex_extracao, flags=re.IGNORECASE, expand=False)
            .iloc[0]
        )

        if pd.isna(extraido):
            continue

        extraido = extraido.strip()
        # print(extraido)
        resultados.append({"id_s": id_s, "texto": extraido})
        arr_temp.append(dicio_obj(str(id_s), str(extraido)))

    return arr_temp


def processar_dicionarios(id_unic_arr: list, arr_temp: list) -> list:
    """
    Se caso a lista separada nao conter o mesmo tamanho da lista de id_unic_arr
    ele irá adicionar o valor 'nan' para o id que nao foi encontrado na lista separada
    Args:
        id_unic_arr (list[str]): Array de id_unico
        arr_temp (list[str]): _description_ arranjo temporario
        arr_data_separate (list[dict]): array de dados separados, uma lista com diciionarios

    Returns:
        list[str]: _description_
    """
    arr_id = [id_["id"] for id_ in arr_temp]

    arr_data_final = []
    for id_unico in id_unic_arr:
        if id_unico not in arr_id:
            f_l = {"Data": "nan", "id": id_unico}
        else:
            f_l = arr_temp[arr_id.index(id_unico)]
        arr_data_final.append(f_l)
    # Extraindo apenas os valores da chave 'Data'
    arr_data_final = [data["Data"] for data in arr_data_final]

    return arr_data_final

    # Exemplo de uso
    # id_unic_arr = ['id1', 'id2', 'id3']
    # arr_fomento = ['id2', 'id3']
    # custo_fomento_arr = [{'Data': '2022-01-01', 'id': 'id2'}, {'Data': '2023-03-04', 'id': 'id3'}]
    # resultado = processar_custos_fomento(id_unic_arr, arr_fomento, custo_fomento_arr)
    # print(resultado)  # Saída: ['nan', '2022-01-01', '2023-03-04']


def dicio_obj(id: str, Data: str) -> dict:
    return {
        "id": id,
        "Data": Data,
    }


list_csv = get_list_csv(output_csv, saida_txt)

# Cada chave representa uma coluna e possui a mesma dupla de regex usada
# originalmente pelo filtro "Integrado".
VALOR_NUMERICO_SEM_GRUPO = (
    r"(?:\[?\s*Ileg[íi]vel\s*\]?|[-+]?\s*[0-9oO]+(?:[.,][0-9oO]+)*)"
)
VALOR_NUMERICO = f"({VALOR_NUMERICO_SEM_GRUPO})"
INICIO_ROTULO = r"(?<!\S)"
ROTULO_FEMEA = r"F[êée]meas?"
ROTULO_LOTE = r"(?<!\bdo\s)\bLot\s*e?\s*:"
ROTULO_ENDERECO = (
    INICIO_ROTULO + r"(?:Endere(?:ç|c)o|Endeeco|Ender)\s*[:.]"
)
ROTULO_TECNICO = INICIO_ROTULO + r"T[ée]c?nico\s*:"
ROTULO_MUNICIPIO = (
    INICIO_ROTULO
    + r"(?:Mun[íi]c[áíi]{0,2}pio|Mun[íi]d[íi]?pio|Muni[áa]pio|Municipic)\s*:"
)
ROTULO_CAPACIDADE = (
    r"\bCapa(?:cidade|didade)\s*\(\s*(?:Matrizes?|M[aã]es)\s*\)\s*:"
)
ROTULO_DT_REFERENCIA = (
    INICIO_ROTULO
    + r"Dt\s+Refer(?:[êe]ncia|[êe]ncdia|[êe]ncda|[êe]nciax|[êe]nda)\s*:?"
)
ROTULO_VLR = r"V(?:l|i)?[ri]"
ROTULO_RACAO_CALCULO = r"Ra[çc][aã][ou]"
ROTULO_AGENCIA = r"Ag(?:[êe]ncia|enda|endia|encdia|encda)"
ROTULO_CONTA_CORRENTE = r"Conta\s+Co(?:rr|m)ente"
ROTULO_VALOR_DEPOSITO = r"Valor\s*Dep[óo]sito"
TECNICOS_SEM_ROTULO = (
    r"(?:ROBSON\s+SOARES\s+CAPECCI|BRUNA\s+BARRETO\s+PRZYBULINSKI)"
)
NUMERO_PLANTEL_REPRODUTOR = r"(?:[1-9][0-9oO]{3,4})"
NUMERO_PLANTEL_MOVIMENTO = r"(?:0|[1-9oO][0-9oO]{0,2})"
LINHA_PLANTEL_SEM_ROTULOS = (
    r"^\s*"
    + NUMERO_PLANTEL_REPRODUTOR
    + (r"\s+" + NUMERO_PLANTEL_MOVIMENTO) * 4
    + r"\s*$"
)

filtros = {
    "Unidade": {
        "busca": r"(?<!\S)(?:Seara\s+)?Un(?:idade|jdade)\s*:",
        "extracao": (
            r"(?<!\S)(?:Seara\s+)?Un(?:idade|jdade)\s*:\s*\|?\s*"
            r"(.+?)(?=\t|$|\s+\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2})"
        ),
    },
    "Modalidade": {
        "busca": r"(?<!\S)M(?:o|6)dal{1,2}i?dade\s*:",
        "extracao": (
            r"(?<!\S)M(?:o|6)dal{1,2}i?dade\s*:\s*\|?\s*"
            r"(.+?)(?=\t|$|\s+\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2})"
        ),
    },
     "Integrado": {
        "busca": r"(?:\b(?:Integrado|fmegrado)\s*:\s*)?(?<!\d)\d{11}(?!\d)",
        "extracao": r"(?:\b(?:Integrado|fmegrado)\s*:\s*)?(?<!\d)(\d{11})(?!\d)",
    },
    "Lote": {
        "busca": ROTULO_LOTE,
        "extracao": (
            ROTULO_LOTE + r"\s*\|?\s*" + VALOR_NUMERICO + r"(?=\t|$)"
        ),
    },
    "Endereco": {
        "busca": (
            ROTULO_ENDERECO
            + r"|^(?![^\r\n]*[:\t])(?=.+\s+"
            + TECNICOS_SEM_ROTULO
            + r"\s*$)"
        ),
        "extracao": (
            r"(?:"
            + ROTULO_ENDERECO
            + r"\s*\|?\s*|^(?![^\r\n]*[:\t])(?=.+\s+"
            + TECNICOS_SEM_ROTULO
            + r"\s*$))([^\t\r\n]+?)"
            + r"(?=\t|$|\s+(?=T[ée]c?nico\s*:)|\s+(?="
            + TECNICOS_SEM_ROTULO
            + r"\s*$))"
        ),
    },
    "Tecnico": {
        "busca": (
            ROTULO_TECNICO
            + r"|^(?![^\r\n]*[:\t]).+\s+"
            + TECNICOS_SEM_ROTULO
            + r"\s*$"
        ),
        "extracao": (
            r"(?:"
            + ROTULO_TECNICO
            + r"\s*\|?\s*|^(?![^\r\n]*[:\t]).+?\s+(?="
            + TECNICOS_SEM_ROTULO
            + r"\s*$))(.+?)(?=\s*\|?\s*$)"
        ),
    },
    "Municipio": {
        "busca": ROTULO_MUNICIPIO,
        "extracao": (
            ROTULO_MUNICIPIO
            + r"\s*\|?\s*([^\t\r\n]+?)"
            + r"(?=\t|$|\s+(?=(?:Capa(?:cidade|didade)\s*\(|"
            + r"Dt\s*Meta\s*:|Granja\s+Agrin(?:ess|oss)\s*:|"
            + r"Dt\s+Refer)))"
        ),
    },
    "Capacidade_Matrizes": {
        "busca": ROTULO_CAPACIDADE,
        "extracao": (
            ROTULO_CAPACIDADE + r"\s*\|?\s*" + VALOR_NUMERICO
        ),
    },
    "Dt_Meta": {
        "busca": r"\bDt\s*Meta\s*:",
        "extracao": r"\bDt\s*Meta\s*:\s*\|?\s*([\d./-]+)",
    },
    "CPF_CGC": {
        "busca": r"\bCPF/(?:CGC|CRCG|CNPJ|CRC|CPC|CFC)\s*:",
        "extracao": r"\bCPF/(?:CGC|CRCG|CNPJ|CRC|CPC|CFC)\s*:\s*\|?\s*([\d./-]+)",
    },
    "Fazenda": {
        "busca": r"\bFazenda\s*:",
        "extracao": r"\bFazenda\s*:\s*\|?\s*" + VALOR_NUMERICO,
    },
    "Granja_Agriness": {
        "busca": r"\bGranja\s+Agrin(?:ess|oss)\s*:",
        "extracao": r"\bGranja\s+Agrin(?:ess|oss)\s*:\s*\|?\s*" + VALOR_NUMERICO,
    },
    "Dt_Referencia": {
        "busca": (
            ROTULO_DT_REFERENCIA
            + r"|\bGranja\s+Agrin(?:ess|oss)\s*:\s*\|?\s*"
            + VALOR_NUMERICO_SEM_GRUPO
            + r"\s+\d{2}/\d{2}/\d{4}\s*$"
        ),
        "extracao": (
            r"(?:"
            + ROTULO_DT_REFERENCIA
            + r"\s*\|?\s*|\bGranja\s+Agrin(?:ess|oss)\s*:\s*\|?\s*"
            + VALOR_NUMERICO_SEM_GRUPO
            + r"\s+(?=\d{2}/\d{2}/\d{4}\s*$))"
            + r"([\d./-]+)(?=\t|\s*$)"
        ),
    },
    "Reprodutores_Femea": {
        "busca": (
            INICIO_ROTULO
            + r"Reprodutores\s*"
            + ROTULO_FEMEA
            + r"\s*:|^\s*"
            + VALOR_NUMERICO_SEM_GRUPO
            + r"\s+(?=Entrada\s+"
            + ROTULO_FEMEA
            + r"\s*:)|"
            + LINHA_PLANTEL_SEM_ROTULOS
        ),
        "extracao": (
            r"(?:"
            + INICIO_ROTULO
            + r"Reprodutores\s*"
            + ROTULO_FEMEA
            + r"\s*:\s*\|?\s*|^\s*(?="
            + VALOR_NUMERICO_SEM_GRUPO
            + r"\s+Entrada\s+"
            + ROTULO_FEMEA
            + r"\s*:)|^\s*(?="
            + NUMERO_PLANTEL_REPRODUTOR
            + (r"\s+" + NUMERO_PLANTEL_MOVIMENTO) * 4
            + r"\s*$))"
            + VALOR_NUMERICO
            + r"(?=\t|$|\s+(?=Entrada\s+F[êée]meas?\s*:)|\s+(?="
            + NUMERO_PLANTEL_MOVIMENTO
            + (r"\s+" + NUMERO_PLANTEL_MOVIMENTO) * 3
            + r"\s*$))"
        ),
    },
    "Entrada_Femea": {
        "busca": (
            r"\bEntrada\s+"
            + ROTULO_FEMEA
            + r"\s*(?::)?|"
            + LINHA_PLANTEL_SEM_ROTULOS
        ),
        "extracao": (
            r"(?:\bEntrada\s+"
            + ROTULO_FEMEA
            + r"\s*:?\s*\|?\s*|^\s*"
            + NUMERO_PLANTEL_REPRODUTOR
            + r"\s+(?="
            + NUMERO_PLANTEL_MOVIMENTO
            + (r"\s+" + NUMERO_PLANTEL_MOVIMENTO) * 3
            + r"\s*$))"
            + VALOR_NUMERICO
        ),
    },
    "Mortes_Femea": {
        "busca": (
            r"\bMortes\s+"
            + ROTULO_FEMEA
            + r"\s*(?::)?|"
            + LINHA_PLANTEL_SEM_ROTULOS
        ),
        "extracao": (
            r"(?:\bMortes\s+"
            + ROTULO_FEMEA
            + r"\s*:?\s*\|?\s*|^\s*"
            + NUMERO_PLANTEL_REPRODUTOR
            + r"\s+"
            + NUMERO_PLANTEL_MOVIMENTO
            + r"\s+(?="
            + NUMERO_PLANTEL_MOVIMENTO
            + (r"\s+" + NUMERO_PLANTEL_MOVIMENTO) * 2
            + r"\s*$))"
            + VALOR_NUMERICO
        ),
    },
    "Abate_Femea": {
        "busca": (
            r"\bAbate\s+"
            + ROTULO_FEMEA
            + r"\s*(?::)?|"
            + LINHA_PLANTEL_SEM_ROTULOS
        ),
        "extracao": (
            r"(?:\bAbate\s+"
            + ROTULO_FEMEA
            + r"\s*:?\s*\|?\s*|^\s*"
            + NUMERO_PLANTEL_REPRODUTOR
            + (r"\s+" + NUMERO_PLANTEL_MOVIMENTO) * 2
            + r"\s+(?="
            + NUMERO_PLANTEL_MOVIMENTO
            + r"\s+"
            + NUMERO_PLANTEL_MOVIMENTO
            + r"\s*$))"
            + VALOR_NUMERICO
        ),
    },
    "Venda_Femea": {
        "busca": (
            r"\bVenda\s+"
            + ROTULO_FEMEA
            + r"\s*(?::)?|"
            + LINHA_PLANTEL_SEM_ROTULOS
        ),
        "extracao": (
            r"(?:\bVenda\s+"
            + ROTULO_FEMEA
            + r"\s*:?\s*\|?\s*|^\s*"
            + NUMERO_PLANTEL_REPRODUTOR
            + (r"\s+" + NUMERO_PLANTEL_MOVIMENTO) * 3
            + r"\s+(?="
            + NUMERO_PLANTEL_MOVIMENTO
            + r"\s*$))"
            + VALOR_NUMERICO
        ),
    },
    "Reprodutores_Macho": {
        "busca": r"\bReprodutores\s+Machos?\s*:",
        "extracao": r"\bReprodutores\s+Machos?\s*:\s*\|?\s*" + VALOR_NUMERICO,
    },
    "Entrada_Macho": {
        "busca": r"\bEntrada\s+Machos?\s*:",
        "extracao": r"\bEntrada\s+Machos?\s*:\s*\|?\s*" + VALOR_NUMERICO,
    },
    "Mortes_Macho": {
        "busca": r"\bMortes\s+Machos?\s*:",
        "extracao": r"\bMortes\s+Machos?\s*:\s*\|?\s*" + VALOR_NUMERICO,
    },
    "Abate_Macho": {
        "busca": r"\bAbate\s+Machos?\s*:",
        "extracao": r"\bAbate\s+Machos?\s*:\s*\|?\s*" + VALOR_NUMERICO,
    },
    "Venda_Macho": {
        "busca": r"\bVenda\s+Machos?\s*(?::)?",
        "extracao": r"\bVenda\s+Machos?\s*:?\s*\|?\s*" + VALOR_NUMERICO,
    },
    "Leitao_Desmamado_Femea_Ano_Prev": {
        "busca": (
            r"\bLeit[aã]o\s+Desmamado\s*/\s*F[êée]mea\s*/\s*Ano\s+Prev\s*:"
            r"|^\s*[0-9oO]{1,2}[.,][0-9oO]{3}\s+"
            r"(?=Mortalidade\s+\S*antel\s*Prev\s*[:.])"
        ),
        "extracao": (
            r"(?:\bLeit[aã]o\s+Desmamado\s*/\s*F[êée]mea\s*/\s*Ano\s+Prev\s*:"
            r"\s*\|?\s*|^\s*(?=[0-9oO]{1,2}[.,][0-9oO]{3}\s+"
            r"Mortalidade\s+\S*antel\s*Prev\s*[:.]))"
            + VALOR_NUMERICO
        ),
    },
    "Leitao_Desmamado_Femea_Ano_Real": {
        "busca": (
            INICIO_ROTULO
            + r"Leit[aã]o\s+Desmamado\s*/\s*"
            + r"F[êèée](?:mea|rnea)\s*/\s*Ano\s+Real\s*:?"
            + r"|^\s*[0-9oO]{1,2}[.,][0-9oO]{3}\s+"
            + r"(?=(?:M|[iv]{1,3})ortalidade\s+"
            + r"(?:P[li]ante[li]|Aante[li])\s*Rea[li]\s*:?)"
        ),
        "extracao": (
            r"(?:"
            + INICIO_ROTULO
            + r"Leit[aã]o\s+Desmamado\s*/\s*"
            + r"F[êèée](?:mea|rnea)\s*/\s*Ano\s+Real\s*:?"
            + r"\s*\|?\s*|^\s*(?=[0-9oO]{1,2}[.,][0-9oO]{3}\s+"
            + r"(?:M|[iv]{1,3})ortalidade\s+"
            + r"(?:P[li]ante[li]|Aante[li])\s*Rea[li]\s*:?))"
            + VALOR_NUMERICO
            + r"(?=\t|$|\s+(?=(?:M|[iv]{1,3})ortalidade\s+))"
        ),
    },
    "Mortalidade_Plantel_Prev": {
        "busca": r"\bMortalidade\s+\S*antel\s*Prev\s*[:.]",
        "extracao": r"\bMortalidade\s+\S*antel\s*Prev\s*[:.]\s*\|?\s*" + VALOR_NUMERICO,
    },
    "Mortalidade_Plantel_Real": {
        "busca": (
            INICIO_ROTULO
            + r"(?:M|[iv]{1,3})ortalidade\s+"
            + r"(?:P[li]ante[li]|Aante[li])\s*Rea[li]\s*:?"
        ),
        "extracao": (
            INICIO_ROTULO
            + r"(?:M|[iv]{1,3})ortalidade\s+"
            + r"(?:P[li]ante[li]|Aante[li])\s*Rea[li]\s*:?\s*\|?\s*"
            + VALOR_NUMERICO
        ),
    },
    "Peso_Medio_Leitao_Prev": {
        "busca": (
            r"\bPeso\s+M[ée]dio\s+Leit[aã]o\s+Prev\s*:"
            r"|^(?![^\r\n]*\t)(?:\s*Leit[aã]o\s+Desmamado\s*/\s*"
            r"F[êée]mea\s*/\s*Ano\s+Prev\s*:\s*)?"
            + VALOR_NUMERICO_SEM_GRUPO
            + r"\s+Mortalidade\s+\S*antel\s*Prev\s*[:.]\s*"
            + VALOR_NUMERICO_SEM_GRUPO
            + r"\s+"
            + VALOR_NUMERICO_SEM_GRUPO
            + r"\s*$"
        ),
        "extracao": (
            r"(?:\bPeso\s+M[ée]dio\s+Leit[aã]o\s+Prev\s*:\s*\|?\s*"
            r"|^(?![^\r\n]*\t)(?:\s*Leit[aã]o\s+Desmamado\s*/\s*"
            r"F[êée]mea\s*/\s*Ano\s+Prev\s*:\s*)?"
            + VALOR_NUMERICO_SEM_GRUPO
            + r"\s+Mortalidade\s+\S*antel\s*Prev\s*[:.]\s*"
            + VALOR_NUMERICO_SEM_GRUPO
            + r"\s+(?="
            + VALOR_NUMERICO_SEM_GRUPO
            + r"\s*$))"
            + VALOR_NUMERICO
        ),
    },
    "Peso_Medio_Leitao_Real": {
        "busca": (
            INICIO_ROTULO
            + r"Peso\s+(?:M|[iv]{1,3})[ée]dio\s+"
            + r"Leit[aã]o\s+Rea[lt]\s*:?"
            + r"|^(?![^\r\n]*\t)(?:\s*Leit[aã]o\s+Desmamado\s*/\s*"
            + r"F[êèée](?:mea|rnea)\s*/\s*Ano\s+Real\s*:\s*)?"
            + VALOR_NUMERICO_SEM_GRUPO
            + r"\s+(?:M|[iv]{1,3})ortalidade\s+"
            + r"(?:P[li]ante[li]|Aante[li])\s*Rea[li]\s*:?\s*"
            + VALOR_NUMERICO_SEM_GRUPO
            + r"\s+"
            + VALOR_NUMERICO_SEM_GRUPO
            + r"\s*$"
        ),
        "extracao": (
            r"(?:"
            + INICIO_ROTULO
            + r"Peso\s+(?:M|[iv]{1,3})[ée]dio\s+"
            + r"Leit[aã]o\s+Rea[lt]\s*:?\s*\|?\s*"
            + r"|^(?![^\r\n]*\t)(?:\s*Leit[aã]o\s+Desmamado\s*/\s*"
            + r"F[êèée](?:mea|rnea)\s*/\s*Ano\s+Real\s*:\s*)?"
            + VALOR_NUMERICO_SEM_GRUPO
            + r"\s+(?:M|[iv]{1,3})ortalidade\s+"
            + r"(?:P[li]ante[li]|Aante[li])\s*Rea[li]\s*:?\s*"
            + VALOR_NUMERICO_SEM_GRUPO
            + r"\s+(?="
            + VALOR_NUMERICO_SEM_GRUPO
            + r"\s*$))"
            + VALOR_NUMERICO
            + r"(?=\t|\s*$)"
        ),
    },
    "Racao_Reprodutor_Femea_Ano_Prev": {
        "busca": (
            r"\bRa[çc][aã]o?\s+Reprodutor\s*/\s*F[êée]mea\s*/\s*Ano\s+Prev\s*:"
            r"|^\s*[0-9oO]{3,4}[.,][0-9oO]{3}\s+"
            r"(?=(?:Ra[çc][aã]o?\s*/\s*Leit[aã]o\s+Entregue\s+Prev\s*:|"
            r"(?:Check|Ched[<k])\s*[-<]?\s*List\s*:))"
        ),
        "extracao": (
            r"(?:(?![^\r\n]*\bEntregue\s+Rea(?:l|i|t|lt))"
            r"\bRa[çc][aã]o?\s+Reprodutor\s*/\s*F[êée]mea\s*/\s*Ano\s+Prev\s*:"
            r"\s*\|?\s*|^\s*(?=[0-9oO]{3,4}[.,][0-9oO]{3}\s+"
            r"(?:Ra[çc][aã]o?\s*/\s*Leit[aã]o\s+Entregue\s+Prev\s*:|"
            r"(?:Check|Ched[<k])\s*[-<]?\s*List\s*:)))"
            + VALOR_NUMERICO
        ),
    },
    "Racao_Reprodutor_Femea_Ano_Real": {
        "busca": (
            r"\bRa[çc][aã]o?\s+Reprodutor\s*/\s*F[êée]mea\s*/\s*Ano\s+Rea[li]\s*:"
            r"|^\s*(?:[89]\d{2}|1\d{3})[.,]\d{3}\s+[0oO][.,][0oO]{3}\s*$"
            r"|\bRa[çc][aã]o?\s+Reprodutor\s*/\s*F[êée]mea\s*/\s*Ano\s+Prev\s*:"
            r"\s*(?:[89]\d{2}|1\d{3})[.,]\d{3}\s+"
            r"(?=Ra[çc][aã]o?\s*/\s*Leit[aã]o\s+Entregue\s+Rea)"
        ),
        "extracao": (
            r"(?:\bRa[çc][aã]o?\s+Reprodutor\s*/\s*F[êée]mea\s*/\s*Ano\s+Rea[li]\s*:"
            r"\s*\|?\s*|^\s*(?=(?:[89]\d{2}|1\d{3})[.,]\d{3}\s+"
            r"[0oO][.,][0oO]{3}\s*$)|\bRa[çc][aã]o?\s+Reprodutor\s*/\s*"
            r"F[êée]mea\s*/\s*Ano\s+Prev\s*:\s*\|?\s*"
            r"(?=(?:[89]\d{2}|1\d{3})[.,]\d{3}\s+Ra[çc][aã]o?\s*/\s*"
            r"Leit[aã]o\s+Entregue\s+Rea))"
            + VALOR_NUMERICO
        ),
    },
    "Racao_Leitao_Entregue_Prev": {
        "busca": r"\bRa[çc][aã]o?\s*/\s*Leit[aã]o\s+Entregue\s+Prev\s*:",
        "extracao": (
            r"\bRa[çc][aã]o?\s*/\s*Leit[aã]o\s+Entregue\s+Prev\s*:"
            r"\s*\|?\s*" + VALOR_NUMERICO
        ),
    },
    "Racao_Leitao_Entregue_Real": {
        "busca": (
            INICIO_ROTULO
            + r"Ra[çc][aã]o?\s*/\s*Leit[aã]o\s+"
            + r"Entregue\s+Rea(?:l|i|t|lt)\s*:?"
            + r"|^\s*(?:[89]\d{2}|1\d{3})[.,]\d{3}\s+"
            + r"[0oO][.,][0oO]{3}\s*$"
        ),
        "extracao": (
            r"(?:"
            + INICIO_ROTULO
            + r"Ra[çc][aã]o?\s*/\s*Leit[aã]o\s+"
            + r"Entregue\s+Rea(?:l|i|t|lt)\s*:?\s*\|?\s*"
            + r"|^\s*(?:[89]\d{2}|1\d{3})[.,]\d{3}\s+"
            + r"(?=[0oO][.,][0oO]{3}\s*$))"
            + VALOR_NUMERICO
            + r"(?=\t|\s*$)"
        ),
    },
    "Check_List": {
        "busca": (
            INICIO_ROTULO
            + r"(?:Check\s*[-<]?\s*List|Ched[<k]\s*[-<]?\s*List|"
            + r"Ajuste\s+(?:Check|Ched[<k])\s*[-<]?\s*List)\s*:?"
        ),
        "extracao": (
            INICIO_ROTULO
            + r"(?:Check\s*[-<]?\s*List|Ched[<k]\s*[-<]?\s*List|"
            + r"Ajuste\s+(?:Check|Ched[<k])\s*[-<]?\s*List)\s*:?"
            + r"\s*\|?\s*([-+]?\s*(?:[0-9oO]+[.,][0-9oO]+|[0-9oO]{4}))"
            + r"(?=\t|$|\s+(?=Ra[çc][aã]o?\s+Reprodutor\s*/))"
        ),
    },
    "Qt_Leitoes": {
        "busca": r"\bQt\.?\s*Leit[õo]es\s*:",
        "extracao": (
            r"\bQt\.?\s*Leit[õo]es\s*:\s*\|?\s*" + VALOR_NUMERICO
        ),
    },
    "Vlr_Leitao": {
        "busca": r"\b" + ROTULO_VLR + r"\s*Leit[aã]o\s*:",
        "extracao": (
            r"\b"
            + ROTULO_VLR
            + r"\s*Leit[aã]o\s*:\s*\|?\s*"
            + VALOR_NUMERICO
        ),
    },
    "Vlr_Reprodutor": {
        "busca": (
            r"\b" + ROTULO_VLR + r"\s*Re(?:produtor|orodutor)\s*[:.]?"
        ),
        "extracao": (
            r"\b"
            + ROTULO_VLR
            + r"\s*Re(?:produtor|orodutor)\s*[:.]?\s*\|?\s*"
            + VALOR_NUMERICO
        ),
    },
    "Vlr_kg_Racao_Matriz": {
        "busca": (
            r"\b"
            + ROTULO_VLR
            + r"\s*k[gy]\s*"
            + ROTULO_RACAO_CALCULO
            + r"\s+(?:Matriz|Mairiz|iviauiz|vauiz)\s*[:.]?"
        ),
        "extracao": (
            r"\b"
            + ROTULO_VLR
            + r"\s*k[gy]\s*"
            + ROTULO_RACAO_CALCULO
            + r"\s+(?:Matriz|Mairiz|iviauiz|vauiz)\s*[:.]?\s*\|?\s*"
            + VALOR_NUMERICO
        ),
    },
    "Vlr_kg_Racao_Leitao": {
        "busca": (
            r"\b"
            + ROTULO_VLR
            + r"\s*k[gy]\s*"
            + ROTULO_RACAO_CALCULO
            + r"\s+Leit[aã]o\s*[:.]?"
        ),
        "extracao": (
            r"\b"
            + ROTULO_VLR
            + r"\s*k[gy]\s*"
            + ROTULO_RACAO_CALCULO
            + r"\s+Leit[aã]o\s*[:.]?\s*\|?\s*"
            + VALOR_NUMERICO
        ),
    },
    "Basico_de_Partilha": {
        "busca": r"\bB[aá]sico\s+de\s+Partilha\s*:",
        "extracao": (
            r"\bB[aá]sico\s+de\s+Partilha\s*:\s*\|?\s*"
            + VALOR_NUMERICO
        ),
    },
    "Ajuste_Leitao_Desmamado_LDFA": {
        "busca": (
            r"\bA(?:j)?uste\s+Leit[aã]o\s*Desmamado\s*"
            r"\(\s*LDFA\s*\)\s*:?"
        ),
        "extracao": (
            r"\bA(?:j)?uste\s+Leit[aã]o\s*Desmamado\s*"
            r"\(\s*LDFA\s*\)\s*:?\s*\|?\s*"
            + VALOR_NUMERICO
        ),
    },
    "Ajuste_Racao_Reprodutor_RRFA": {
        "busca": (
            r"\bA(?:j)?uste\s+"
            + ROTULO_RACAO_CALCULO
            + r"\s+Reprodutor\s*\(\s*RRFA\s*\)\s*:?"
        ),
        "extracao": (
            r"\bA(?:j)?uste\s+"
            + ROTULO_RACAO_CALCULO
            + r"\s+Reprodutor\s*\(\s*RRFA\s*\)\s*:?\s*\|?\s*"
            + VALOR_NUMERICO
        ),
    },
    "Ajuste_Racao_Leitao_RLT": {
        "busca": (
            r"\bA(?:j)?uste\s+(?:"
            + ROTULO_RACAO_CALCULO
            + r"\s+)?(?:Leit[aã]o|Lelt[aã]o)\s*\(\s*RLT\s*\)\s*:?"
        ),
        "extracao": (
            r"\bA(?:j)?uste\s+(?:"
            + ROTULO_RACAO_CALCULO
            + r"\s+)?(?:Leit[aã]o|Lelt[aã]o)\s*\(\s*RLT\s*\)\s*:?"
            r"\s*\|?\s*"
            + VALOR_NUMERICO
        ),
    },
    "Ajuste_Mortalidade": {
        "busca": r"\bA(?:j)?uste\s+Mortalidade\s*:",
        "extracao": (
            r"\bA(?:j)?uste\s+Mortalidade\s*:\s*\|?\s*"
            + VALOR_NUMERICO
        ),
    },
    "Ajuste_Peso_Medio_PMT": {
        "busca": (
            r"\bA(?:j)?uste\s+Peso\s+M[ée]di[on]\s*\(\s*PMT\s*\)\s*:?"
        ),
        "extracao": (
            r"\bA(?:j)?uste\s+Peso\s+M[ée]di[on]\s*\(\s*PMT\s*\)\s*:?"
            r"\s*\|?\s*"
            + VALOR_NUMERICO
        ),
    },
    "Ajuste_Check_List": {
        "busca": (
            r"\bA(?:j)?uste\s+(?:Check|Ched[<k])\s*[-<]?\s*List\s*:?"
        ),
        "extracao": (
            r"\bA(?:j)?uste\s+(?:Check|Ched[<k])\s*[-<]?\s*List\s*:?"
            r"\s*\|?\s*"
            + VALOR_NUMERICO
        ),
    },
    "Resultado_Bruto_do_Lote": {
        "busca": r"\bResultado\s+Bruto\s+do\s+Lote\s*:?",
        "extracao": (
            r"\bResultado\s+Bruto\s+do\s+Lote\s*:?\s*\|?\s*"
            + VALOR_NUMERICO
        ),
    },
    "Valor_Renda": {
        "busca": r"\bValor\s*Renda\s*:?",
        "extracao": (
            r"\bValor\s*Renda\s*:?\s*\|?\s*"
            r"(?:Valor'\s*:\s*"
            + VALOR_NUMERICO_SEM_GRUPO
            + r"\s+)?"
            + VALOR_NUMERICO
        ),
    },
    "Valor_NF": {
        "busca": r"\bValor\s*NF\s*(?::|[•·]\s*\.?)",
        "extracao": (
            r"\bValor\s*NF\s*(?::|[•·]\s*\.?)\s*\|?\s*"
            r"(?:Vala\s*:\s*"
            + VALOR_NUMERICO_SEM_GRUPO
            + r"\s+)?"
            + VALOR_NUMERICO
        ),
    },
    "Valor_Total_Depositar": {
        "busca": r"\bValor\s*Total\s*[aàá]\s*Depositar\s*:?",
        "extracao": (
            r"\bValor\s*Total\s*[aàá]\s*Depositar\s*:?\s*\|?\s*"
            r"(?:Vadan\s*:\s*[0-9oO]+\s*,\s*[0-9oO]+\s+)?"
            + VALOR_NUMERICO
        ),
    },
    "Favorecido": {
        "busca": (
            r"\bFavor(?:ecido|eddo|edido|ecdo|eido|ecdido|ed[áa]do)\s*:"
        ),
        "extracao": (
            r"\bFavor(?:ecido|eddo|edido|ecdo|eido|ecdido|ed[áa]do)\s*:"
            r"\s*\|?\s*(.+?)(?=\t|$|\s+CPF/(?:CGC|CNPJ)\s*:)"
        ),
    },
    "Banco": {
        "busca": r"\bBanco\s*:",
        "extracao": (
            r"\bBanco\s*:\s*\|?\s*([0-9oO]+)"
            r"(?=\t|$|\s+"
            + ROTULO_AGENCIA
            + r"\s*:)"
        ),
    },
    "Agencia": {
        "busca": r"\b" + ROTULO_AGENCIA + r"\s*:",
        "extracao": (
            r"\b"
            + ROTULO_AGENCIA
            + r"\s*:\s*\|?\s*:?\s*([0-9oO]+(?:-[0-9oO]*)?)"
            r"(?=\t|$|\s+"
            + ROTULO_CONTA_CORRENTE
            + r"\s*:)"
        ),
    },
    "Conta_Corrente": {
        "busca": r"\b" + ROTULO_CONTA_CORRENTE + r"\s*:",
        "extracao": (
            r"\b"
            + ROTULO_CONTA_CORRENTE
            + r"\s*:\s*\|?\s*([0-9oO]+(?:-[0-9oO]+)?)"
            r"(?=\t|$|\s+D[áa]ta\s*:)"
        ),
    },
    "Data_Pagamento": {
        "busca": r"\bD[áa]ta\s*:",
        "extracao": (
            r"\bD[áa]ta\s*:\s*\|?\s*"
            r"([0-9oO]{1,2}/[0-9oO]{1,2}/[0-9oO]{4})"
        ),
    },
    "Cidade": {
        "busca": r"\bCidade\s*:",
        "extracao": (
            r"\bCidade\s*:\s*\|?\s*(.+?)(?=\t|$|\s+"
            + ROTULO_VALOR_DEPOSITO
            + r"\s*:)"
        ),
    },
    "Valor_Deposito": {
        "busca": r"\b" + ROTULO_VALOR_DEPOSITO + r"\s*:",
        "extracao": (
            r"\b"
            + ROTULO_VALOR_DEPOSITO
            + r"\s*:\s*\|?\s*"
            + VALOR_NUMERICO
        ),
    },
}

dados_filtros = {chave: [] for chave in filtros}

for csv_file in list_csv:
    for file in csv_file:
        df = pd.read_csv(file, dtype=str, encoding="utf-8")

        for chave, regex in filtros.items():

            dados_filtros[chave].extend(
                extrair_linha(
                    regex_busca=regex["busca"],
                    regex_extracao=regex["extracao"],
                    df=df,
                )
            )


new_dataFrame["id_file"] = ids_list

for chave, valores in dados_filtros.items():
    new_dataFrame[chave] = processar_dicionarios(
        id_unic_arr=ids_list,
        arr_temp=valores,
    )

# Corrige somente confusões numéricas comprovadas no contexto destes campos.
new_dataFrame["Racao_Leitao_Entregue_Real"] = new_dataFrame[
    "Racao_Leitao_Entregue_Real"
].str.replace(r"[oO]", "0", regex=True)

check_list = new_dataFrame["Check_List"].str.replace(r"[oO]", "0", regex=True)
check_compacto = check_list.str.fullmatch(r"\d{4}", na=False)
check_list.loc[check_compacto] = (
    check_list.loc[check_compacto].str[0]
    + "."
    + check_list.loc[check_compacto].str[1:]
)
new_dataFrame["Check_List"] = check_list

# O número de Integrado precisa corresponder ao início do ID do documento.
# Quando o rótulo não aparece ou o OCR altera algum dígito, usa como fallback
# os 11 dígitos iniciais do nome do próprio documento.

integrado_esperado = new_dataFrame["id_file"].str.split("-", n=1).str[0]

integrado_incorreto = (
    new_dataFrame["Integrado"].ne("nan")
    & new_dataFrame["Integrado"].ne(integrado_esperado)
)
new_dataFrame.loc[integrado_incorreto, "Integrado"] = "nan"

integrado_ausente = (
    new_dataFrame["Integrado"].eq("nan")
    & integrado_esperado.str.fullmatch(r"\d{11}", na=False)
)
new_dataFrame.loc[integrado_ausente, "Integrado"] = integrado_esperado[
    integrado_ausente
]


arquivo_saida = f"upl_tabela{get_date_now()}.csv"
new_dataFrame.to_csv(arquivo_saida, mode="w", index=False, encoding="utf-8-sig")
# new_dataFrame.to_excel(arquivo_saida, index=False) 

print(f"Arquivo salvo em: {arquivo_saida}")

# Mantém os nomes usados pelo filtro original.
integrado_ss = dados_filtros["Integrado"]
integrado_arr = new_dataFrame["Integrado"].tolist()
