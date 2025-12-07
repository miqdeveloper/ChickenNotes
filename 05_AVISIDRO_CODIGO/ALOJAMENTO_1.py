from math import nan
from operator import index
from os import remove
import pandas as pd
import re, ast, os
from tqdm import tqdm
from datetime import datetime
from typing import List, Tuple, Union, Pattern


lote_mtrz_arr = []
idade_mtrz_arr = []
linhagem_arr = []
incubatorio = []
peso_pinto = []
qtde_aloj_arr = []
total_incubados_arr = []

dados_index = []

id_uni = []


def create_dirs(dirs):
    """Create directories if they don't exist."""
    for dir_ in dirs:
        if not os.path.exists(dir_):
            os.mkdir(dir_)


def get_date_now():
    date_now = datetime.now()
    d = str(date_now.strftime("""_%d_%m_%Y"""))
    return d


file = ["Colunas_Criadas_CSV"]
create_dirs(file)

file_execel = r"Arquivos_Extraidos_CSV/dados_extraidos_avisidro.csv"

# df_2 = pd.read_csv(file_execel, encoding="utf-8", index_col=0)
df = pd.read_csv(file_execel, encoding="utf-8")
new_dataFrame = pd.DataFrame()


def remove_last_space(s):
    return re.sub(r" +$", " ", s).strip()


def find_dates(text):
    date_pattern = r"\b(0[1-9]|1[0-9]|2[0-9]|3[01])/(0[1-9]|1[0-2])/([0-9]{2})\b"
    return re.findall(date_pattern, text)


def remove_empty_spaces(lst):
    return list(filter(lambda item: item.strip() != "", lst))


def find_numbers(input_str):
    pattern = r"\b\d+\b"  # Padrão para números
    result = re.findall(pattern, input_str)
    return result


def find_letters(input_str):
    pattern = r"\b[A-Za-z\s]+\b"  # Padrão para letras
    result = re.findall(pattern, input_str)
    return result


def remove_chars(input_str):
    chars_to_remove = ["[", '"', "'", "nan", "]", ":"]
    for char in chars_to_remove:
        input_str = input_str.replace(char, "")
    return input_str


def remove_chars_s_points(input_str):
    chars_to_remove = ["[", '"', "'", "nan", "]"]
    for char in chars_to_remove:
        input_str = input_str.replace(char, "")
    return input_str


def converter_para_float(numero_str):
    numero_str = numero_str.replace(",", ".")
    return float(numero_str)

    # ITERA SOBRE O NOVO DATA FRAME FILTRADO


def find_number_id(id_):
    # id_ = ast.literal_eval(id_)
    # id_ = id_[0]
    padrao = r"\d+-\d+"
    id_f = re.match(padrao, str(id_))
    return id_f


def remover_duplicatas(lista):
    lista_sem_duplicatas = []
    [
        lista_sem_duplicatas.append(item)
        for item in lista
        if item not in lista_sem_duplicatas
    ]
    return lista_sem_duplicatas


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


def extrair_grupos(
    pattern: Union[str, Pattern], texto: str
) -> List[Union[str, Tuple[str, ...]]]:
    """
    Recebe uma expressão regular e um texto.
    Retorna uma lista de grupos capturados para cada correspondência encontrada.
    Se o padrão não corresponder, retorna lista vazia.
    """
    # abaixo transformamos pattern em um objeto regex se for string
    regex = re.compile(pattern) if isinstance(pattern, str) else pattern

    matches = regex.finditer(texto)
    resultado = []
    for m in matches:
        # m.groups() retorna uma tupla com os grupos de captura (sem o grupo 0)
        resultado.append(m.groups())
    return resultado


def dicio_obj(id: str, Data: str) -> dict:
    return {
        "id": id,
        "Data": Data,
    }


def process_():
    for idx in dados_index:
        for linha in range(2, 100):
            linha = df.iloc[idx + linha][0]
            linha = remove_chars(linha)

            l_ = remove_empty_spaces(re.split(r",\s+", linha, maxsplit=1))
            id_ll = l_[0]

            # LOTE_MTRZ
            lote_mtrz = l_[1]
            lote_mtrz = extrair_grupos(r"(\d{11}-\d{4}[A-Za-z]{2})", lote_mtrz)

            if lote_mtrz:
                l_t_f = (
                    str(lote_mtrz[0])
                    .replace("(", " ")
                    .replace(")", "")
                    .replace(",", "")
                    .replace("'", "")
                )

                lote_mtrz_arr.append(dicio_obj(id_ll, l_t_f))

            # IDADE_MTRZ
            glb_values = re.sub(r"(?:\s*,\s*)+$", "", str(l_[1]).strip())

            idade_mtrz = extrair_grupos(
                r"^(\d{11}-\d{4}[A-Za-z]{2})\s+(\d+(?:,\d+)?)\s+([A-Za-z0-9_]+)\s*,?\s+(.+?)\s*(\d+,\d+)\s*,?\s*(\d{1,3}(?:\.\d{3})*|\d+)\s*$",
                glb_values,
            )

            if idade_mtrz:

                idade_mtrz_f = idade_mtrz[0][1]
                # if not
                idade_mtrz_arr.append(dicio_obj(id_ll, idade_mtrz_f))

                # LINHAGEM
                lnhg_f = idade_mtrz[0][2].rstrip(",")
                lnhg_f = dicio_obj(id_ll, lnhg_f)
                linhagem_arr.append(lnhg_f)

                # INCUBATORIO
                incb_f = idade_mtrz[0][3].rstrip(",")
                incb_f = dicio_obj(id_ll, incb_f)
                incubatorio.append(incb_f)

                # PESO_PINTO
                peso_pt = idade_mtrz[0][4]
                peso_pt = dicio_obj(id_ll, peso_pt)
                peso_pinto.append(peso_pt)

                #  QTDE_ALOJ
                qtde_aloj = idade_mtrz[0][5]
                qtde_aloj = dicio_obj(id_ll, qtde_aloj)
                qtde_aloj_arr.append(qtde_aloj)
                
            # if not idade_mtrz:
            #     print(linha)
            if "Total Incubados" in linha:
                m = re.search(r"Total Incubados[^\d]*(\d{1,3}(?:\.\d{3})*)", linha)
                if m:
                    t_i = m.group(1)
                    total_incubados_arr.append(dicio_obj(id_ll, t_i))
                break


def main():
    global avcl_n
    integrado_nome_arr = []

    print("Filtrando aguarde...")
    for index, row in df.itertuples():
        line_item = str(row)

        new_str = re.sub(r"\[|\]", "", line_item)
        new_str = remove_empty_spaces(re.split(r",\s+", remove_chars(new_str), maxsplit=1))

        len_newStr = len(new_str)

        if find_number_id(new_str[0]):
            id_uni.append(new_str[0])
            id_l = new_str[0]

        # if "MOVIMENTAÇÃO (PINTOS/MATRIZES)" in str(new_str) or "Total Incubados" in  str(new_str) or "Total Incubados:" in  str(new_str) or "Total Incubados: " in  str(new_str) :
        if "MOVIMENTAÇÃO (PINTOS/MATRIZES)" in str(new_str) or "No Composto Lote Mtrz Idade Mtrz" in str(new_str):
            if len(new_str) > 1:
                dados_index.append(index)

    id_uni_f = remover_duplicatas(id_uni)
    process_()

    lote_mtrz_ = processar_dicionarios(id_uni_f, lote_mtrz_arr)
    idade_mtrz_ = processar_dicionarios(id_uni_f, idade_mtrz_arr)
    total_incubados_arr_ = processar_dicionarios(id_uni_f, total_incubados_arr)
    linhagem_ar = processar_dicionarios(id_uni_f, linhagem_arr)
    incubatorio_ = processar_dicionarios(id_uni_f, incubatorio)
    peso_pinto_ = processar_dicionarios(id_uni_f, peso_pinto)
    qtde_aloj_ = processar_dicionarios(id_uni_f, qtde_aloj_arr)

    new_dataFrame["CHAVE"] = id_uni_f
    new_dataFrame["LOTE_MTRZ"] = lote_mtrz_
    new_dataFrame["IDADE_MTRZ"] = idade_mtrz_
    new_dataFrame["LINHAGEM"] = linhagem_ar
    new_dataFrame["INCUBATORIO"] = incubatorio_
    new_dataFrame["PESO_PINTO"] = peso_pinto_
    new_dataFrame["QTDE_ALOJ"] = qtde_aloj_
    # #
    new_dataFrame["TOTAL_INCUBADOS"] = total_incubados_arr_

    # lista_c = [(va, id) for id, va in zip(dc_pr_arr, id_uni_f)]

    print("Salvando arquivo...")
    new_dataFrame.to_csv(
        f"{file[0]}/avisidro_tabela_alojamento_{get_date_now()}.csv",
        mode="w",
        index=False,
    )
    # input("Arquivo salvo com sucesso...")


main()
