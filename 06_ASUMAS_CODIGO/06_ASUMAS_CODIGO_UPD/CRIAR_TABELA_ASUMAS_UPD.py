import array
from calendar import c
from collections import OrderedDict
from glob import glob
from hmac import new
from math import e, nan
from os import remove
import pandas as pd
import re, ast, os
from datetime import datetime
from warnings import simplefilter


simplefilter(action="ignore", category=pd.errors.PerformanceWarning)


def remover_percent(string: array) -> list:
    """Remove percentage signs from a string."""
    arr = [s.replace("%", "") for s in string]
    return arr


def create_dirs(dirs):
    """Create directories if they don't exist."""
    for dir_ in dirs:
        if not os.path.exists(dir_):
            os.mkdir(dir_)


def get_date_now():
    date_now = datetime.now()
    d = str(date_now.strftime("""_%d_%m_%Y"""))
    return d


def remove_last_space(s):
    return re.sub(r" +$", " ", s).strip()


def find_dates(text: str):
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


def remove_chars(input_str: str) -> str:
    chars_to_remove = ["[", '"', "'", "nan", "]", ":", ".pdf", "/", "/"]
    for char in chars_to_remove:
        input_str = input_str.replace(char, "")
    return input_str


def remove_chars_s_points(input_str):
    chars_to_remove = ["[", '"', "'", "nan", "]", "="]
    for char in chars_to_remove:
        input_str = input_str.replace(char, "")
    return input_str


def converter_para_float(numero_str):
    numero_str = numero_str.replace(",", ".")
    return float(numero_str)

    # ITERA SOBRE O NOVO DATA FRAME FILTRADO


def filter_pattern(text: str) -> str:
    pattern = re.compile(r"^(?:[A-Za-z]-)?\d+-\d+$")
    return "\n".join(
        line.strip() for line in text.splitlines() if re.match(pattern, line.strip())
    )


def adicionar_espacos(texto):
    # Corrigir o formato final removendo espaços extras
    texto = re.findall(r"\d{1,3}(?:\.\d{3})*(?:,\d+)?", texto)
    return texto


def remove_points(texto: str) -> str:
    texto = texto.replace(".", "").replace(":", "")
    return texto


def separar_numeros(texto):
    numeros = re.findall(r"\d+,\d+", texto)
    # Junta os números encontrados com espaço entre eles
    numeros_limpos = " ".join(numeros)
    return numeros_limpos


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
    arr_id = [str(id_["id"]) for id_ in arr_temp]

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


def _norm(v):
    return v.translate(str.maketrans({"O": "0", "o": "0", "Ó": "0", "ó": "0"})).replace(
        ",", "."
    )


def limpar_texto(texto):
    padrao = (
        str(texto)
        .replace(":", "")
        .replace("CONVERSÃO META DA SEMANA", "")
        .replace("CONVERSAO META DA SEMANA", "")
    )
    # padrao = r'CONVERSÃO META DA SEMANA \d+\s*'
    # padrao = padrao.replace("CONVERSÃO META DA SEMANA", "").replace(":", "")

    return padrao


def dicio_obj(idl, value) -> dict:
    obj = {"Data": idl, "id": value}
    return obj


file = ["Colunas_Criadas_CSV"]
create_dirs(file)

file_execel = "output/output.csv"

# df_2 = pd.read_csv(file_execel, encoding="utf-8", index_col=0)
df = pd.read_csv(
    file_execel, encoding="utf-8", sep=",", engine="python", on_bad_lines="skip"
)
if "text" in df.columns:
    df = df.rename(columns={"text": "content"})


id_unic_arr = []
id_unic_s = []

new_dataFrame = pd.DataFrame()

integrado_map = {}
endereco_map = {}
municipio_map = {}
cpf_cgc_map = {}
fazenda_map = {}
lote_map = {}
capacidade_map = {}
referencia_map = {}
qtde_alojada_map = {}
qtde_mortos_map = {}
qtde_retorno_map = {}
peso_abatido_map = {}
data_alojamento_map = {}
suino_consumo_map = {}
qtde_abatido_map = {}
sexo_map = {}
peso_medio_aloj_map = {}
idade_media_map = {}
peso_recebido_map = {}
ajuste_nutricao_map = {}
peso_suino_map = {}
gpd_map = {}
racao_consumida_map = {}
ajuste_modal_map = {}
ajs_peso_leitao_map = {}
data_abate_map = {}
abate_femea_map = {}
venda_femea_map = {}
reprodutores_macho_map = {}
entrada_macho_map = {}
mortes_macho_map = {}
abate_macho_map = {}
venda_macho_map = {}
peso_entreg_fase_map = {}
ajuste_desmame_21d_map = {}
qtde_mortos_transp_map = {}
peso_total_aloj_map = {}
qtde_cab_faltantes_map = {}
qtde_cab_sinistro_map = {}
outras_perdas_map = {}
conv_ajustada_prev_map = {}
conv_alimentar_real_map = {}
conv_real_ajustada_map = {}
condenados_total_map = {}
mortalidade_prev_map = {}
condenados_parcial_map = {}
mortalidade_real_map = {}
condenacoes_prev_map = {}
dif_mort_map = {}
condenacoes_real_map = {}
ganho_peso_diario_prev_map = {}
peso_suino_map = {}
dif_cond_prev_real_map = {}
vlr_kg_leitao_map = {}
pc_basico_partilha_map = {}
percent_partilha_map = {}
kg_partilha_map = {}
rs_partilha_map = {}
vlr_kg_suino_map = {}
vlr_mortalidade_pct_map = {}
vlr_mortalidade_map = {}

vlr_pct_map = {}
vlr_kg_map = {}
vlr_rs_map = {}
vlr_rscab_map = {}
vlr_kg_racao_map = {}
avl_conv_perc_conv_map = {}
avl_conv_kg_conv_map = {}
avl_conv_rs_conv_map = {}
avl_conv_rs_cab_conv_map = {}


avl_conv_kg_racao_map = {}

avaliacao_condenacao_perc_map = {}
avaliacao_condenacao_kg_map = {}
avaliacao_condenacao_rs_map = {}
avaliacao_condenacao_rs_cab_map = {}

avaliacao_ganho_peso_diario_perc_map = {}
avaliacao_ganho_peso_diario_kg_map = {}
avaliacao_ganho_peso_diario_rs_map = {}
avaliacao_ganho_peso_diario_rs_cab_map = {}


avaliacao_checklist_perc_map = {}
avaliacao_checklist_kg_map = {}
avaliacao_checklist_rs_map = {}
avaliacao_checklist_rs_cab_map = {}

valor_auxilio_baixa_produtividade_perc_map = {}
valor_auxilio_baixa_produtividade_kg_map = {}
valor_auxilio_baixa_produtividade_rs_map = {}
valor_auxilio_baixa_produtividade_rs_cab_map = {}

resultado_bruto_lote_v1_map = {}
resultado_bruto_lote_v2_map = {}
resultado_bruto_lote_v3_map = {}
resultado_bruto_lote_v4_map = {}


for row in df.itertuples():

    line_item = row.filename
    content_line = str(row.content)
    # content_line = str(row.get("content", ""))
    new_string = remove_chars_s_points(content_line)

    separe_id_ = remove_chars(line_item)

    separe_id_ = separe_id_.replace("=", "").replace(" ", "")
    if "-" in separe_id_:
        separe_id_ = separe_id_.split("_pg")[0]
        id_unic_s.append(separe_id_)

    if "Integrado:" in new_string:
        m = re.search(
            r"Integrado:\s*\d+\s*-?\s*(.+?)(?=\s+-\s+CH|\s+GP|\s+Lote)", new_string
        )
        if m:
            integrado_map[separe_id_] = m.group(1).strip()
            # print(m.group(1))

    if "Endereço:" in new_string:
        # print(new_string)
        m = re.search(
            r"(?i)endere[cç]o\s*:\s*(.+?)(?=\s+t[^:]{0,20}:|$)",
            new_string,
            re.IGNORECASE,
        )
        if m:
            # print(m.group(1).strip())
            endereco_map[separe_id_] = m.group(1).strip()
            # print(endereco_map[separe_id_])
            # print(endereco_map[separe_id_])
            # print(m.group(1).strip())

    if (
        ("Municipio" in new_string)
        or ("Município" in new_string)
        or ("Muniapio" in new_string)
    ):
        m = re.search(
            r"Munic(?:ipio|ípio)\s*:\s*([A-Za-zÀ-ÖØ-öø-ÿ' ]+?)(?=\s+[A-Za-zÀ-ÖØ-öø-ÿ][^:]*:|$)",
            new_string,
            re.IGNORECASE,
        )
        if m:
            municipio_map[separe_id_] = m.group(1).strip()
        if not m:
            m = re.compile(
                r"\bMuni\w*\s*:\s*(?P<municipio>.+?)\s*\bCap\w*\s*:", re.IGNORECASE
            )
            m = m.search(new_string)
            if m:
                municipio_map[separe_id_] = m.group("municipio").strip()

    if "CPF/CGC" in new_string:
        m = re.search(
            r"CPF\s*/\s*CGC\s*:\s*(.*?)\s*(?=\s*[A-Za-zÀ-ÖØ-öø-ÿ][^:]*:|$)",
            new_string,
            re.IGNORECASE,
        )
        if m:
            seg = m.group(1).strip()
            # mcpf = re.search(r"\b\d{3}[.\s]?\d{3}[.\s]?\d{3}[-\s]?\d{2}\b", seg)
            # if mcpf:
            cpf_cgc_map[separe_id_] = seg.replace("—", "").strip()

    if "Fazenda" in new_string:
        m = re.search(
            r"Fazenda\s*:\s*(.*?)\s*(?=\s*[A-Za-zÀ-ÖØ-öø-ÿ][^:]*:|$)",
            new_string,
            re.IGNORECASE,
        )
        if m:
            fazenda_map[separe_id_] = m.group(1).strip()

    if "Lote" in new_string and "Integrado" in new_string:
        m = re.search(
            r"\bLote\s*:\s*(\S+)\s*(?=\s*[A-Za-zÀ-ÖØ-öø-ÿ][^:]*:|$)",
            new_string,
            re.IGNORECASE,
        )
        if m:
            lote_map[separe_id_] = m.group(1).strip()

    if "Técnico" in new_string:
        pass

    if "Capacidade(Matrizes):" in new_string:
        m = re.search(
            r"Capacidade\s*\(\s*Matrizes?\s*\)\s*:\s*([\d,]+)",
            new_string,
            flags=re.IGNORECASE,
        )
        if m:
            capacidade_map[separe_id_] = int(m.group(1))

    if (
        "Ref" in new_string
        or "Referência" in new_string
        or "Referencia:" in new_string
        or "Dt Referência:" in new_string
    ):
        m = re.search(
            r"\bRefer\w*\s*:\s*(\d{2}/\d{2}/\d{4})\b", new_string, flags=re.IGNORECASE
        )
        referencia = m.group(1) if m else None
        referencia_map[separe_id_] = referencia

    if "Dt Meta:" in new_string:
        m = re.search(
            r"(?i)\bDt\s*Meta\s*:\s*(\d{2}/\d{2}/\d{4})\b",
            new_string,
            flags=re.IGNORECASE,
        )
        if m:

            qtde_abatido = m.group(1).translate(
                str.maketrans({"O": "0", "o": "0", "Ó": "0", "ó": "0"})
            )

            qtde_alojada_map[separe_id_] = qtde_abatido

    if "Reprodutores Fêmea: " in new_string or "Reprodutores Femea: " in new_string:

        m = re.search(
            r"\bReprodutores\s*F[eê]mea\b\s*[:=-]?\s*([0-9Oo]+)\b",
            new_string,
            flags=re.IGNORECASE,
        )
        if m:

            qtde_mortos = (
                int(m.group(1).replace("O", "0").replace("o", "0")) if m else "nan"
            )
            qtde_mortos_map[separe_id_] = qtde_mortos

    if "Entrada Fêmea:" in new_string or "Entrada Femea:" in new_string:

        m = re.search(
            r"\bEntrada\s*F[eê]mea\b\s*[:=-]?\s*([0-9Oo]+)\b",
            new_string,
            flags=re.IGNORECASE,
        )
        if m:
            qtde_retorno = m.group(1)

        if not m:
            qtde_retorno = "nan"

        qtde_retorno_map[separe_id_] = qtde_retorno

    if "Mortes Fêmea" in new_string or "Mortes Femea" in new_string:

        m = re.search(
            r"\bMortes?\s*F[eê]mea\b[^0-9Oo]*?([0-9Oo]+)\b",
            new_string,
            flags=re.IGNORECASE,
        )
        mortes_femea = (
            int(m.group(1).replace("O", "0").replace("o", "0")) if m else "nan"
        )
        peso_abatido_map[separe_id_] = mortes_femea

    if "Abate Fêmea:" in new_string:
        m = re.search(
            r"\bAbate\s*F[eê]mea\b\s*[:=-]?\s*([0-9Oo]+)\b",
            new_string,
            flags=re.IGNORECASE,
        )
        abate_femea = (
            int(m.group(1).replace("O", "0").replace("o", "0")) if m else "nan"
        )
        abate_femea_map[separe_id_] = abate_femea

    if "Venda Fêmea:" in new_string or "Venda Femea:" in new_string:
        m = re.search(
            r"\bVenda\s*F[eê]mea\b\s*[:=-]?\s*([0-9Oo]+)\b",
            new_string,
            flags=re.IGNORECASE,
        )
        venda_femea = (
            int(m.group(1).replace("O", "0").replace("o", "0")) if m else "nan"
        )
        venda_femea_map[separe_id_] = venda_femea

    if "Reprodutores Macho:" in new_string:
        m = re.search(
            r"\bReprodutores\s*Macho\b\s*[:=-]?\s*([0-9Oo]+)\b",
            new_string,
            flags=re.IGNORECASE,
        )
        reprodutores_macho = (
            int(m.group(1).replace("O", "0").replace("o", "0")) if m else "nan"
        )
        reprodutores_macho_map[separe_id_] = reprodutores_macho

    if "Entrada Macho:" in new_string:
        m = re.search(
            r"\bEntrada\s*Macho\b\s*[:=-]?\s*([0-9Oo]+)\b",
            new_string,
            flags=re.IGNORECASE,
        )
        entrada_macho = (
            int(m.group(1).replace("O", "0").replace("o", "0")) if m else "nan"
        )
        entrada_macho_map[separe_id_] = entrada_macho

    if "Mortes Macho" in new_string:
        m = re.search(
            r"\bMortes?\s*Macho\b[^0-9Oo]*?([0-9Oo]+)\b",
            new_string,
            flags=re.IGNORECASE,
        )
        mortes_macho = (
            int(m.group(1).replace("O", "0").replace("o", "0")) if m else "nan"
        )
        mortes_macho_map[separe_id_] = mortes_macho

    if "Abate Macho:" in new_string:
        m = re.search(
            r"\bAbate\s*Macho\b\s*[:=-]?\s*([0-9Oo]+)\b",
            new_string,
            flags=re.IGNORECASE,
        )
        abate_macho = (
            int(m.group(1).replace("O", "0").replace("o", "0")) if m else "nan"
        )
        abate_macho_map[separe_id_] = abate_macho

    if "Venda Macho:" in new_string:
        m = re.search(
            r"\bVenda\s*Macho\b\s*[:=-]?\s*([0-9Oo]+)\b",
            new_string,
            flags=re.IGNORECASE,
        )
        venda_macho = (
            int(m.group(1).replace("O", "0").replace("o", "0")) if m else "nan"
        )
        venda_macho_map[separe_id_] = venda_macho

    if "Leitao Desmamado/Fêmea/Ano Prev:" in new_string:

        m = re.search(
            r"Leitao\s+Desmamado/Fêmea/Ano\s+Prev:\s*([0-9]+(?:[.,][0-9]+)?)",
            new_string,
            flags=re.IGNORECASE,
        )
        peso_recebido_raw = m.group(1) if m else None
        peso_recebido_map[separe_id_] = peso_recebido_raw

    if "Mortalidade Plantel Prev:" in new_string:

        m = re.search(
            r"Mortalidade\s+Plantel\s+Prev:\s*([0-9O]+(?:[.,/][0-9O]+)?)",
            new_string,
            flags=re.IGNORECASE,
        )
        ajuste_nutricao = m.group(1) if m else None
        ajuste_nutricao_map[separe_id_] = ajuste_nutricao

    if "Peso Médio Leitão Prev:" in new_string:
        m = re.search(
            r"Peso\s*M[eé]dio\s*Leit[aã]o\s*Prev:\s*([0-9]+(?:\.[0-9]+)?)",
            new_string,
            flags=re.IGNORECASE,
        )

        ajs_peso_suino = None
        if m:
            s = m.group(1).translate(
                str.maketrans({"O": "0", "o": "0", "Ó": "0", "ó": "0"})
            )
            s = s.replace(",", ".")
            ajs_peso_suino = float(s)

            peso_suino_map[separe_id_] = ajs_peso_suino

    if (
        "Leitao Desmamado/Fêmea/Ano Real:" in new_string
        or "Leitão Desmamado/F" in new_string
    ):

        m = re.search(
            r"Leit[aã]o\s+Desmamado\/F[eê]mea\/Ano\s+Real:\s*([0-9]+(?:\.[0-9]+)?)",
            new_string,
            flags=re.IGNORECASE,
        )
        if m:
            gpd = m.group(1)
            gpd_map[separe_id_] = gpd

    if "Mortalidade PlantelReal:" in new_string:

        m = re.search(
            r"Mortalidade\s*Plantel\s*Real:\s*([0-9]+(?:\.[0-9]+)?)(?![\s/,])",
            new_string,
            flags=re.IGNORECASE,
        )
        if m:
            raw = m.group(1)
            # corrige "O", "o", "Ó", "ó" → "0"
            raw = raw.translate(str.maketrans({"O": "0", "o": "0", "Ó": "0", "ó": "0"}))
            # remove qualquer letra que ainda reste
            raw = re.sub(r"[A-Za-z]", "", raw)
            # converte vírgula para ponto, se houver
            raw = raw.replace(",", ".")
            try:
                racao = int(float(raw))
            except ValueError:
                racao = None
        else:
            racao = None
        racao_consumida_map[separe_id_] = racao

    if "Peso Médio Leitão Real:" in new_string:

        m = re.search(
            r"Peso\s*M[eéê]dio\s*Leit[aã]o\s*Real:\s*([0-9]+(?:[.,][0-9]+)?)(?![\w/])",
            new_string,
            flags=re.IGNORECASE,
        )
        ajuste_modal = m.group(1) if m else None

        ajuste_modal_map[separe_id_] = ajuste_modal

    if "Ração Reprodutor/Fêmea/Ano Prev:" in new_string:

        m = re.search(
            r"(?i)ra[cç](?:ã|a)o\s+reprodutor\/f[eê]mea\/ano\s+prev\s*:\s*([0-9.]+)",
            new_string,
            flags=re.IGNORECASE,
        )
        if m:
            raw_leitao = m.group(1)
            try:
                ajs_peso_leitao = raw_leitao
            except ValueError:
                ajs_peso_leitao = None
        else:
            ajs_peso_leitao = None
        # store in a new map (create it at the top with the others)
        ajs_peso_leitao_map[separe_id_] = ajs_peso_leitao

    if "Ração/Leitão Entregue Prev:" in new_string:

        m = re.search(
            r"(?i)ra[cç](?:ã|a)o\/leita[oã]\s+entregue\s+prev\s*:\s*(?:check\s*list\s*:?\s*)?([0-9.]+)?",
            new_string,
        )
        if m:
            data_abate = m.group(1)
        else:
            data_abate = "None"
        # store in a new map (create it at the top with the others)
        data_abate_map[separe_id_] = data_abate

    if "Check List:" in new_string:
        m = re.search(
            r"(?i)\bch[e3]ck[\s\-\_]*li[s5]t\b[:\s]*([0-9]+(?:[.,][0-9]+)*)",
            new_string,
            flags=re.IGNORECASE,
        )
        peso_entreg_fase_map[separe_id_] = m.group(1) if m else "nan"

    if "Ração Reprodutor/Fêmea/Ano Real:" in new_string:

        m = re.search(
            r"(?i)ra[cç](?:ã|a)o\s+reprodutor\/f[eê]mea\/ano\s+real\s*:\s*([0-9]+(?:[.,][0-9]+)*)",
            new_string,
            flags=re.IGNORECASE,
        )
        ajuste_desmame_21d = m.group(1) if m else None
        ajuste_desmame_21d_map[separe_id_] = ajuste_desmame_21d
        # print(new_string)

    if "Ração/Leitão Entregue Real:" in new_string:
        m = re.search(
            r"(?i)ra[cç](?:ã|a)o\/leit(?:ã|a)o\s+entregue\s+real\s*:\s*([0-9]+(?:[.,][0-9]+)*)",
            new_string,
            flags=re.IGNORECASE,
        )
        qtde_mortos_transp = (
            float(
                m.group(1).translate(
                    str.maketrans({"O": "0", "o": "0", "Ó": "0", "ó": "0"})
                )
            )
            if m
            else "nan"
        )
        qtde_mortos_transp_map[separe_id_] = qtde_mortos_transp

    if "Qt.Leitões:" in new_string:

        m = re.search(
            r"qt\.?\s*leit(?:[õo]e?s)?\s*[:=]?\s*(\d+)",
            new_string,
            flags=re.IGNORECASE,
        )
        peso_total_aloj = m.group(1) if m else "nan"

        peso_total_aloj_map[separe_id_] = peso_total_aloj

    if "Vlr Leitão:" in new_string:

        m = re.search(
            r"vlr\.?\s*leit(?:[ãa]o|[õo]es)\s*[:=]?\s*(\d+(?:[.,]\d+)?)",
            new_string,
            flags=re.IGNORECASE,
        )

        qtde_cab_faltantes = m.group(1).replace("O", "0") if m else "nan"
        qtde_cab_faltantes_map[separe_id_] = qtde_cab_faltantes

    if "Vlr Reprodutor:" in new_string:

        m = re.search(
            r"v[il]r\.?\s*reprodutor\s*[:=]?\s*[dD]?(\d+(?:[.,]\d+)?)",
            new_string,
            flags=re.IGNORECASE,
        )

        qtde_cab_sinistro = m.group(1).replace("O", "0") if m else nan

        qtde_cab_sinistro_map[separe_id_] = qtde_cab_sinistro

    if "Vlr kg Ração Matriz:" in new_string or "Matriz:" in new_string:
        m = re.search(
            r"vlr\s*kg\s*ra[cç][aã]o\s*matriz\s*[:=]?\s*(\d+(?:[.,]\d+)?)",
            new_string,
            flags=re.IGNORECASE,
        )
        outras_perdas = (
            float(
                m.group(1).translate(
                    str.maketrans({"O": "0", "o": "0", "Ó": "0", "ó": "0"})
                )
            )
            if m
            else "nan"
        )
        outras_perdas_map[separe_id_] = outras_perdas

    if "Vlr kg Ração Leitão:" in new_string or "Vlr kg" in new_string:
        pattern = re.compile(
            r"(?ix)(?:v[l1iI]{0,3}r\W*kg\W*ra[cç](?:ao|ão)\W*leit(?:ao|ão))\W*[:\-]?\W*([0-9]+(?:[.,][0-9]+)?)",
            flags=re.IGNORECASE,
        )

        m = pattern.search(new_string)
        conv_ajustada_prev = m.group(1) if m else "nan"
        conv_ajustada_prev_map[separe_id_] = conv_ajustada_prev

    if "Básico de Partilha:" in new_string:

        m = re.search(
            r"""(?ix)                       # ignore case + verbose
            básico\ de\ partilha[:\s]*      # texto fixo
            (?P<percentual>\d{1,2}(?:[.,]\d+)?)   # percentual
            (?:\s+                          # opcional R$/Cab
                (?P<r_cab>\d+(?:[.,]\d+)?)
            )?
            \s+
            (?P<valor>\d+(?:[.,]\d+)?)      # valor em R$
            """,
            new_string,
        )

        percentual = m.group("percentual") if m and m.group("percentual") else "nan"

        r_cab = m.group("r_cab") if m and m.group("r_cab") else "nan"

        valor = m.group("valor") if m and m.group("valor") else "nan"

        # opcional converter para float
        def norm_number(s):
            return (
                float(s.replace(",", ".").replace("O", "0").replace("o", "0"))
                if s != "nan"
                else "nan"
            )

        percentual = norm_number(percentual)
        r_cab = norm_number(r_cab)
        valor = norm_number(valor)

        conv_alimentar_real_map[separe_id_] = {
            "percentual": percentual,
            "r_cab": r_cab,
            "valor": valor,
        }
        # print(conv_alimentar_real_map[separe_id_]['percentual'])
        # conv_alimentar_real = m.group(1) if m else "nan"
        # conv_alimentar_real_map[separe_id_] = conv_alimentar_real

    if "Ajuste Leitão Desmamado(LDFA):" in new_string:
        # print(new_string)
        pattern = re.compile(
            r"""(?ix)                             # case-insensitive + verbose
    ajuste\ leit[ãa]o\ desmamado          # texto fixo “Ajuste Leitão Desmamado”
    \s* \(?LDFA\)?                       # pode ter parênteses em LDFA
    \s*[:\-]?\s*                         # separador possivel (: ou -)
    
    (?P<coef_a>\d+(?:[.,]\d+)?)          # coeficiente A (obrigatório)
    
    (?:                                  # grupo opcional para coeficiente B
        \s+
        (?P<coef_b>\d+(?:[.,]\d+)?)
    )?
    
    (?:                                  # grupo opcional para valor em R$
        \s+
        (?P<valor>                       # captura valor bruto
            \d+(?:[.,]\d+)?              # valor normal
            (?:                         # ou valor sujo com / no meio
                /[0-9]*\d+[.,]?\d*
            )?
        )
    )?
    """,
        )

        m = pattern.search(new_string)
        # print(m)

        coef_a = m.group("coef_a") if m and m.group("coef_a") else "nan"
        coef_b = m.group("coef_b") if m and m.group("coef_b") else "nan"
        valor_ldfa = m.group("valor") if m and m.group("valor") else "nan"

        # converter para float (normalizando vírgula e caracteres estranhos)
        def to_float(x):
            try:
                return float(x.replace(",", "."))
            except:
                return float("nan")

        coef_a = to_float(coef_a)
        coef_b = to_float(coef_b)
        valor_ldfa = to_float(valor_ldfa)

        conv_real_ajustada_map[separe_id_] = {
            "coef_a": coef_a,
            "coef_b": coef_b,
            "valor": valor_ldfa,
        }
        # print(conv_real_ajustada_map[separe_id_])

    if "Ajuste Ração Reprodutor(RRFA):" in new_string:
        pattern = re.compile(
            r"""(?ix)                                # verbose + ignorecase
            ajuste\ ra[cç]\w*\s*reprodutor           # "Ajuste Ração Reprodutor"
            \s* \(?RRFA\)?                           # (RRFA) com ou sem parênteses
            \s*[:\-]?\s*                             # separador opcional (: ou -)

            (?P<coef_a>[-−]?\s*\d+(?:[.,]\d+)?)      # coeficiente A (com sinal opcional)

            (?:\s*[-–—]?\s*                          # possível separador tipo "-" entre campos
                (?P<coef_b>[-−]?\s*\d+(?:[.,]\d+)?)
            )?                                       # coef B opcional

            (?:\s*[-–—]?\s*                          # possível separador tipo "-" antes do valor
                (?P<valor>[-−]?\s*\d+(?:[.,]\d+)?(?:/[0-9]+[.,]?\d*)?)
            )?                                       # valor opcional (aceita uma barra suja)
            """,
        )

        m = pattern.search(new_string)

        raw_a = m.group("coef_a") if m and m.group("coef_a") else "nan"
        raw_b = m.group("coef_b") if m and m.group("coef_b") else "nan"
        raw_val = m.group("valor") if m and m.group("valor") else "nan"

        # Normalização robusta: troca OCR, mantem sinal, lida com barras, vírgulas, pontos repetidos
        def clean_and_float(s):
            if s is None:
                return float("nan")
            if isinstance(s, (int, float)):
                return float(s)
            s = s.strip()
            if s.lower() in ("nan", ""):
                return float("nan")

            # Correções OCR comuns
            s = s.replace("O", "0").replace("o", "0")
            s = s.replace("−", "-").replace("—", "-").replace("–", "-")
            s = s.replace(",", ".")

            # Remover caracteres indesejados mantendo digits, dot, minus, slash
            s = re.sub(r"[^0-9\.\-\/]", "", s)

            # Se houver múltiplas barras, pegar a última parte que parece número
            if "/" in s:
                parts = [p for p in s.split("/") if p != ""]
                # tentar escolher a parte mais plausível (preferir que contenha '.')
                chosen = parts[-1]
                for p in reversed(parts):
                    if re.match(r"^-?\d+\.\d+$", p):
                        chosen = p
                        break
                s = chosen

            # Se houver mais de um ponto (.) — juntar tudo mantendo o primeiro como decimal
            if s.count(".") > 1:
                first, rest = s.split(".", 1)
                rest = re.sub(r"\.", "", rest)  # remove outros pontos
                s = first + "." + rest

            # Se string for só um sinal ou vazio, retornar nan
            if re.fullmatch(r"[-]+", s) or s == "":
                return float("nan")

            try:
                return float(s)
            except Exception:
                # tentativa final: extrair o primeiro número com regex
                mnum = re.search(r"-?\d+(?:\.\d+)?", s)
                if mnum:
                    return float(mnum.group(0))
                return float("nan")

        coef_a = clean_and_float(raw_a)
        coef_b = clean_and_float(raw_b)
        valor_rrfa = clean_and_float(raw_val)

        condenados_total_map[separe_id_] = {
            "coef_a": coef_a,
            "coef_b": coef_b,
            "valor": valor_rrfa,
        }

    if "Ajuste Ração Leitão (RLT):" in new_string:
        # print(new_string)
        pattern = re.compile(
            r"""(?ix)
            ajuste\ ra[cç]\w*\s*leit[oã]o             # texto fixo “Ajuste Ração Leitão”
            \s* \(?RLT\)?                             # (RLT) com ou sem parênteses
            \s*[:\-]?\s*                              # separador opcional

            (?P<coef_a>[-−]?\s*\d+(?:[.,]\d+)?)        # coeficiente A (com sinal opcional)

            (?:\s*[-–—]?\s*                            # possível separador “-” entre campos
                (?P<coef_b>[-−]?\s*\d+(?:[.,]\d+)?)
            )?                                         # coef B opcional

            (?:\s*[-–—]?\s*                            # possível separador antes do valor
                (?P<valor>[-−]?\s*\d+(?:[.,]\d+)?(?:/[0-9]+[.,]?\d*)?)
            )?                                         # valor opcional
            """,
        )

        m = pattern.search(new_string)

        raw_a = m.group("coef_a") if m and m.group("coef_a") else "nan"
        raw_b = m.group("coef_b") if m and m.group("coef_b") else "nan"
        raw_val = m.group("valor") if m and m.group("valor") else "nan"

        # Função de normalização robusta (tratando vírgula, sinais e barras)
        def clean_and_float(s):
            if s is None:
                return "nan"
            if isinstance(s, (int, str)):
                return s
            s = s.strip()
            if s.lower() in ("nan", ""):
                return "nan"

            # normalizações típicas (OCR, vírgulas, sinais)
            s = s.replace("O", "0").replace("o", "0")
            s = s.replace("−", "-").replace("—", "-").replace("–", "-")
            s = s.replace(",", ".")

            # remover chars indesejados mantendo dígitos, ponto, sinal e barra
            s = re.sub(r"[^0-9\.\-\/]", "", s)

            # se houver barra, usar parte final plausível
            if "/" in s:
                parts = [p for p in s.split("/") if p != ""]
                chosen = parts[-1]
                for p in reversed(parts):
                    if re.match(r"^-?\d+\.\d+$", p):
                        chosen = p
                        break
                s = chosen

            # se houver múltiplos pontos, manter apenas o primeiro como decimal
            if s.count(".") > 1:
                first, rest = s.split(".", 1)
                rest = re.sub(r"\.", "", rest)
                s = first + "." + rest

            if re.fullmatch(r"[-]+", s) or s == "":
                return "nan"

            try:
                return s
            except:
                mnum = re.search(r"-?\d+(?:\.\d+)?", s)
                if mnum:
                    return mnum.group(0)
                return float("nan")

        coef_a = clean_and_float(raw_a)
        coef_b = clean_and_float(raw_b)
        valor_rlt = clean_and_float(raw_val)

        mortalidade_prev_map[separe_id_] = {
            "coef_a": coef_a,
            "coef_b": coef_b,
            "valor": valor_rlt,
        }

        # Armazena o valor (ex: '3.050')
        # mortalidade_prev_map[separe_id_] = mortalidade_prev

    if "Ajuste Mortalidade:" in new_string:
        pattern = re.compile(
            r"""(?ix)
            ajuste\ mortalidade
            \s*[:\-]?\s*

            (?P<coef_a>[-−]?\s*\d+(?:[.,]\d+)?)

            (?:\s*[-–—]?\s*
                (?P<coef_b>[-−]?\s*\d+(?:[.,]\d+)?)
            )?

            (?:\s*[-–—]?\s*
                (?P<valor>[-−]?\s*\d+(?:[.,]\d+)?(?:/[0-9]+[.,]?\d*)?)
            )?
            """,
        )

        m = pattern.search(new_string)

        coef_a = m.group("coef_a").strip() if m and m.group("coef_a") else "nan"
        coef_b = m.group("coef_b").strip() if m and m.group("coef_b") else "nan"
        valor_mortalidade = (
            m.group("valor").strip() if m and m.group("valor") else "nan"
        )

        # limpeza leve: normalizar vírgula → ponto, remover espaços desnecessários
        def clean_str(s):
            if not s or s.lower() == "nan":
                return "nan"
            # trocar O errados por 0 e vírgula por ponto
            s = s.replace("O", "0").replace("o", "0")
            s = s.replace(",", ".")
            # remover espaços múltiplos
            s = re.sub(r"\s+", "", s)
            return s

        coef_a = clean_str(coef_a)
        coef_b = clean_str(coef_b)
        valor_mortalidade = clean_str(valor_mortalidade)

        condenados_parcial_map[separe_id_] = {
            "coef_a": coef_a,
            "coef_b": coef_b,
            "valor": valor_mortalidade,
        }
        # print(condenados_parcial_map[separe_id_])

    # condenados_parcial_map[separe_id_] = condenados_parcial

    if "Ajuste Peso Médio(PMT):" in new_string:
        # print(new_string)
        pattern_pmt = re.compile(
            r"""(?ix)
            ajuste\ peso\ m[eé]dio\s*\(PMT\)    # cabeçalho
            \s*[:\-]?\s*
            (?P<coef_a>[-−]?\s*\d+(?:[.,]\d+)?)            # coef A (obrigatório)
            (?:\s*[-–—]?\s*(?P<coef_b>[-−]?\s*\d+(?:[.,]\d+)?))?   # coef B (opcional)
            (?:\s*[-–—]?\s*(?P<valor>[-−]?\s*\d+(?:[.,]\d+)?(?:/[0-9]+[.,]?\d*)?))? # valor (opcional)
            """,
        )

        m = pattern_pmt.search(new_string)

        def clean_str(s):
            if not s:
                return "nan"
            s = s.strip().replace("O", "0").replace("o", "0").replace(",", ".")
            s = re.sub(r"\s+", "", s)
            return s

        mortalidade_real_map[separe_id_] = {
            "coef_a": (
                clean_str(m.group("coef_a")) if m and m.group("coef_a") else "nan"
            ),
            "coef_b": (
                clean_str(m.group("coef_b")) if m and m.group("coef_b") else "nan"
            ),
            "valor": clean_str(m.group("valor")) if m and m.group("valor") else "nan",
        }
        # print(mortalidade_real_map[separe_id_])
        # mortalidade_real_map[separe_id_] = mortalidade_real

    if "Ajuste Check-List:" in new_string:
        pattern_check = re.compile(
            r"""(?ix)
            ajuste\ check-?list
            \s*[:\-]?\s*
            (?P<coef_a>[-]?\s*\d+(?:[.,]\d+)?)
            (?:\s*[-]?\s*(?P<coef_b>[-]?\s*\d+(?:[.,]\d+)?))?
            (?:\s*[-]?\s*(?P<valor>[-]?\s*\d+(?:[.,]\d+)?(?:/[0-9]+[.,]?\d*)?))?
            """,
        )

        m = pattern_check.search(new_string)

        def clean_str(s):
            if not s:
                return "nan"
            s = s.strip().replace("O", "0").replace("o", "0").replace(",", ".")
            return re.sub(r"\s+", "", s)

        condenacoes_prev_map[separe_id_] = {
            "coef_a": (
                clean_str(m.group("coef_a")) if m and m.group("coef_a") else "nan"
            ),
            "coef_b": (
                clean_str(m.group("coef_b")) if m and m.group("coef_b") else "nan"
            ),
            "valor": clean_str(m.group("valor")) if m and m.group("valor") else "nan",
        }
        # condenacoes_prev_map[separe_id_] = condenacoes_prev

    if "Resultado Bruto do Lote:" in new_string:

        pattern_result = re.compile(
            r"""(?ix)
        resultado\ bruto\ do\ lote     # texto fixo
        \s*[:\-]?\s*

        (?P<campo_a>[-]?\s*\d+(?:[.,]\d+)?)   # primeiro número

        (?:                                  # segundo número (opcional)
            [^\d\-]*                         # permite chars sujos no meio
            (?P<campo_b>[-]?\s*\d+(?:[.,]\d+)?)
        )?

        (?:                                  # terceiro número (opcional)
            [^\d\-]*                         # permite chars sujos entre grupos
            (?P<valor>[-]?\s*\d+(?:[.,]\d+)?(?:/[0-9]+[.,]?\d*)?)
        )?
        """,
        )

        m = pattern_result.search(new_string)

        def clean_str(s):
            if not s:
                return "nan"
            s = s.strip()
            s = s.replace("O", "0").replace("o", "0")  # correção OCR comum
            s = s.replace(",", ".")
            # remove chars indesejados, mantém dígitos, ponto, barra e sinal
            s = re.sub(r"[^0-9\.\-\/]", "", s)
            # colapsa múltiplos pontos (mantém o primeiro como decimal)
            if s.count(".") > 1:
                first, rest = s.split(".", 1)
                rest = re.sub(r"\.", "", rest)
                s = first + "." + rest
            return s if s != "" else "nan"

        dif_mort_map[separe_id_] = {
            "campo_a": (
                clean_str(m.group("campo_a")) if m and m.group("campo_a") else "nan"
            ),
            "campo_b": (
                clean_str(m.group("campo_b")) if m and m.group("campo_b") else "nan"
            ),
            "valor": clean_str(m.group("valor")) if m and m.group("valor") else "nan",
        }

    if "Desconto Senar Normal" in new_string:
        # print(new_string)

        pattern_senar = re.compile(
            r"""(?ix)
            desconto\ senar\ normal        # texto fixo
            \s*[:\-]?\s*
            (?P<valor1>[-]?\s*\d+(?:[.,]\d+)?)    # primeiro valor
            (?:\s+
                (?P<valor2>[-]?\s*\d+(?:[.,]\d+)?)
            )?                                  # segundo valor opcional
            (?:\s+
                (?P<valor3>[-]?\s*\d+(?:[.,]\d+)?)
            )?                                  # terceiro valor opcional
            """,
        )

        m = pattern_senar.search(new_string)

        def clean_str(s):
            if not s:
                return "nan"
            s = s.strip().replace("O", "0").replace("o", "0")
            s = s.replace(",", ".")
            s = re.sub(r"[^0-9\.\-]", "", s)
            return s if s != "" else "nan"

        condenacoes_real_map[separe_id_] = {
            "valor1": (
                clean_str(m.group("valor1")) if m and m.group("valor1") else "nan"
            ),
            "valor2": (
                clean_str(m.group("valor2")) if m and m.group("valor2") else "nan"
            ),
            "valor3": (
                clean_str(m.group("valor3")) if m and m.group("valor3") else "nan"
            ),
        }

        # condenacoes_real_map[separe_id_] = condenacoes_real

    # if "Ganho Peso Diario Prev:" in new_string:
    # 	m = re.search(
    # 		r"Ganho\s*Peso\s*Diario\s*Prev\s*:\s*([\d\.,]+)",
    # 		new_string,
    # 		flags=re.IGNORECASE,
    # 	)

    # # PARA O ASSISTENTE DE IA - REVISE  TODAS AS REGEX
    # if "Ajuste peso Suino:" in new_string:
    # 	m = re.search(
    # 		r"\bAjuste\s*peso\s*Su[ií]no\s*:\s*(?P<ajuste_peso_suino>[0-9OoÓó]+(?:[.,][0-9OoÓó]+)*)",
    # 		new_string,
    # 		flags=re.IGNORECASE,
    # 	)
    # 	if m:
    # 		raw_ajuste_peso_suino = (
    # 			m.group("ajuste_peso_suino")
    # 			.translate(str.maketrans({"O": "0", "o": "0", "Ó": "0", "ó": "0"}))
    # 			.replace(",", ".")
    # 		)
    # 		try:
    # 			ajuste_peso_suino = float(raw_ajuste_peso_suino)
    # 		except ValueError:
    # 			ajuste_peso_suino = None
    # 	else:
    # 		ajuste_peso_suino = None
    # 	peso_suino_map[separe_id_] = ajuste_peso_suino

    # if "Dif. Cond (PrevXReal):" in new_string:
    # 	m = re.search(
    # 		r"Dif\.?\s*Cond\s*\(PrevXReal\)\s*:\s*([-]?\s*[\d\.,]+)",
    # 		new_string,
    # 		flags=re.IGNORECASE,
    # 	)
    # 	dif_cond_prev_real = m.group(1).replace(" ", "") if m else "nan"
    # 	# store in a new map (create it at the top with the others)
    # 	dif_cond_prev_real_map[separe_id_] = dif_cond_prev_real

    # if (
    # 	"Kg Leitão:" in new_string
    # 	or "VirKg Leitão:" in new_string
    # 	or "KgLeitão:" in new_string
    # ):
    # 	m = re.search(
    # 		r"(?:VIr\s*)?Kg\s*Leitão:\s*([0-9.,]+)", new_string, re.IGNORECASE
    # 	)
    # 	if m:
    # 		vlr_kg_leitao = m.group(1).replace(",", ".")
    # 		vlr_kg_leitao_map[separe_id_] = vlr_kg_leitao
    # 	else:
    # 		vlr_kg_leitao_map[separe_id_] = None

    # # % Kg R$ R$/Cab
    # if "Pc Básico de Partilha:" in new_string:
    # 	m = re.search(
    # 		r"Pc Básico de Partilha:\s*([0-9.,]+)\s+([^\s]+)\s+([0-9.,]+)\s+(.+)",
    # 		new_string,
    # 	)
    # 	if m:
    # 		pc_basico = m.group(1).replace(",", ".")
    # 		percent = m.group(2)
    # 		kg = m.group(3).replace(",", ".")
    # 		rs = m.group(4).replace(" ", "")

    # 		# print(cab, new_string)
    # 		pc_basico_partilha_map[separe_id_] = pc_basico
    # 		percent_partilha_map[separe_id_] = percent
    # 		kg_partilha_map[separe_id_] = kg
    # 		rs_partilha_map[separe_id_] = rs
    # 	else:
    # 		pc_basico_partilha_map[separe_id_] = None
    # 		percent_partilha_map[separe_id_] = None
    # 		kg_partilha_map[separe_id_] = None
    # 		rs_partilha_map[separe_id_] = None

    # if (
    # 	"Vlr Kg Suíno:" in new_string
    # 	or "Kg Suíno:" in new_string
    # 	or "Vir Kg" in new_string
    # ):
    # 	m = re.search(
    # 		r"\b(?:Vir|Vlr)\s*Kg\s*Su[ií]no\s*:\s*([0-9.,-]+)",
    # 		new_string,
    # 		flags=re.IGNORECASE,
    # 	)
    # 	if m:
    # 		vlr_kg_suino = m.group(1).replace(",", ".")
    # 		vlr_kg_suino_map[separe_id_] = vlr_kg_suino

    # # % Kg R$ R$/Cab
    # if "Avaliação Mortalidade:" in new_string:
    # 	# Captura os quatro valores sequenciais (G1, G2, G3, G4)
    # 	m = re.search(
    # 		r"Avalia[çc][ãa]o\s*Mortalidade\s*:\s*(-?\s*[\d\.\/,]+)\s+(-?\s*[\d\.\/,]+)\s+(-?\s*[\d\.\/,]+)\s+(-?\s*[\d\.\/,]+)",
    # 		new_string,
    # 		flags=re.IGNORECASE,
    # 	)

    # 	if m:
    # 		vlr_pct_bruto = m.group(1)
    # 		vlr_kg_bruto = m.group(2)
    # 		vlr_rs_bruto = m.group(3)
    # 		vlr_rscab_bruto = m.group(4)

    # 		# Função de limpeza para normalizar os dados (remover espaços e o erro '/')
    # 		def limpar_valor(valor_bruto):
    # 			return valor_bruto.replace(" ", "").replace("/", "")

    # 		vlr_pct = limpar_valor(vlr_pct_bruto)
    # 		vlr_kg = limpar_valor(vlr_kg_bruto)
    # 		vlr_rs = limpar_valor(vlr_rs_bruto)
    # 		vlr_rscab = limpar_valor(vlr_rscab_bruto)

    # 		# Junta os valores em uma string separada por ponto

    # 		vlr_pct_map[separe_id_] = vlr_pct
    # 		vlr_kg_map[separe_id_] = vlr_kg
    # 		vlr_rs_map[separe_id_] = vlr_rs
    # 		vlr_rscab_map[separe_id_] = vlr_rscab

    # 	else:
    # 		vlr_pct_map[separe_id_] = None
    # 		vlr_kg_map[separe_id_] = None
    # 		vlr_rs_map[separe_id_] = None
    # 		vlr_rscab_map[separe_id_] = None

    # if "kg Ração:" in new_string:
    # 	pattern = re.compile(
    # 		r"(?i)\bvi(?:r|l)?k{0,2}g?\s*ração\s*[:\-]?\s*(\d+(?:[.,]\d+)?)"
    # 	)

    # 	m = pattern.search(new_string)

    # 	if m:
    # 		vlr_kg_racao = m.group(1)
    # 		vlr_kg_racao_map[separe_id_] = vlr_kg_racao

    # 	else:
    # 		vlr_kg_racao = "nan"
    # 		vlr_kg_racao_map[separe_id_] = vlr_kg_racao

    # if "Avaliação Conversão:" in new_string:
    # 	m = re.search(
    # 		r"(?i)avaliação\s*conversão\s*:\s*-?\s*"
    # 		r"(-?[0-9OoÓó]+(?:[.,][0-9OoÓó]+)?)\s*-?\s*"
    # 		r"(-?[0-9OoÓó]+(?:[.,][0-9OoÓó]+)?)\s*-?\s*"
    # 		r"(-?[0-9OoÓó]+(?:[.,][0-9OoÓó]+)?)\s*-?\s*"
    # 		r"(-?[0-9OoÓó]+(?:[.,][0-9OoÓó]+)?)",
    # 		new_string,
    # 		flags=re.IGNORECASE,
    # 	)

    # 	if m:

    # 		avl_conv_perc_conv_map[separe_id_] = _norm(m.group(1))

    # 		avl_conv_kg_conv_map[separe_id_] = _norm(m.group(2))
    # 		avl_conv_rs_conv_map[separe_id_] = _norm(m.group(3))
    # 		avl_conv_rs_cab_conv_map[separe_id_] = _norm(m.group(4))

    # 	else:
    # 		avl_conv_perc_conv_map[separe_id_] = "nan"
    # 		avl_conv_kg_conv_map[separe_id_] = "nan"
    # 		avl_conv_rs_conv_map[separe_id_] = "nan"
    # 		avl_conv_rs_cab_conv_map[separe_id_] = "nan"

    # if "Avaliação Condenação:" in new_string:
    # 	m = re.search(
    # 		r"(?i)avaliação\s*condena(?:ção|cao)\s*:\s*-?\s*"
    # 		r"(-?[0-9OoÓó]+(?:[.,][0-9OoÓó]+)?)\s*-?\s*"
    # 		r"(-?[0-9OoÓó]+(?:[.,][0-9OoÓó]+)?)\s*-?\s*"
    # 		r"(-?[0-9OoÓó]+(?:[.,][0-9OoÓó]+)?)\s*-?\s*"
    # 		r"(-?[0-9OoÓó]+(?:[.,][0-9OoÓó]+)?)",
    # 		new_string,
    # 		flags=re.IGNORECASE,
    # 	)

    # 	if m:
    # 		cond_perc = _norm(m.group(1))
    # 		cond_kg = _norm(m.group(2))
    # 		cond_rs = _norm(m.group(3))
    # 		cond_rs_cab = _norm(m.group(4))
    # 	else:
    # 		cond_perc = cond_kg = cond_rs = cond_rs_cab = "nan"

    # 	avaliacao_condenacao_perc_map[separe_id_] = cond_perc
    # 	avaliacao_condenacao_kg_map[separe_id_] = cond_kg
    # 	avaliacao_condenacao_rs_map[separe_id_] = cond_rs
    # 	avaliacao_condenacao_rs_cab_map[separe_id_] = cond_rs_cab

    # if "Avaliação Ganho de Peso diario:" in new_string:

    # 	m = re.search(
    # 		r"(?i)avaliação\s*ganho\s*de\s*peso\s*di[aá]r(?:io|o)\s*:\s*-?\s*"
    # 		r"(-?[0-9OoÓó]+(?:[.,/][0-9OoÓó]+)?)\s*-?\s*"
    # 		r"(-?[0-9OoÓó]+(?:[.,/][0-9OoÓó]+)?)\s*-?\s*"
    # 		r"(-?[0-9OoÓó]+(?:[.,/][0-9OoÓó]+)?)\s*-?\s*"
    # 		r"(-?[0-9OoÓó]+(?:[.,/][0-9OoÓó]+)?)",
    # 		new_string,
    # 		flags=re.IGNORECASE,
    # 	)

    # 	if m:
    # 		ganho_peso_perc = _norm(m.group(1))
    # 		ganho_peso_kg = _norm(m.group(2))
    # 		ganho_peso_rs = _norm(m.group(3))
    # 		ganho_peso_rs_cab = _norm(m.group(4))

    # 	else:

    # 		ganho_peso_perc = ganho_peso_kg = ganho_peso_rs = ganho_peso_rs_cab = "nan"

    # 	avaliacao_ganho_peso_diario_perc_map[separe_id_] = ganho_peso_perc
    # 	avaliacao_ganho_peso_diario_kg_map[separe_id_] = ganho_peso_kg
    # 	avaliacao_ganho_peso_diario_rs_map[separe_id_] = ganho_peso_rs
    # 	avaliacao_ganho_peso_diario_rs_cab_map[separe_id_] = ganho_peso_rs_cab

    # if "Avaliação Check-List:" in new_string or "Avaliação Check-List" in new_string:
    # 	m = re.search(
    # 		r"(?i)avaliação\s*check[-\s]*list\s*(?::)?\s*-?\s*"
    # 		r"(-?[0-9OoÓó]+(?:[.,/][0-9OoÓó]+)?)\s*-?\s*"
    # 		r"(-?[0-9OoÓó]+(?:[.,/][0-9OoÓó]+)?)\s*-?\s*"
    # 		r"(-?[0-9OoÓó]+(?:[.,/][0-9OoÓó]+)?)\s*-?\s*"
    # 		r"(-?[0-9OoÓó]+(?:[.,/][0-9OoÓó]+)?)",
    # 		new_string,
    # 		flags=re.IGNORECASE,
    # 	)

    # 	if m:
    # 		checklist_perc = _norm(m.group(1))
    # 		checklist_kg = _norm(m.group(2))
    # 		checklist_rs = _norm(m.group(3))
    # 		checklist_rs_cab = _norm(m.group(4))
    # 	else:
    # 		checklist_perc = checklist_kg = checklist_rs = checklist_rs_cab = "nan"

    # 	avaliacao_checklist_perc_map[separe_id_] = checklist_perc
    # 	avaliacao_checklist_kg_map[separe_id_] = checklist_kg
    # 	avaliacao_checklist_rs_map[separe_id_] = checklist_rs
    # 	avaliacao_checklist_rs_cab_map[separe_id_] = checklist_rs_cab

    # if "Valor Auxilio Baixa Produtividade:" in new_string:
    # 	m = re.search(
    # 		r"(?i)valor\s*auxili(?:o|ó)\s*baixa\s*produtiv\w*\s*(?::)?\s*-?\s*"
    # 		r"(-?[0-9OoÓó]+(?:[.,/][0-9OoÓó]+)?)\s*-?\s*"
    # 		r"(-?[0-9OoÓó]+(?:[.,/][0-9OoÓó]+)?)\s*-?\s*"
    # 		r"(-?[0-9OoÓó]+(?:[.,/][0-9OoÓó]+)?)\s*-?\s*"
    # 		r"(-?[0-9OoÓó]+(?:[.,/][0-9OoÓó]+)?)",
    # 		new_string,
    # 		flags=re.IGNORECASE,
    # 	)

    # 	if m:
    # 		aux_bp_perc = _norm(m.group(1))
    # 		aux_bp_kg = _norm(m.group(2))
    # 		aux_bp_rs = _norm(m.group(3))
    # 		aux_bp_rs_cab = _norm(m.group(4))
    # 	else:
    # 		aux_bp_perc = aux_bp_kg = aux_bp_rs = aux_bp_rs_cab = "nan"

    # 	valor_auxilio_baixa_produtividade_perc_map[separe_id_] = aux_bp_perc
    # 	valor_auxilio_baixa_produtividade_kg_map[separe_id_] = aux_bp_kg
    # 	valor_auxilio_baixa_produtividade_rs_map[separe_id_] = aux_bp_rs
    # 	valor_auxilio_baixa_produtividade_rs_cab_map[separe_id_] = aux_bp_rs_cab

    # if "Resultado Bruto do Lote:" in new_string:
    # 	m = re.search(
    # 	r"(?i)resultado\s*bruto\s*do\s*lote\s*(?::)?\s*-?\s*"
    # 	r"(-?[0-9OoÓó]+(?:[.,/][0-9OoÓó]+)?)?"
    # 	r"(?:\s+-?\s*([0-9OoÓó]+(?:[.,/][0-9OoÓó]+)?))?"
    # 	r"(?:\s+-?\s*([0-9OoÓó]+(?:[.,/][0-9OoÓó]+)?))?"
    # 	r"(?:\s+-?\s*([0-9OoÓó]+(?:[.,/][0-9OoÓó]+)?))?",
    # 	new_string,
    # 	flags=re.IGNORECASE)

    # 	if m:
    # 		resultado_bruto_valor1 = _norm(m.group(1)) if m.group(1) else "nan"
    # 		resultado_bruto_valor2 = _norm(m.group(2)) if m.group(2) else "nan"
    # 		resultado_bruto_valor3 = _norm(m.group(3)) if m.group(3) else "nan"
    # 		resultado_bruto_valor4 = _norm(m.group(4)) if m.group(4) else "nan"
    # 	else:
    # 		resultado_bruto_valor1 = resultado_bruto_valor2 = resultado_bruto_valor3 = resultado_bruto_valor4 = "nan"

    # 	resultado_bruto_lote_v1_map[separe_id_] = resultado_bruto_valor1
    # 	resultado_bruto_lote_v2_map[separe_id_] = resultado_bruto_valor2
    # 	resultado_bruto_lote_v3_map[separe_id_] = resultado_bruto_valor3
    # 	resultado_bruto_lote_v4_map[separe_id_] = resultado_bruto_valor4

    # if "" in new_string:
    # 	pass
    # if "" in new_string:
    # 	pass

    # ganho_peso_diario_prev = m.group(1) if m else "nan"

    # ganho_peso_diario_prev_map[separe_id_] = ganho_peso_diario_prev


# a_arr = []

id_unic_arr = list(OrderedDict.fromkeys(id_unic_s))
# key_integrado_arr = list(OrderedDict.fromkeys(key_integrado_arr))
# nome_integrado_arr = list(OrderedDict.fromkeys(nome_integrado_arr))
# name_arr = list(set(nome_integrado_arr))

# f = len(test_arr)


# print("Contagem: id_unic_arr", len(id_unic_arr))

new_dataFrame["ID_INTEGRAD"] = id_unic_arr

new_dataFrame["INTEGRADO"] = [integrado_map.get(i, "") for i in id_unic_arr]
new_dataFrame["ENDERECO"] = [endereco_map.get(i, "") for i in id_unic_arr]

new_dataFrame["MUNICIPIO"] = [municipio_map.get(i, "") for i in id_unic_arr]
new_dataFrame["CPF_CGC"] = [cpf_cgc_map.get(i, "") for i in id_unic_arr]
new_dataFrame["FAZENDA"] = [fazenda_map.get(i, "") for i in id_unic_arr]
new_dataFrame["LOTE"] = [lote_map.get(i, "") for i in id_unic_arr]

new_dataFrame["CAPACIDADE"] = [capacidade_map.get(i, "") for i in id_unic_arr]
new_dataFrame["REFERENCIA"] = [referencia_map.get(i, "") for i in id_unic_arr]

new_dataFrame["DATA_META"] = [qtde_alojada_map.get(i, "") for i in id_unic_arr]
new_dataFrame["REPRODUTORES_FEMEA"] = [qtde_mortos_map.get(i, "") for i in id_unic_arr]
new_dataFrame["ENTRADA_FEMEA"] = [qtde_retorno_map.get(i, "") for i in id_unic_arr]

new_dataFrame["MORTES_FEMEA"] = [peso_abatido_map.get(i, "") for i in id_unic_arr]
new_dataFrame["ABATE_FEMEA"] = [abate_femea_map.get(i, "") for i in id_unic_arr]
new_dataFrame["VENDA_FEMEA"] = [venda_femea_map.get(i, "") for i in id_unic_arr]
new_dataFrame["REPRODUTORES_MACHO"] = [
    reprodutores_macho_map.get(i, "") for i in id_unic_arr
]
new_dataFrame["ENTRADA_MACHO"] = [entrada_macho_map.get(i, "") for i in id_unic_arr]
new_dataFrame["MORTES_MACHO"] = [mortes_macho_map.get(i, "") for i in id_unic_arr]
new_dataFrame["ABATE_MACHO"] = [abate_macho_map.get(i, "") for i in id_unic_arr]
new_dataFrame["VENDA_MACHO"] = [venda_macho_map.get(i, "") for i in id_unic_arr]
new_dataFrame["LEITAO_DESMAMADO_FEMEA_ANO_PREV"] = [
    peso_recebido_map.get(i, "") for i in id_unic_arr
]
new_dataFrame["MORTALIDADE_PLANTEL_PREV"] = [
    ajuste_nutricao_map.get(i, "") for i in id_unic_arr
]
new_dataFrame["PESO_MEDIO_LEITAO_PREV"] = [
    peso_suino_map.get(i, "") for i in id_unic_arr
]
new_dataFrame["LEITAO_DESMAMADO_FEMEA_ANO_REAL"] = [
    gpd_map.get(i, "") for i in id_unic_arr
]
new_dataFrame["MORTALIDADE_PLANTE_REAL"] = [
    racao_consumida_map.get(i, "") for i in id_unic_arr
]
new_dataFrame["PESO_MEDIO_LEITAO_REAL"] = [
    ajuste_modal_map.get(i, "") for i in id_unic_arr
]

new_dataFrame["RACAO_REPRODUTOR_FEMEA_ANO_PREV"] = [
    ajs_peso_leitao_map.get(i, "") for i in id_unic_arr
]
new_dataFrame["RACAO_LEITAO_ENTREGUE_PREV"] = [
    data_abate_map.get(i, "") for i in id_unic_arr
]
new_dataFrame["CHECK_LIST"] = [peso_entreg_fase_map.get(i, "") for i in id_unic_arr]
new_dataFrame["RACAO_REPRODUTOR_FEMEA_ANO_REAL"] = [
    ajuste_desmame_21d_map.get(i, "") for i in id_unic_arr
]

new_dataFrame["RACAO_LEITAO_ENTREGUE_REAL"] = [
    qtde_mortos_transp_map.get(i, "") for i in id_unic_arr
]
new_dataFrame["QT_LEITOES"] = [peso_total_aloj_map.get(i, "") for i in id_unic_arr]

new_dataFrame["VLR_LEITAO"] = [qtde_cab_faltantes_map.get(i, "") for i in id_unic_arr]

new_dataFrame["VLR_PRODUTOR"] = [qtde_cab_sinistro_map.get(i, "") for i in id_unic_arr]
new_dataFrame["VLR_KG_RACAO_MATRIZ"] = [
    outras_perdas_map.get(i, "") for i in id_unic_arr
]

new_dataFrame["VLR_KG_RACAO_LEITAO"] = [
    conv_ajustada_prev_map.get(i, "") for i in id_unic_arr
]


new_dataFrame["BASICO_PERCENTUAL"] = [
    conv_alimentar_real_map.get(i, {}).get("percentual", float("nan"))
    for i in id_unic_arr
]

new_dataFrame["BASICO_R_CAB"] = [
    conv_alimentar_real_map.get(i, {}).get("r_cab", float("nan")) for i in id_unic_arr
]

new_dataFrame["BASICO_VALOR"] = [
    conv_alimentar_real_map.get(i, {}).get("valor", float("nan")) for i in id_unic_arr
]


new_dataFrame["AJUSTE_LDFA_PORCETAGE"] = [
    conv_real_ajustada_map.get(i, {}).get("coef_a", float("nan")) for i in id_unic_arr
]

new_dataFrame["AJUSTE_LDFA_REAL_CAB"] = [
    conv_real_ajustada_map.get(i, {}).get("coef_b", float("nan")) for i in id_unic_arr
]


new_dataFrame["AJUSTE_LDFA_REAL_REAL"] = [
    conv_real_ajustada_map.get(i, {}).get("valor", float("nan")) for i in id_unic_arr
]

new_dataFrame["RRFA_%"] = [
    condenados_total_map.get(i, {}).get("coef_a", ("nan")) for i in id_unic_arr
]
new_dataFrame["RRFA_R$_CAB"] = [
    condenados_total_map.get(i, {}).get("coef_b", ("nan")) for i in id_unic_arr
]
new_dataFrame["RRFA_REAL_$"] = [
    condenados_total_map.get(i, {}).get("valor", ("nan")) for i in id_unic_arr
]


new_dataFrame["AJUSTE_RACAO_LEITAO_RLT_%"] = [
    mortalidade_prev_map.get(i, {}).get("coef_a", ("nan")) for i in id_unic_arr
]


new_dataFrame["AJUSTE_RACAO_LEITAO_RLT_$_CAB"] = [
    mortalidade_prev_map.get(i, {}).get("coef_b", ("nan")) for i in id_unic_arr
]


new_dataFrame["AJUSTE_RACAO_LEITAO_RLT_$"] = [
    mortalidade_prev_map.get(i, {}).get("valor", ("nan")) for i in id_unic_arr
]


new_dataFrame["AJUSTE_MORTALIDADE_%"] = [
    condenados_parcial_map.get(i, {}).get("coef_a", ("nan")) for i in id_unic_arr
]


new_dataFrame["AJUSTE_MORTALIDADE_R$_CAB"] = [
    condenados_parcial_map.get(i, {}).get("coef_b", ("nan")) for i in id_unic_arr
]


new_dataFrame["AJUSTE_MORTALIDADE_R$"] = [
    condenados_parcial_map.get(i, {}).get("valor", ("nan")) for i in id_unic_arr
]


new_dataFrame["AJUSTE_MORTALIDADE_R$"] = [
    condenados_parcial_map.get(i, {}).get("valor", ("nan")) for i in id_unic_arr
]


new_dataFrame["AJUSTE_PESO_MEDIO_PMT_%"] = [
    mortalidade_real_map.get(i, {}).get("coef_a", ("nan")) for i in id_unic_arr
]


new_dataFrame["AJUSTE_PESO_MEDIO_PMT_R$_CAB"] = [
    mortalidade_real_map.get(i, {}).get("coef_b", ("nan")) for i in id_unic_arr
]

new_dataFrame["AJUSTE_PESO_MEDIO_PMT_R$"] = [
    mortalidade_real_map.get(i, {}).get("valor", ("nan")) for i in id_unic_arr
]


new_dataFrame["AJUSTE_CHECK_LIST_%"] = [
    condenacoes_prev_map.get(i, {}).get("coef_a", ("nan")) for i in id_unic_arr
]

new_dataFrame["AJUSTE_CHECK_LIST_R$_CAB"] = [
    condenacoes_prev_map.get(i, {}).get("coef_b", ("nan")) for i in id_unic_arr
]

new_dataFrame["AJUSTE_CHECK_LIST_R$"] = [
    condenacoes_prev_map.get(i, {}).get("valor", ("nan")) for i in id_unic_arr
]


new_dataFrame["RESULTADO_BRUTO_LOTE_%"] = [
    dif_mort_map.get(i, {}).get("campo_a", ("nan")) for i in id_unic_arr
]

new_dataFrame["RESULTADO_BRUTO_LOTE_R$_CAB"] = [
    dif_mort_map.get(i, {}).get("campo_b", ("nan")) for i in id_unic_arr
]


new_dataFrame["RESULTADO_BRUTO_LOTE_R$_CAB"] = [
    dif_mort_map.get(i, {}).get("valor", ("nan")) for i in id_unic_arr
]


new_dataFrame["DESCONTO_SENAR_NORMAL_DEBITO"] = [
    dif_mort_map.get(i, {}).get("valor1", ("nan")) for i in id_unic_arr
]


new_dataFrame["DESCONTO_SENAR_NORMAL_CREDITO"] = [
    dif_mort_map.get(i, {}).get("valor2", ("nan")) for i in id_unic_arr
]


new_dataFrame["DESCONTO_SENAR_NORMAL_R$_CAB"] = [
    dif_mort_map.get(i, {}).get("valor3", ("nan")) for i in id_unic_arr
]


# new_dataFrame["QTDE_ABATIDO"] = [qtde_abatido_map.get(i, "") for i in id_unic_arr]
# new_dataFrame["SEXO"] = [sexo_map.get(i, "") for i in id_unic_arr]
# new_dataFrame["PESO_MEDIO_ALOJ"] = [peso_medio_aloj_map.get(i, "") for i in id_unic_arr]
# new_dataFrame["IDADE_MEDIA"] = [idade_media_map.get(i, "") for i in id_unic_arr]
# new_dataFrame["PESO_SUINO"] = [peso_suino_map.get(i, "") for i in id_unic_arr]

# new_dataFrame["RACAO_CONSUMIDA"] = [racao_consumida_map.get(i, "") for i in id_unic_arr]


# new_dataFrame["CONV_REAL_AJUSTADA"] = [
#     conv_real_ajustada_map.get(i, "") for i in id_unic_arr
# ]
# new_dataFrame["QTDE_CONDENADOS_TOTAL"] = [
#     condenados_total_map.get(i, "") for i in id_unic_arr
# ]

# new_dataFrame["DIF_COND_PREV_REAL"] = [
#     dif_cond_prev_real_map.get(i, "") for i in id_unic_arr
# ]

# new_dataFrame["VLR_KG_LEITAO"] = [vlr_kg_leitao_map.get(i, "") for i in id_unic_arr]

# new_dataFrame["PC_BASICO_PARTILHA"] = [
#     pc_basico_partilha_map.get(i, "") for i in id_unic_arr
# ]
# new_dataFrame["PERCENT_PARTILHA"] = [
#     percent_partilha_map.get(i, "") for i in id_unic_arr
# ]
# new_dataFrame["KG_PARTILHA"] = [kg_partilha_map.get(i, "") for i in id_unic_arr]
# new_dataFrame["RS_PARTILHA"] = [rs_partilha_map.get(i, "") for i in id_unic_arr]
# new_dataFrame["VLR_KG_SUINO"] = [vlr_kg_suino_map.get(i, "") for i in id_unic_arr]
# new_dataFrame["AVALIACAO_MORTALIDADE_VLR_PCT"] = [
#     vlr_pct_map.get(i, "") for i in id_unic_arr
# ]
# new_dataFrame["AVALIACAO_MORTALIDADE_VLR_KG"] = [
#     vlr_kg_map.get(i, "") for i in id_unic_arr
# ]
# new_dataFrame["AVALIACAO_MORTALIDADE_VLR_RS"] = [
#     vlr_rs_map.get(i, "") for i in id_unic_arr
# ]
# new_dataFrame["AVALIACAO_MORTALIDADE_VLR_RSCAB"] = [
#     vlr_rscab_map.get(i, "") for i in id_unic_arr
# ]

# new_dataFrame["VLR_KG_RACAO"] = [vlr_kg_racao_map.get(i, "") for i in id_unic_arr]

# new_dataFrame["AVL_CONVERSAO_PERC_CONV"] = [
#     avl_conv_perc_conv_map.get(i, "") for i in id_unic_arr
# ]
# new_dataFrame["AVL_CONVERSAO_KG_CONV"] = [
#     avl_conv_kg_conv_map.get(i, "") for i in id_unic_arr
# ]
# new_dataFrame["AVL_CONVERSAO_RS_CONV"] = [
#     avl_conv_rs_conv_map.get(i, "") for i in id_unic_arr
# ]
# new_dataFrame["AVL_CONVERSAO_RS_CAB_CONV"] = [
#     avl_conv_rs_cab_conv_map.get(i, "") for i in id_unic_arr
#  ]

# new_dataFrame["AVALIACAO_CONDENACAO_PERC"] = [
#     avaliacao_condenacao_perc_map.get(i, "") for i in id_unic_arr
# ]
# new_dataFrame["AVALIACAO_CONDENACAO_KG"] = [
#     avaliacao_condenacao_kg_map.get(i, "") for i in id_unic_arr
# ]
# new_dataFrame["AVALIACAO_CONDENACAO_RS"] = [
#     avaliacao_condenacao_rs_map.get(i, "") for i in id_unic_arr
# ]
# new_dataFrame["AVALIACAO_CONDENACAO_RS_CAB"] = [
#     avaliacao_condenacao_rs_cab_map.get(i, "") for i in id_unic_arr
# ]

# new_dataFrame["AVALIACAO_GANHO_PESO_DIARIO_PERC"] = [
#     avaliacao_ganho_peso_diario_perc_map.get(i, "") for i in id_unic_arr
# ]
# new_dataFrame["AVALIACAO_GANHO_PESO_DIARIO_KG"] = [
#     avaliacao_ganho_peso_diario_kg_map.get(i, "") for i in id_unic_arr
# ]
# new_dataFrame["AVALIACAO_GANHO_PESO_DIARIO_RS"] = [
#     avaliacao_ganho_peso_diario_rs_map.get(i, "") for i in id_unic_arr
# ]
# new_dataFrame["AVALIACAO_GANHO_PESO_DIARIO_RS_CAB"] = [
#     avaliacao_ganho_peso_diario_rs_cab_map.get(i, "") for i in id_unic_arr
# ]

# new_dataFrame["AVALIACAO_CHECKLIST_PERC"] = [
#     avaliacao_checklist_perc_map.get(i, "") for i in id_unic_arr
# ]
# new_dataFrame["AVALIACAO_CHECKLIST_KG"] = [
#     avaliacao_checklist_kg_map.get(i, "") for i in id_unic_arr
# ]
# new_dataFrame["AVALIACAO_CHECKLIST_RS"] = [
#     avaliacao_checklist_rs_map.get(i, "") for i in id_unic_arr
# ]
# new_dataFrame["AVALIACAO_CHECKLIST_RS_CAB"] = [
#     avaliacao_checklist_rs_cab_map.get(i, "") for i in id_unic_arr
# ]


# new_dataFrame["VALOR_AUXILIO_BAIXA_PRODUTIVIDADE_PERC"] = [
#     valor_auxilio_baixa_produtividade_perc_map.get(i, "") for i in id_unic_arr
# ]
# new_dataFrame["VALOR_AUXILIO_BAIXA_PRODUTIVIDADE_KG"] = [
#     valor_auxilio_baixa_produtividade_kg_map.get(i, "") for i in id_unic_arr
# ]
# new_dataFrame["VALOR_AUXILIO_BAIXA_PRODUTIVIDADE_RS"] = [
#     valor_auxilio_baixa_produtividade_rs_map.get(i, "") for i in id_unic_arr
# ]
# new_dataFrame["VALOR_AUXILIO_BAIXA_PRODUTIVIDADE_RS_CAB"] = [
#     valor_auxilio_baixa_produtividade_rs_cab_map.get(i, "") for i in id_unic_arr
# ]

# new_dataFrame["RESULTADO_BRUTO_LOTE_V1"] = [
#     resultado_bruto_lote_v1_map.get(i, "") for i in id_unic_arr
# ]
# new_dataFrame["RESULTADO_BRUTO_LOTE_V2"] = [
#     resultado_bruto_lote_v2_map.get(i, "") for i in id_unic_arr
# ]
# new_dataFrame["RESULTADO_BRUTO_LOTE_V3"] = [
#     resultado_bruto_lote_v3_map.get(i, "") for i in id_unic_arr
# ]
# new_dataFrame["RESULTADO_BRUTO_LOTE_V4"] = [
#     resultado_bruto_lote_v4_map.get(i, "") for i in id_unic_arr
# ]

print("Salvando arquivo...")
new_dataFrame.to_csv(
    rf"Colunas_Criadas_CSV/asumas_tabela_{get_date_now()}.csv", index=False
)
print("Arquivo salvo com sucesso...")
