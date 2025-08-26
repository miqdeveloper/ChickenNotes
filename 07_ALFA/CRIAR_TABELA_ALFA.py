from collections import OrderedDict
from glob import glob
from hmac import new
from operator import ne
from os import remove
import pprint
from threading import local
import pandas as pd
import re, ast, os
from datetime import datetime
from warnings import simplefilter


simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

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
    return re.sub(r' +$', ' ', s).strip()

def find_dates(text: str):
    date_pattern = r"\b(0[1-9]|1[0-9]|2[0-9]|3[01])/(0[1-9]|1[0-2])/([0-9]{2})\b"
    return re.findall(date_pattern, text)

def remove_empty_spaces(lst):
    return list(filter(lambda item: item.strip() != '', lst))

def find_numbers(input_str):
    pattern = r'\b\d+\b'  # Padrão para números
    result = re.findall(pattern, input_str)
    return result

def find_letters(input_str):
    pattern = r'\b[A-Za-z\s]+\b'  # Padrão para letras
    result = re.findall(pattern, input_str)
    return result

def remove_chars(input_str: str) -> str:
    chars_to_remove = ["[", "\"", "'", "nan", "]", ":", ".pdf"]
    for char in chars_to_remove:
        input_str = input_str.replace(char, "")
    return input_str

def remove_chars_s_points(input_str):
    chars_to_remove = ["[", "\"", "'", "nan", "]", "="]
    for char in chars_to_remove:
        input_str = input_str.replace(char, "")
    return input_str

def converter_para_float(numero_str):
    numero_str = numero_str.replace(',', '.')
    return float(numero_str)
            
    #ITERA SOBRE O NOVO DATA FRAME FILTRADO
    
def filter_pattern(text: str) -> str:
    pattern = re.compile(r'^(?:[A-Za-z]-)?\d+-\d+$')
    return '\n'.join(line.strip() for line in text.splitlines() if re.match(pattern, line.strip()))

def adicionar_espacos(texto):
    # Corrigir o formato final removendo espaços extras
    texto = re.findall(r'\d{1,3}(?:\.\d{3})*(?:,\d+)?', texto)
    return texto

def remove_points(texto: str) -> str:
    texto = texto.replace(".", "").replace(":", "")
    return texto

def separar_numeros(texto): 
    numeros = re.findall(r'\d+,\d+', texto) 
    # Junta os números encontrados com espaço entre eles 
    numeros_limpos = ' '.join(numeros)
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
        arr_id = [id_["id"] for id_ in arr_temp]
        
        arr_data_final = []
        for id_unico in id_unic_arr:
            if id_unico not in arr_id:
                f_l = {'Data': 'nan', 'id': id_unico}
            else:
                f_l = arr_temp[arr_id.index(id_unico)]
            arr_data_final.append(f_l)
        # Extraindo apenas os valores da chave 'Data'
        arr_data_final = [data['Data'] for data in arr_data_final]
        return arr_data_final

        # Exemplo de uso
        # id_unic_arr = ['id1', 'id2', 'id3']
        # arr_fomento = ['id2', 'id3']
        # custo_fomento_arr = [{'Data': '2022-01-01', 'id': 'id2'}, {'Data': '2023-03-04', 'id': 'id3'}]
        # resultado = processar_custos_fomento(id_unic_arr, arr_fomento, custo_fomento_arr)
        # print(resultado)  # Saída: ['nan', '2022-01-01', '2023-03-04']

def limpar_texto(texto):
    padrao = str(texto).replace(":", "").replace("CONVERSÃO META DA SEMANA", "").replace("CONVERSAO META DA SEMANA", "")
    # padrao = r'CONVERSÃO META DA SEMANA \d+\s*'
    # padrao = padrao.replace("CONVERSÃO META DA SEMANA", "").replace(":", "")
    
    
    return padrao


file=['Colunas_Criadas_CSV']
create_dirs(file)

file_execel = "Arquivos_Extraidos_CSV/file_csv.csv"

# df_2 = pd.read_csv(file_execel, encoding="utf-8", index_col=0)   
df = pd.read_csv(file_execel, encoding="utf-8")

new_dataFrame = pd.DataFrame()


id_unic_arr = []
parceiro_id_arr = []
parceiro_nome_arr = []
local_arr = []
prd_unit_arr = []
pocilga_arr = []
lt_suino_arr = []
user_arr = []
filial_arr = []
Matricula_arr = []
data_first_aloc_arr = []
data_last_aloc_arr = []
emissao_arr = []


for index, row in df.iterrows():
    id_l = str(row[0])
    id_unic_arr.append(id_l)


for index, row in df.iterrows():
    
    line_string = str(row[1])
    id_l = str(row[0])

    
    # nome_parceiro
    if "Parceiro" in line_string:
        
        pcr_s = remove_chars_s_points(line_string).replace("Parceiro", "").replace(".", "").replace(":", "")
        
        pcr_s = find_letters(pcr_s)[0]
        pcr_id = find_numbers(line_string)[0]
        
        
        parceiro_nome_arr.append({
            "id": id_l,
            "Data": pcr_s
        }) 

        parceiro_id_arr.append({ 
            "id": id_l,
            "Data": pcr_id
        })
    
    # Local
    if "Local" in line_string:
        
        local_s = remove_chars_s_points(line_string).replace("Local", "").replace(".", "").replace(":", "")
        local_s = remove_empty_spaces(find_letters(local_s))
        local_s = " ".join(local_s)
        
        local_arr.append({
            "id": id_l,
            "Data": local_s
        })

    # Unidade Producao
    if "Unidade Produção" in line_string:
        prd_und = find_numbers(line_string.replace("Unidade Produção",  "").replace(":", ""))
        prd_und = remove_empty_spaces(prd_und)
        prd_und = " ".join(prd_und)
        
        prd_unit_arr.append({
            "id": id_l,
            "Data": prd_und
        })
    
    # "Pocilga"
    if "Pocilga" in line_string:
        pcilga_s = (line_string).replace("Pocilga", "").replace(":", "").replace(".", "")
        pcilga_s = find_numbers(pcilga_s)[-1]
        pocilga_arr.append({
            "id": id_l,
            "Data": pcilga_s
        })
        
    # "Lote Suínos"   
    if "Lote Suínos" in line_string:
        
        lt_suino = remove_chars_s_points(line_string).replace("Lote Suínos", "").replace(".", "").replace(":", "")
        lt_suino_arr.append({
            "id": id_l,
            "Data": lt_suino
        })
    
    # Usuário
    if "Usuário" in line_string:
        
        user_s = remove_chars(line_string).replace("Usuário..", "").split(" ")
        user_s_s  = user_s[0]
        emiss_s = user_s[1]
        
        # emissao  
        
        emissao_arr.append({
            "id": id_l,
            "Data": emiss_s
        })
        
        user_arr.append({
            "id": id_l,
            "Data": user_s_s
        })

    # Filial
    if "Filial" in line_string:
        if find_numbers(line_string):
            flial_s =  find_numbers(line_string)
        
            filial_arr.append({
                
                "id": id_l,
                "Data": flial_s
                
        
            })
    
    # Matricula
    if "Matricula" in line_string:
        mtr_s = remove_chars(line_string).replace("Matricula", "").replace(".", "").replace(":", "")
        mtr_s = find_numbers(mtr_s)[0]
        
        Matricula_arr.append({
            "id": id_l,
            "Data": mtr_s
        })
        
    # "Data Primeiro Alojamento"
    if "Data Primeiro Alojamento" in line_string:
        dts_s = (line_string).replace("Data Primeiro Alojamento", "").replace(".", "").replace(":", "")
        dts_s = remove_empty_spaces(dts_s.split(" "))[0]
       
        data_first_aloc_arr.append({
            "id": id_l,
            "Data": dts_s
        })

        
        
    if "" in line_string:
        pass
    if "" in line_string:
        pass
    if "" in line_string:
        pass


id_unic_arr = list(OrderedDict.fromkeys(id_unic_arr))


parceiro_nome_arr = processar_dicionarios(id_unic_arr, parceiro_nome_arr)
parceiro_id_arr = processar_dicionarios(id_unic_arr, parceiro_id_arr)
pocilga_arr = processar_dicionarios(id_unic_arr, pocilga_arr)
lt_suino_arr = processar_dicionarios(id_unic_arr, lt_suino_arr)
user_arr = processar_dicionarios(id_unic_arr, user_arr)
emissao_arr = processar_dicionarios(id_unic_arr, emissao_arr)
filial_arr = processar_dicionarios(id_unic_arr, filial_arr)
Matricula_arr = processar_dicionarios(id_unic_arr, Matricula_arr)
data_first_aloc_arr = processar_dicionarios(id_unic_arr, data_first_aloc_arr)
# data_last_aloc_arr = processar_dicionarios(id_unic_arr, data_last_aloc_arr)


# Não aplique processar_dicionarios para arrays simples (listas de valores)
# parceiro_id_arr = processar_dicionarios(id_)

new_dataFrame["id"] = id_unic_arr
new_dataFrame["PARCEIRO_ID"] = parceiro_id_arr
new_dataFrame["NOME_PARCEIRO"] = parceiro_nome_arr
new_dataFrame["POCILGA"] = pocilga_arr
new_dataFrame["LOTE_SUINOS"] = lt_suino_arr
new_dataFrame["USUARIO"] = user_arr
new_dataFrame["EMISSAO"] = emissao_arr
new_dataFrame["MATRICULA"] = Matricula_arr
new_dataFrame["DATA_PRIMEIRO_ALOJAMENTO"] = data_first_aloc_arr


print("Salvando arquivo...")
new_dataFrame.to_csv(rf"Colunas_Criadas_CSV/alfa_tabela_{get_date_now()}.csv",  index=False)
input("Arquivo salvo com sucesso...")