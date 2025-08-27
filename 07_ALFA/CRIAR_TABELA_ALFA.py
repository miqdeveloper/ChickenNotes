from collections import OrderedDict
from glob import glob
from hmac import new
from operator import ne
from os import remove
import pprint
from threading import local
from tkinter import N
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
    d = str(date_now.strftime("""%d_%m_%Y"""))
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
            arr_temp (list[str]): description arranjo temporario
            arr_data_separate (list[dict]): array de dados separados, uma lista com diciionarios

        Returns:
            list[str]: description
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
###################################
data_last_ulent_arr=[]
total_dia_aloj_arr=[]
dt_aloj_pond_arr=[]
dt_saida_pond_arr=[]
dia_aloj_pond_arr=[]
perc_morte_arr=[]
total_morte_arr=[]
mort_propr_arr=[]
###################################
qtd_elim_cond_arr=[]
qtd_mort_rom_arr=[]
qtd_leit_ent_arr=[]
qtd_leit_said_arr=[]
diferenca_arr=[]
pes_leit_ent_arr=[]
pes_saida_arr=[]
pes_med_leit_ent_arr=[]
pes_med_saida_arr=[]
ganho_peso_arr=[]
gpmd_pond_arr=[]
conv_alim_real_arr=[]
conv_alim_real_ajst_arr=[]
conv_alim_arr=[]
crit_conv_alim_arr=[]
crit_mortalidade_arr=[]
crit_percent_peso_ideal_arr=[]
crit_percent_check_list_arr = []
crit_gmpd_arr = []
table_value_anual_arr = []
value_descont_animal_arr =  []

for index, row in df.iterrows():
    id_l = str(row.iloc[0])
    id_unic_arr.append(id_l)

for index, row in df.iterrows():
    
    line_string = str(row.iloc[1])
    id_l = str(row.iloc[0])

    
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

##############################################################################################################
    # "Data Última Entrega"
    if "Data Última Entrega" in line_string:
        dts_u = (line_string).replace("Data Última Entrega", "").replace(".", "").replace(":", "")
        dts_u = remove_empty_spaces(dts_u.split(" "))[0]
       
        data_last_ulent_arr.append({
            "id": id_l,
            "Data": dts_u
        })

    # "Total Dias Alojamento"
    if "Total Dias Alojamento" in line_string:
        t_d_a = (line_string).replace("Total Dias Alojamento", "").replace(".", "").replace(":", "")
        t_d_a = remove_empty_spaces(t_d_a.split(" "))[0]
       
        total_dia_aloj_arr.append({
            "id": id_l,
            "Data": t_d_a
        })

    # "Dt.Aloj.Ponderada"
    if "Dt.Aloj.Ponderada" in line_string:
        d_a_p = (line_string).replace("Dt.Aloj.Ponderada", "").replace(".", "").replace(":", "")
        d_a_p = remove_empty_spaces(d_a_p.split(" "))[0]
       
        dt_aloj_pond_arr.append({
            "id": id_l,
            "Data": d_a_p
        })

    # "Data Saída Ponderada"
    if "Data Saída Ponderada" in line_string:
        dt_sa_po = (line_string).replace("Data Saída Ponderada", "").replace(".", "").replace(":", "")
        dt_sa_po = remove_empty_spaces(dt_sa_po.split(" "))[0]
       
        dt_saida_pond_arr.append({
            "id": id_l,
            "Data": dt_sa_po
        }) 

    # "Dias Aloj.Ponderada"  
    if "Dias Aloj.Ponderada" in line_string:
        di_aloj_po = (line_string).replace("Dias Aloj.Ponderada", "").replace(".", "").replace(":", "")
        di_aloj_po = remove_empty_spaces(di_aloj_po.split(" "))[0]
       
        dia_aloj_pond_arr.append({
            "id": id_l,
            "Data": di_aloj_po
        })

    # "Percentual Mortalidade"  
    if "Percentual Mortalidade" in line_string:
        perc_morte = (line_string).replace("Percentual Mortalidade", "").replace(".", "").replace(":", "")
        perc_morte = remove_empty_spaces(perc_morte.split(" "))[0]
       
        perc_morte_arr.append({
            "id": id_l,
            "Data": perc_morte
        })

    # "Total de Mortes"
    if "Total de Mortes" in line_string:
        total_morte = (line_string).replace("Total de Mortes", "").replace(".", "").replace(":", "")
        total_morte = remove_empty_spaces(total_morte.split(" "))[0]
       
        total_morte_arr.append({
            "id": id_l,
            "Data": total_morte
        })
        
   # "Morto na Propriedade"
    if "Morto na Propriedade" in line_string:
        mort_propr = (line_string).replace("Morto na Propriedade", "").replace(".", "").replace(":", "")
        mort_propr = remove_empty_spaces(mort_propr.split(" "))[0]
       
        mort_propr_arr.append({
            "id": id_l,
            "Data": mort_propr
        })

    # "Qtde Eliminado/Condenado"
    if "Qtde Eliminado/Condenado" in line_string:
        qtd_elim_cond = (line_string).replace("Qtde Eliminado/Condenado", "").replace(".", "").replace(":", "")
        qtd_elim_cond = remove_empty_spaces(qtd_elim_cond.split(" "))[0]
       
        qtd_elim_cond_arr.append({
            "id": id_l,
            "Data": qtd_elim_cond
        })

    # "Qtde Mortes Romaneio"
    if "Qtde Mortes Romaneio" in line_string:
        qtd_mort_rom = (line_string).replace("Qtde Mortes Romaneio", "").replace(".", "").replace(":", "")
        qtd_mort_rom = remove_empty_spaces(qtd_mort_rom.split(" "))[0]
       
        qtd_mort_rom_arr.append({
            "id": id_l,
            "Data": qtd_mort_rom
        })

    # "Qtde Leitões Entrada"
    if "Qtde Leitões Entrada" in line_string:
        qtd_leit_ent = (line_string).replace("Qtde Leitões Entrada", "").replace(".", "").replace(":", "")
        qtd_leit_ent = remove_empty_spaces(qtd_leit_ent.split(" "))[-1]
       
        qtd_leit_ent_arr.append({
            "id": id_l,
            "Data": qtd_leit_ent
        })

    # "Qtde Leitões Saída"
    if "Qtde Leitões Saída" in line_string:
        qtd_leit_said = (line_string).replace("Qtde Leitões Saída", "").replace(".", "").replace(":", "")
        qtd_leit_said = remove_empty_spaces(qtd_leit_said.split(" "))[-1]
       
        qtd_leit_said_arr.append({
            "id": id_l,
            "Data": qtd_leit_said
        })

    # "Diferença"
    if "Diferença" in line_string:
        diferenca = (line_string).replace("Diferença", "").replace(".", "").replace(":", "")
        diferenca = remove_empty_spaces(diferenca.split(" "))[-1]
       
        diferenca_arr.append({
            "id": id_l,
            "Data": diferenca
        })

    # "Peso Leitões Entrada"
    if "Peso Leitões Entrada" in line_string:
        pes_leit_ent = (line_string).replace("Peso Leitões Entrada", "").replace(".", "").replace(":", "")
        pes_leit_ent = remove_empty_spaces(pes_leit_ent.split(" "))[-1]

        pes_leit_ent_arr.append({
            "id": id_l,
            "Data": pes_leit_ent
        })

    # "Peso Saída"
    if "Peso Saída" in line_string:
        pes_saida = (line_string).replace("Peso Saída", "").replace(".", "").replace(":", "")
        pes_saida = remove_empty_spaces(pes_saida.split(" "))[-1]
       
        pes_saida_arr.append({
            "id": id_l,
            "Data": pes_saida
        })

    # "Peso Médio Leitão Entrada"
    if "Peso Médio Leitão Entrada" in line_string:
        pes_med_leit_ent = (line_string).replace("Peso Médio Leitão Entrada", "").replace(".", "").replace(":", "")
        pes_med_leit_ent = remove_empty_spaces(pes_med_leit_ent.split(" "))[-1]
       
        pes_med_leit_ent_arr.append({
            "id": id_l,
            "Data": pes_med_leit_ent
        })

    # "Peso Médio Saída"
    if "Peso Médio Saída" in line_string:
        pes_med_saida = (line_string).replace("Peso Médio Saída", "").replace(".", "").replace(":", "")
        pes_med_saida = remove_empty_spaces(pes_med_saida.split(" "))[-1]
       
        pes_med_saida_arr.append({
            "id": id_l,
            "Data": pes_med_saida
        })        
       
    # "Ganho de Peso" 
    if "Ganho de Peso" in line_string:
        ganho_peso = (line_string).replace("Ganho de Peso", "").replace(".", "").replace(":", "")
        ganho_peso = remove_empty_spaces(ganho_peso.split(" "))[-1]
       
        ganho_peso_arr.append({
            "id": id_l,
            "Data": ganho_peso
        }) 

    # "GPMD Ponderado"
    if "GPMD Ponderado" in line_string:
        gpmd_pond = (line_string).replace("GPMD Ponderado", "").replace(".", "").replace(":", "")
        gpmd_pond = remove_empty_spaces(gpmd_pond.split(" "))[-1]
       
        gpmd_pond_arr.append({
            "id": id_l,
            "Data": gpmd_pond
        })

    # "Conversão Alimentar Real"
    if "Conversão Alimentar Real" in line_string:
        conv_alim_real = (line_string).replace("Conversão Alimentar Real", "").replace(".", "").replace(":", "")
        conv_alim_real = remove_empty_spaces(conv_alim_real.split(" "))[-1]
       
        conv_alim_real_arr.append({
            "id": id_l,
            "Data": conv_alim_real
        }) 

    # "Conv. Alimentar Ajustada"
    if "Conv. Alimentar Ajustada" in line_string:
        conv_alim_real_ajst = (line_string).replace("Conv. Alimentar Ajustada", "").replace(".", "").replace(":", "")
        conv_alim_real_ajst = remove_empty_spaces(conv_alim_real_ajst.split(" "))[-1]
       
        conv_alim_real_ajst_arr.append({
            "id": id_l,
            "Data": conv_alim_real_ajst
        })    
       
    # Conversão Alimentar
    if "Conversão Alimentar" in line_string:
        pdr = re.compile(r"^Conversão Alimentar\s+(\d+,\d+)$")
        conv_alim_real_ajst = "nan"
        if pdr.match(line_string):
            conv_alim_real_ajst = pdr.findall(line_string)[0]
            # print(conv_alim_real_ajst)
        
            conv_alim_arr.append({
                "id": id_l,
                "Data": conv_alim_real_ajst
            })
            
    if "Critério Conversão Alimentar" in line_string:
        crit_con_alm_s = line_string.replace("Critério Conversão Alimentar", "").strip()
        
        crit_conv_alim_arr.append({
            "id": id_l,
            "Data": crit_con_alm_s
        })
        
    # Critério Mortalidade
    if "Critério Mortalidade" in line_string:
        crit_mort_s = line_string.replace("Critério Mortalidade", "").strip()
        
        crit_mortalidade_arr.append({
            "id": id_l, 
            "Data": crit_mort_s
        })
        
    # "Critério Percentual de Peso Ideal" 
    if "Critério Percentual de Peso Ideal" in line_string:
        crit_percent_peso_real_s = line_string.replace("Critério Percentual de Peso Ideal", "").strip()
        
        crit_percent_peso_ideal_arr.append({
                "id": id_l,
                "Data": crit_percent_peso_real_s
            })
        
    # "Critério Percentual de Check List"
    if "Critério Percentual de Check List" in line_string:
        crit_percent_check_list = line_string.replace("Critério Percentual de Check List", "").strip()
        
        crit_percent_check_list_arr.append({
                "id": id_l,
                "Data": crit_percent_check_list
            })
    
    if "Critério GPMD"  in line_string:
        crit_gmpd  = line_string.replace("Critério GPMD", "").strip()
        
        crit_gmpd_arr.append({
                "id": id_l,
                "Data": crit_gmpd
            })
        
        
    if "Valor de Tabela por Animal"  in line_string:
        table_value_s =  line_string.replace("Valor de Tabela por Animal", "").strip()
        
        table_value_anual_arr.append({
                "id": id_l,
                "Data": table_value_s
            })
        
    if "Valor Desconto por Animal"  in line_string:
        value_desc_animal  = line_string.replace("Valor Desconto por Animal", "").strip()
        value_descont_animal_arr.append({
                "id": id_l,
                "Data": value_desc_animal
            })
        
        
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

##############################################################################
data_last_ulent_arr = processar_dicionarios(id_unic_arr, data_last_ulent_arr)
total_dia_aloj_arr = processar_dicionarios(id_unic_arr, total_dia_aloj_arr)
dt_aloj_pond_arr = processar_dicionarios(id_unic_arr, dt_aloj_pond_arr)
dt_saida_pond_arr = processar_dicionarios(id_unic_arr, dt_saida_pond_arr)
dia_aloj_pond_arr = processar_dicionarios(id_unic_arr, dia_aloj_pond_arr)
perc_morte_arr = processar_dicionarios(id_unic_arr, perc_morte_arr)
total_morte_arr = processar_dicionarios(id_unic_arr, total_morte_arr)
mort_propr_arr = processar_dicionarios(id_unic_arr, mort_propr_arr)
##############################################################################
qtd_mort_rom_arr = processar_dicionarios(id_unic_arr, qtd_mort_rom_arr)
qtd_leit_ent_arr = processar_dicionarios(id_unic_arr, qtd_leit_ent_arr)
qtd_leit_said_arr = processar_dicionarios(id_unic_arr, qtd_leit_said_arr)
diferenca_arr = processar_dicionarios(id_unic_arr, diferenca_arr)
pes_leit_ent_arr  = processar_dicionarios(id_unic_arr, pes_leit_ent_arr)
pes_saida_arr = processar_dicionarios(id_unic_arr, pes_saida_arr)
pes_med_leit_ent_arr = processar_dicionarios(id_unic_arr, pes_med_leit_ent_arr)
pes_med_saida_arr = processar_dicionarios(id_unic_arr, pes_med_saida_arr)
ganho_peso_arr = processar_dicionarios(id_unic_arr, ganho_peso_arr)
gpmd_pond_arr = processar_dicionarios(id_unic_arr, gpmd_pond_arr)
conv_alim_real_arr = processar_dicionarios(id_unic_arr, conv_alim_real_arr)
conv_alim_real_ajst_arr = processar_dicionarios(id_unic_arr, conv_alim_real_ajst_arr)
qtd_elim_cond_arr  = processar_dicionarios(id_unic_arr, qtd_elim_cond_arr)
conv_alim_arr = processar_dicionarios(id_unic_arr, conv_alim_arr)
crit_conv_alim_arr = processar_dicionarios(id_unic_arr, crit_conv_alim_arr)
crit_mortalidade_arr = processar_dicionarios(id_unic_arr, crit_mortalidade_arr)
crit_percent_peso_ideal_arr = processar_dicionarios(id_unic_arr, crit_percent_peso_ideal_arr)
crit_percent_check_list_arr = processar_dicionarios(id_unic_arr, crit_percent_check_list_arr)
crit_gmpd_arr = processar_dicionarios(id_unic_arr, crit_gmpd_arr)
table_value_anual_arr =  processar_dicionarios(id_unic_arr, table_value_anual_arr)
value_descont_animal_arr  =  processar_dicionarios(id_unic_arr, value_descont_animal_arr)

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

##################################################################
new_dataFrame["DATA_ULTIMA_ENTREGA"] = data_last_ulent_arr
new_dataFrame["TOTAL_DIAS_ALOJAMENTO"] = total_dia_aloj_arr
new_dataFrame["DATA_ALOJAMENTO_PONDERADA"] = dt_aloj_pond_arr
new_dataFrame["DATA_SAIDA_PONDERADA"] = dt_saida_pond_arr
new_dataFrame["DIAS_ALOJAMENTO_PONDERADA"] = dia_aloj_pond_arr
new_dataFrame["PERCENTUAL_MORTALIDADE"] = perc_morte_arr
new_dataFrame["TOTAL_MORTE"] = total_morte_arr
new_dataFrame["MORTE_PROPRIEDADE"] = mort_propr_arr
####################################################################


new_dataFrame["QTDA_ELIMINADO_CONDENADO"] = qtd_elim_cond_arr
new_dataFrame["QTDA_MORTES_ROMANEIO"] = qtd_elim_cond_arr
new_dataFrame["QTDA_LEITOES_ENTRADA"] = qtd_leit_ent_arr
new_dataFrame["QTDA_LEITOES_SAIDA"] = qtd_leit_said_arr
new_dataFrame["DIFERENCA"] = diferenca_arr
new_dataFrame["PESO_LEITOES_ENTRADA"] = pes_leit_ent_arr
new_dataFrame["PESO_LEITOES_SAIDA"] = pes_saida_arr
new_dataFrame["PM_LEITOES_ENTRADA"] = pes_med_leit_ent_arr
new_dataFrame["PM_SAIDA"] = pes_med_saida_arr
new_dataFrame["GANHO_PESO"] = ganho_peso_arr
new_dataFrame["GPMD_PONDERADO"] = gpmd_pond_arr
new_dataFrame["CONV_ALIMENTAR_REAL"] = conv_alim_real_arr
new_dataFrame["CONV_ALIMENTAR_REAL_AJST"] = conv_alim_real_ajst_arr
new_dataFrame["CONV_ALIMENTAR"] = conv_alim_arr
new_dataFrame["CRITERIO_CONV_ALIMENTAR"] = crit_conv_alim_arr
new_dataFrame["CRITERIO_MORTALIDADE"] = crit_mortalidade_arr
new_dataFrame["CRITERIO_PERCENTUAL_PESO_IDEAL"] = crit_percent_peso_ideal_arr
new_dataFrame["CRITERIO_PERCENTUAL_CHECK_LIST"] = crit_percent_check_list_arr
new_dataFrame["CRITERIO_GMPD"] = crit_gmpd_arr
new_dataFrame["VALOR_TABELA_POR_ANIMAL"] = table_value_anual_arr
new_dataFrame["VALOR_DESCONTO_POR_ANIMAL"] = value_descont_animal_arr


print("Salvando arquivo...")
new_dataFrame.to_csv(rf"Colunas_Criadas_CSV/alfa_tabela_{get_date_now()}.csv",  index=False)
input("Arquivo salvo com sucesso...")