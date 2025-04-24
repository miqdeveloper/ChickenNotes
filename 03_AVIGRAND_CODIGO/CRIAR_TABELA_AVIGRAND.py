from genericpath import exists
import glob, datetime, os
from datetime import datetime
from math import nan
from operator import index
import pandas as pd

from tqdm import tqdm
import re

from warnings import simplefilter

simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

def get_date_now():
    date_now = datetime.now()
    d = str(date_now.strftime("""_%d_%m_%Y"""))
    return d

global key_arr
global arr_tmp 

# 'ArquivosCSV/file_csv.csv'
arr_filter = ["Integrado", 
              "Pedido", 
              "Município", 
              "Data Alojamento", 
              "Linhagem", 
              "Qtde Alojada",
              "Peso Méd Pintinho", 
              "Área Alojada", 
              "Data Abate", 
              "Categoria",
              "Técnico",
              "Telefone",
              "E-mail",
              "Tipo Ventilação",
              "Kg/m2",
              "Material",
              "Aves/m2",
              "Qtde Abatida",
              "Mort. Tota",
              "Qtde Mortos",
              "Qtde Eliminados",
              "Idade Abate",
              "Aves Faltantes",
              "Peso Médio",
              "GPD",
              "Peso Total",
              "CAAF",
              "Ração Consumida",
              "Valor do Frango Vivo por Kg em R$",
              "Instalações No",
              "Valor da Ração por Kg em R$",
              "Valor do Pinto em R$",
              "Percentual Basico",
              "Aj Escala Produção",
              "Aj Sazonalidade",
              "Aj Sexo e Peso",
              "Aj Idade",
              "Aj Mortalidade",
              "Aj Conv Alimentar",
              "Aj Meritocracia",
              "Aj Calo Pata A",
              "Aj Condenações",
              "Aj Qualidade",
              "Aj Estrutural",
              "Aj Procedimento",
              "Aj Processos/Procedimentos (PP)",
              "Resultado Lote",
              "Renda Bruta / Ave",
              "Imposto FUNRURAL",
              "SENAR",
              "Conta Corrente Produtor",
              "Conta Corrente Vinculada",
              "Conversão Alimentar",
              "Idade de Abate",
              "Mortalidade",
              "% Calo de Pata A",
              "% Arranhaduras",
              "% Papo Cheio", 
              "% Condenação",
              "Centro"
            ]

# print("Qtd. Filtros Disponíveis: ", len(arr_filter))
# print("LCFD: ", arr_filter.index("Peso Médio"))

file_execel = r'Arquivos_Extraidos_CSV/dados_extraidos_avigrand.csv'
# file_execel = 'ArquivosCSV/file_csv.csv'

dir_s = ["Colunas_Criadas_CSV"]


def search_term(name, lines_n):
    
    int(lines_n)   
    
    # "TABELA_1.xlsx"
    df = pd.read_excel(file_execel)

    lines, col =df.shape 
    filter_arr = []

    for line in tqdm(range(lines), desc="Processando Linhas"):
        line_ = df.iloc[line].values
        if name in line_:            
            new_df = df.iloc[line+1: (line+lines_n)]
            new_df = filter_arr.append(pd.DataFrame(new_df))

    new_df = pd.concat(filter_arr, ignore_index=True)
    new_df.to_excel("filter_term.xlsx")
    
    print("Filtragem Completa")
    print("Arquivo filter_term.xlsx salvo!"+"\n")

def create_table_csv(termos_list):
    filter_arr_b = []
        
    # termos_list = []
    

    df = pd.read_csv(file_execel)
    lines, col = df.shape 
    
    for line in tqdm(range(lines), desc="Processando Linhas"):
        for palavra_  in termos_list:
            line_ = df.iloc[line].values
            # print(line_[0])
            line_ = str(line_[0])
            
            if palavra_ in line_:
                filter_arr_b.append(line_)
            
    df = pd.DataFrame(filter_arr_b)    
    df.to_csv("ArquivosCSV/extract_terms_table.csv", mode="w", index=False)

def ler_lista(dados):
    contas_correntes = {}
    for item in dados:
        conta_principal, conta_secundaria = item[0].split('-')
        descricao_valor = item[1]
        
        # Extraindo descrição e valor
        if 'R$' in descricao_valor:
            descricao, valor = descricao_valor.rsplit('R$', 1)
        else:
            descricao, valor = descricao_valor.rsplit(' ', 1)
        
        # Removendo possíveis caracteres adicionais do valor
        valor = valor.replace(',', '.').strip()
        
        # Convertendo valor para float
        valor = float(valor)
        
        # Atualizando o valor no dicionário
        if conta_principal not in contas_correntes:
            contas_correntes[conta_principal] = {}
        
        contas_correntes[conta_principal][conta_secundaria] = (descricao, valor)
    
    return

def separate_():

    key_arr = []
    name_arr = []
    clifor_arr = []
    arr_municipio = []
    tecnico_arr = []
    telefone_arr = []
    
    email_arr = []
    t_vent_arr = []
    arr_categoria = []
    arr_linhagem =  []
    kgm2_arr = []
    material_arr = []
    ave_m2_arr = []
    
    qabate_arr = []
    mort_total_arr = []
    quant_mortes_arr = []
    quant_eliminados_arr = []
    idade_abate_arr = []
    aves_faltantes_arr = []
    peso_medio_f_arr = []
    gpd_arr = []
    peso_total_arr = []
    caaf_arr = []
    racao_c_arr = []
    valor_kg_f_arr = []
    aviario_arr = []
    valor_kg_racao_arr = []
    valor_pinto_real_arr = []
    percentual_basico_arr = []
    carne_base_arr = []
    real_base_arr = []
    arr_pedido = []
    
    
    arr_data_aloj = []
    
    arr_quant_alojado = []
    arr_peso_medio = []
    arr_area_aloj = []
    arr_data_abate = []
    
    aj_porcent_arr = []
    aj_kg_arr = []
    aj_real_arr = []

    aj_sazonalidade_percent_arr = []
    aj_sazonalidade_kg_arr = []
    aj_sazonalidade_real_arr = []
    
    aj_sex_pes_percent_arr = []
    aj_sex_pes_kg_arr = []
    aj_sex_pes_real_arr = []
    
        
    aj_idade_percent_arr = []
    aj_idade_kg_arr = []
    aj_idade_real_arr = []
    
    aj_mortalidade_percent_arr = []
    aj_mortalidade_kg_arr = []
    aj_mortalidade_real_arr = []
    
    aj_conv_alimentar_percent_arr = []
    aj_conv_alimentar_kg_arr = []
    aj_conv_alimentar_real_arr = []
    
    aj_meritocracia_mt_percent_arr = []
    aj_meritocracia_mt_kg_arr = []
    aj_meritocracia_mt_real_arr = []
    
    aj_calo_pata_a_percent_arr = []
    aj_calo_pata_a_kg_arr = []
    aj_calo_pata_a_real_arr = []
    
    condenacoes_percent_arr = []
    condenacoes_kg_arr = []
    codenacoes_real_arr = []
    
    aj_qualidade_percent_arr = []
    aj_qualidade_kg_arr = []
    aj_qualidade_real_arr = []
    
    aj_estrutural_percent_arr = []
    aj_estrutural_kg_arr = []
    aj_estrutural_real_arr = []
    
    aj_procedimentos_percent_arr = []
    aj_procedimentos_kg_arr = []
    aj_procedimentos_real_arr = []
    
    aj_procedimentos_percent_arr = []
    aj_procedimentos_kg_arr = []
    aj_procedimentos_real_arr = []
    
    aj_processos_procedimentos_pp_percent_arr = []
    aj_processos_procedimentos_pp_kg_arr = []
    aj_processos_procedimentos_pp_real_arr = []
    
    resultado_lote_percent_arr = []
    resultado_lote_kg_arr = []
    resultado_lote_real_arr = []
    
    ave_real_arr = []
    ton_real_arr = []
    m2_real_arr = []
    
    funrural_arr = []
    senar_arr = []
    
    conta_corrente_arr = []
    conta_vinculada = []
    
    conv_aliment_real_arr= []
    conv_aliment_real_aj_arr = []
    conv_aliment_prev_aj_arr = []
    conv_aliment_diferenca_arr = []

    idade_de_abate_real_arr = []
    idade_de_abate_real_prev_aj_arr = []
    idade_de_abate_real_dif_arr = []

    peso_medio_real_arr = []
    peso_medio_prevaj_arr = []
    peso_medio_diferenca_arr = []

    mortalidade_real_arr = []
    mortalidade_real_aj_arr = []
    mortalidade_prev = []
    mortalidade_diferenca = []

    percent_calo_real_arr = []
    percent_calo_prev_arr = []
    percent_calo_dife_arr = []

    percent_arranhaduras_real_arr = []
    percent_arranhaduras_prevaj_arr = []
    percent_arranhaduras_diferenca_arr = []

    percent_papo_cheio_real_arr = []
    percent_papo_cheio_prev_arr = []
    percent_papo_cheio_diferenca_arr = []

    percent_codenacao_real_arr = []
    percent_codenacao_prev_arr = []
    percent_codenacao_diferenca_arr = []


    mod = []
    mod_2 = []
    
    id_uni = []
    
    arr_tmp =[]
    arr_tmp_1 =[]
    
    arr_tmp_2 = []

    centro_arr = []
    
    df = pd.read_csv(file_execel)    
    new_dataFrame= pd.DataFrame()
    
    def create_dirs(dirs):
        """Create directories if they don't exist."""
        for dir_ in dirs:
            if not os.path.exists(dir_):
                os.mkdir(dir_)

    def find_dates(text):
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
    
    def remove_chars(input_str):
        chars_to_remove = ["[", "\"", "'", "nan", "]", ":"]
        for char in chars_to_remove:
            input_str = input_str.replace(char, "")
        return input_str
    
    def converter_para_float(numero_str):
        numero_str = numero_str.replace(',', '.')
        return float(numero_str)

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
                f_l = {'Data': "nan", 'id': id_unico}
            else:
                f_l = arr_temp[arr_id.index(id_unico)]
            arr_data_final.append(f_l)
        # Extraindo apenas os valores da chave 'Data'
        arr_data_final = [data['Data'] for data in arr_data_final]
        return arr_data_final

     #ITERA SOBRE O NOVO DATA FRAME FILTRADO
    
    create_dirs(dir_s)
    for index, row in df.iterrows():
        line_item = str(row.iloc[0])

        
        new_str = re.sub(r"\[|\]", "", line_item)
                
        new_str = remove_chars(new_str)

        for item in arr_filter:
            if(item in new_str):
                    #get_integrado e atributo
                if arr_filter[0] == item:
                    separate_str = remove_empty_spaces(new_str.split(","))                    
                    # get clifor
                    chave = separate_str[0]
                    key_arr.append(chave)
                    

                    clifor_ = find_numbers(separate_str[0])
                    clifor_arr.append(clifor_[0])
                    
                    # # get name integrado
                    name_integrado = find_letters(separate_str[-1].replace("Integrado ", ""))
                    if name_integrado:
                        name_arr.append(name_integrado[0])
                        
                # Get_PEDIDO/LOTE                
                if arr_filter[1] == item:
                    get_pedido = remove_empty_spaces(new_str.split(","))
                    
                    n_pedido =find_numbers(get_pedido[1])
                    if (find_numbers(get_pedido[1])):
                        get_p = get_pedido[1].replace("Pedido ", "")
                        
                        lote_ = remove_empty_spaces(get_p.split(" "))
                        lote_ = lote_[0].replace("Vlr", "")                        
                        if lote_:
                            mod.append(lote_)
                            id_uni.append(get_pedido[0])
                            
                arr_pedido = list(dict.fromkeys(mod))
                id_uni = list(dict.fromkeys(id_uni))
            
                # Get_municipio
                if  arr_filter[2] == item:
                    separate_mun = new_str.split(",")
                    municipio = (separate_mun[1].replace("Município ", ""))
                    arr_municipio.append(municipio)
                    
                # GET_DATA_ALOJAMENTO
                if arr_filter[3] == item:
                    separate_data_aloj = new_str.split(",")
                    data_alojamento = separate_data_aloj[1].replace("Data Alojamento ", "")
                    arr_data_aloj.append(data_alojamento)
                
                # GET_linhagem 
                if arr_filter[4] == item:
                    separate_linhagem = new_str.split(",")
                    linhagem = remove_empty_spaces(separate_linhagem[1].split("Linhagem "))

                    arr_linhagem.append(linhagem[0])
                
                # GET_quantidade_alojada                    
                if arr_filter[5] == item:
                    separate_quat_aloj = new_str.split(",")
                    quant_aloj = (remove_empty_spaces(separate_quat_aloj))
                    quant_aloj = quant_aloj[-1]
                    quant_aloj = quant_aloj.replace("Qtde Alojada ", "")

                    arr_quant_alojado.append(quant_aloj)
                
                #get_peso_medio_pinto
                if arr_filter[6] == item:                    
                    separate_peso_med_p = new_str.split(", ")
                    peso_med_p = separate_peso_med_p[1]
                    peso_med_p = peso_med_p.replace("Peso Méd Pintinho", "")
                    arr_peso_medio.append(peso_med_p)
                
                # get_area_aloj
                if arr_filter[7] == item:
                    separate_area_aloj = new_str.split(",")
                    area_aloj = separate_area_aloj[1].replace("Área Alojada", "")
                    arr_area_aloj.append(area_aloj)

                # get DATA_ABATE
                if arr_filter[8] == item:
                    new_str = new_str.split(",", )
                    if find_dates(new_str[1]):
                        data_abate = new_str[1].replace("Data Abate ", "")
                        arr_data_abate.append(data_abate)
                    
                #Get_categoria
                if arr_filter[9] == item:
                    separate_categoria = new_str.split(", ")
                    categoria_s = remove_empty_spaces(separate_categoria)
                    categoria_ = categoria_s[-1]
                    categoria_ = categoria_.replace("Categoria ", "").replace("Categoria", "")
                    if not categoria_:
                        categoria_ = nan
                        

                    arr_categoria.append(categoria_)
                    
                # GET_TECNICO
                if arr_filter[10] == item:
                    separate_tcnico = new_str.split(", ")
                    separate_tcnico = remove_empty_spaces(separate_tcnico)
                    name_tcnico = separate_tcnico[-1].replace("Técnico ", "")
                    tecnico_arr.append(name_tcnico)
                    
                # GET_TELEFONE
                if arr_filter[11] == item:
                    separate_tel = new_str.split(", ")
                    separate_tel = remove_empty_spaces(separate_tel)
                    telefone = separate_tel[-1].replace("Telefone", "").replace("Telefone ", "")
                    if not telefone:
                        telefone = "nan"                        
                    telefone_arr.append(telefone)
                    
                # GET_EMAIL
                if arr_filter[12] == item:
                    separate_email = new_str.split(", ")
                    separate_email = remove_empty_spaces(separate_email)
                    email = separate_email[-1].replace("E-mail ", "").replace("E-mail", "")
                    if not email:
                        email = "nan"                
                    email_arr.append(email)
                    
                # GET_TIPO_DE_VENTILACAO
                if arr_filter[13] == item:
                    separate_t_vent = new_str.split(", ")
                    separate_t_vent = remove_empty_spaces(separate_t_vent)
                    t_vent = separate_t_vent[-1].replace("Tipo Ventilação ", "").replace("Tipo Ventilação", "")
                    t_vent_arr.append(t_vent)
                    
                # GET_Kg/m2                
                if arr_filter[14] == item:
                    separate_kg_m2 = new_str.split(", ")
                    separate_kg_m2 = remove_empty_spaces(separate_kg_m2)
                    separate_kg_m2 = separate_kg_m2[-1].replace("Kg/m2", "").replace("Kg/m2 ", "")
                    if not separate_kg_m2:
                        separate_kg_m2 = "nan"                        
                    kgm2_arr.append(separate_kg_m2)
                    
                # GET_MATERIAL
                if arr_filter[15] == item:
                    separate_material = new_str.split(", ")
                    separate_material = remove_empty_spaces(separate_material)
                    get_m = separate_material[1].split(" ")
                    if item in get_m[0]:
                        material = separate_material[1].replace("Material ", "")
                        material_arr.append(material)
                # GET_ave/m2
                if arr_filter[16] == item:
                    separate_avem2 = new_str.split(", ")
                    separate_avem2 = remove_empty_spaces(separate_avem2)
                    get_avem2 = separate_avem2[-1].replace("Aves/m2", "").replace("Aves/m2 ", "").replace(" ", "")
                    ave_m2_arr.append(get_avem2)
                # GET_QUANT_ABATIDA
                if arr_filter[17] == item:
                    separate_qabate = new_str.split(", ")
                    qabates_ = remove_empty_spaces(separate_qabate)
                    qabate_arr.append(qabates_[-1].replace("Qtde Abatida ", ""))
                
                # GET_MORTE_TOTAL
                if arr_filter[18] == item:
                    separate_mtotal = new_str.split(", ")
                    separate_mtotal = remove_empty_spaces(separate_mtotal)
                    m_total = separate_mtotal[-1].replace("Mort. Total ", "").replace("Mort. Total", "")
                    mort_total_arr.append(m_total)
                
                # GET_quant_mortes_arr
                if arr_filter[19] == item:
                    separate_qmortes = new_str.split(", ")
                    separate_qmortes = remove_empty_spaces(separate_qmortes)
                    separate_qmortes = separate_qmortes[-1].replace("Qtde Mortos ", "").replace("Qtde Mortos", "")
                    quant_mortes_arr.append(separate_qmortes)
                    
                # GET_Qtde Eliminados 
                if arr_filter[20] == item:
                    separate_qmt_eliminados = new_str.split(", ")
                    separate_qmt_eliminados = remove_empty_spaces(separate_qmt_eliminados)
                    separate_qmt_eliminados = separate_qmt_eliminados[-1].replace("Qtde Eliminados ", "").replace("Qtde Eliminados", "")
                    if not separate_qmt_eliminados:
                        separate_qmt_eliminados = nan
                    quant_eliminados_arr.append(separate_qmt_eliminados)
                    
                # GET_Idade Abate 
                if arr_filter[21] == item:
                    separate_idade_abate = new_str.split(", ")
                    separate_idade_abate = remove_empty_spaces(separate_idade_abate)
                    separate_idade_abate = separate_idade_abate[1].replace("Idade Abate ", "").replace("Idade Abate", "")
                    if not separate_idade_abate:
                        separate_idade_abate = nan
                        
                    idade_abate_arr.append(separate_idade_abate)
                
                #GET_Aves Faltantes
                if arr_filter[22] == item:
                    separate_aves_falt = new_str.split(", ")
                    separate_aves_falt = remove_empty_spaces(separate_aves_falt)
                    separate_aves_falt = separate_aves_falt[-1].replace("Aves Faltantes ", "").replace("Aves Faltantes", "")
                    if not separate_aves_falt:
                        separate_aves_falt = nan
                    aves_faltantes_arr.append(separate_aves_falt)
                    
                #GET_Peso Médio

                if arr_filter[23] == item:
                    separate_peso_m_f_ = None
                    
                    separate_peso_m_f = new_str.split(", ")

                    separate_peso_m_f = remove_empty_spaces(separate_peso_m_f)
                    # print(separate_peso_m_f)
                    
                    
                    if len(separate_peso_m_f) == 3:

                        if "GPD" in str(separate_peso_m_f):

                            
                            separate_peso_m_f_ = separate_peso_m_f[1]
                            separate_peso_m_f_ = separate_peso_m_f_.replace("Peso Médio ", "").replace("Peso Médio", "")
                            
                            if not separate_peso_m_f_:
                                separate_peso_m_f_ = "nan"
                               
                            peso_medio_f_arr.append(separate_peso_m_f_)

                        # PENSE NUM BAGULHO DOIDO EM
                        if "GPD" not in str(separate_peso_m_f):
                            # real - PESO MEDIO
                            peso_med_r =  separate_peso_m_f[1].replace("Peso Médio","").replace(" ", "")
                            
                            peso_medio_real_arr.append(peso_med_r)

                            # prevaj - PESO MEDIO
                            peso_medio_prevaj_arr.append(separate_peso_m_f[-1].split(" ")[0])

                            # diferenca - PESO MEDIO
                            peso_medio_diferenca_arr.append(separate_peso_m_f[-1].split(" ")[1])


                    if len(separate_peso_m_f) == 4:

                        # real
                        # arr_tmp.append(separate_peso_m_f[0])

                        peso_medio_real = (separate_peso_m_f[1].replace("Peso Médio",""))

                        if not peso_medio_real:
                            peso_medio_real = "nan"
                        peso_medio_real_arr.append(peso_medio_real)

                        # prevaj
                        peso_medio_prevaj = separate_peso_m_f[2]

                        if not peso_medio_prevaj:
                            peso_medio_prevaj = "nan"

                        peso_medio_prevaj_arr.append(peso_medio_prevaj)

                        # diferenca
                        peso_medio_diferenca = separate_peso_m_f[3]
                        
                        if not peso_medio_diferenca:
                            peso_medio_diferenca = "nan"

                        peso_medio_diferenca_arr.append(peso_medio_diferenca)

                    if len(separate_peso_m_f) == 5:
                        print(separate_peso_m_f)
                    
                # GET_GPD
                if arr_filter[24] == item:
                    separate_gpd = new_str.split(", ")
                    separate_gpd = remove_empty_spaces(separate_gpd)
                    gpd_ = separate_gpd[-1].replace("GPD ", "").replace("GPD", "")
                    if not gpd_:
                        gpd_ = nan
                    gpd_arr.append(gpd_)
                    
                # GET_peso total
                if arr_filter[25] == item:
                    separate_p_total = new_str.split(", ")
                    separate_p_total = remove_empty_spaces(separate_p_total)
                    separate_p_total = separate_p_total[1].replace("Peso Total", "").replace("Peso Total ", "")
                    if not separate_p_total:
                        separate_p_total = nan
                    peso_total_arr.append(separate_p_total)
                
                # GET_CAAF
                if arr_filter[26] == item:
                    separate_caaf = new_str.split(", ")
                    separate_caaf = remove_empty_spaces(separate_caaf)
                    if (len(separate_caaf)) > 2:
                        caaf = separate_caaf[-1].replace("CAAF ", "").replace("CAAF", "")
                        if not caaf:
                            caaf = nan
                        caaf_arr.append(caaf)
                
                # GET_Ração Consumida
                if arr_filter[27] == item:
                    separate_racao_c = new_str.split(", ")
                    separate_racao_c = remove_empty_spaces(separate_racao_c)
                    racao_c = separate_racao_c[-1].replace("Ração Consumida ", "").replace("Ração Consumida", "")
                    if not racao_c:
                        racao_c = nan
                    racao_c_arr.append(racao_c)
                
                # GET_Valor do Frango Vivo por Kg em R$
                if arr_filter[28] == item:
                    separate_valor_f = new_str.split(", ")
                    separate_valor_f = remove_empty_spaces(separate_valor_f)
                    if len(separate_valor_f) == 2:
                        valor_f = (separate_valor_f[-1].replace("Valor do Frango Vivo por Kg em R$ ", "").replace("Valor do Frango Vivo por Kg em R$", ""))
                    if (len(separate_valor_f)) > 2:
                        valor_f = separate_valor_f[-1]
                    valor_f = converter_para_float(valor_f)
                    valor_kg_f_arr.append(valor_f)
                    
                
                # GET_aviario_instalacoes
                if arr_filter[29] == item:
                    separate_av = new_str.split(", ")
                    separate_av = remove_empty_spaces(separate_av)
                    f = list(find_numbers(separate_av[2]))
                    inst_ = separate_av[1].replace("Instalações No ", "").replace("Instalações No", "")
                    if not f:
                        f = ""
                        inst_ = inst_
                    if f:
                        f=f[0]
                        inst_ = inst_ +","+f
                        
                    aviario_arr.append(inst_)
                
                # GET_Valor da Ração por Kg em R$:
                if arr_filter[30] == item:
                    separate_kg_raca=new_str.split(", ")
                    separate_kg_raca = remove_empty_spaces(separate_kg_raca)
                    if len(separate_kg_raca) == 2:
                        val_kg = separate_kg_raca[-1].replace("Valor da Ração por Kg em R$ ", "").replace("Valor da Ração por Kg em R$", "")
                    if len(separate_kg_raca) > 2:
                        val_kg = separate_kg_raca[-1]
                    val_kg = converter_para_float(val_kg)
                    valor_kg_racao_arr.append(val_kg)
                    
                # GET_VALOR_REAL_pinto
                if arr_filter[31] == item:
                    separate_pinto=new_str.split(", ")
                    separate_pinto=remove_empty_spaces(separate_pinto)
                    if len(separate_pinto) > 2:
                        val_pinto= converter_para_float(separate_pinto[-1])
                    elif len(separate_pinto) == 2:
                        val_pinto = converter_para_float(separate_pinto[-1].replace("Valor do Pinto em R$ ", "").replace("Valor do Pinto em R$", ""))
                    
                    valor_pinto_real_arr.append(val_pinto)
                
                # GET_percentual_basico AND Kg
                if arr_filter[32] == item:                    
                    separate_percentual=new_str.split(", ")
                    separate_percentual = remove_empty_spaces(separate_percentual)
                    
                    percent_basic = separate_percentual[1].replace("Percentual Basico ", "").replace("Percentual Basico", "")
                    porcetage_percent = separate_percentual[2]
                    mod_2.append(separate_percentual)
                    
                    if not percent_basic:
                       percent_basic = separate_percentual[2].split(" ")
                       percent_basic = percent_basic[0]
                   
                    percent_basic = converter_para_float(percent_basic)
                  
                    percentual_basico_arr.append(percent_basic)
                
                # GET_AJ_%_Kg_R$
                if arr_filter[33] == item:
                    aj_separate = new_str.split(", ")
                    aj_separate = remove_empty_spaces(aj_separate)
                    
                    # aj_porcent_arr.append(aj_separate)
                    
                    if len(aj_separate) == 3:
                        
                        # %_percent_aj
                        aj_percent = aj_separate[1]
                        percent_aj = aj_percent.replace("Aj Escala Produção ", "").replace("Aj Escala Produção", "")

                        # aj_kg
                        kg_aj_separate = aj_separate[-1].split(" ")
                                               
                        if len(kg_aj_separate) ==2:
                            kg_aj_final = kg_aj_separate[0]
                        if len(kg_aj_separate) == 3:
                            kg_aj_final = kg_aj_separate[1]
                        
                        # aj_R$
                        aj_real_separate = aj_separate[-1].split(" ")
                        aj_real_final = aj_real_separate[-1]
                        
                        
                        if not percent_aj:
                            percent_aj = aj_separate[-1].split(" ")
                            percent_aj = percent_aj[0]
                        
                        percent_aj = converter_para_float(percent_aj)
                        aj_porcent_arr.append(percent_aj)
                        
                        kg_aj_final = converter_para_float(kg_aj_final)
                        aj_kg_arr.append(kg_aj_final)
                        
                        aj_real_arr.append(aj_real_final)

                    if len(aj_separate) == 4:
                        kg_aj_separate = aj_separate[-1].split(" ")
                        
                        kg_aj_final = kg_aj_separate[0]
                        aj_real_final = aj_separate[-1].split(" ")[-1]
                        aj_real_arr.append(aj_real_final)

                        aj_porcent_arr.append(aj_separate[2])
                        
                        aj_kg_arr.append(kg_aj_final)

                # GET_AJ_SAZONALIDADE_%_Kg_R$
                if arr_filter[34] == item:
                    aj_sazonalidade_separate = new_str.split(", ")
                    aj_sazonalidade_separate = remove_empty_spaces(aj_sazonalidade_separate)
                    n_c_ = len(aj_sazonalidade_separate)
                    
                    if n_c_ == 3:
                        
                        # GET_AJ_SAZONALIDADE_%
                        porcetage_aj_saz = (aj_sazonalidade_separate[1].replace("Aj Sazonalidade ","").replace("Aj Sazonalidade",""))
                        
                        # GET_AJ_SAZONALIDADE_KG
                        aj_saz_kg_separate = aj_sazonalidade_separate                        
                        aj_saz_kg_separate = aj_saz_kg_separate[-1].split(" ")
                        
                        # GET_AJ_SAZONALIDADE_R$
                        aj_saz_real_separate = aj_sazonalidade_separate
                        aj_saz_real_f = (aj_saz_real_separate[-1].split(" ")[-1])
                
                        if len(aj_saz_kg_separate) == 2:
                            aj_saz_kg_f = aj_saz_kg_separate[0]
            
                        if len(aj_saz_kg_separate) == 3:
                            aj_saz_kg_f = aj_saz_kg_separate[1] 

                        if not porcetage_aj_saz:
                            porcetage_aj_saz = aj_sazonalidade_separate[-1].split(" ")
                            porcetage_aj_saz = porcetage_aj_saz[0]
                            
                        if not aj_saz_kg_f:
                            aj_saz_kg_f =  nan
                            
                        if not aj_saz_real_f:
                            aj_saz_real_f =  nan
                            
                        # GET_AJ_SAZONALIDADE_R$
                        aj_sazonalidade_real_arr.append(aj_saz_real_f)
                        
                        aj_sazonalidade_kg_arr.append(aj_saz_kg_f)
                        
                        aj_sazonalidade_percent_arr.append(porcetage_aj_saz)
                        
                        
                    elif n_c_ == 4:
                        
                        # GET_AJ_SAZONALIDADE_%
                        porcetage_aj_saz = (aj_sazonalidade_separate[1].replace("Aj Sazonalidade ","").replace("Aj Sazonalidade",""))
                        
                        # GET_AJ_SAZONALIDADE_KG
                        aj_saz_kg_separate = aj_sazonalidade_separate
                        aj_saz_kg_f = aj_saz_kg_separate[2]
                        
                        # GET_AJ_SAZONALIDADE_R$
                        aj_saz_real_separate = aj_sazonalidade_separate
                        aj_saz_real_f = aj_saz_real_separate[-1]
                        
                        if " " in aj_saz_real_f:
                            aj_saz_real_f = aj_saz_real_f.split(" ")[-1]
                            
                        if not aj_saz_real_f:
                            aj_saz_real_f = nan
                            
                        if not aj_saz_kg_f:
                            aj_saz_kg_f = nan                        
                        
                        if not porcetage_aj_saz:
                            porcetage_aj_saz = aj_sazonalidade_separate[2]
                        
                        # GET_AJ_SAZONALIDADE_R$
                        aj_sazonalidade_real_arr.append(aj_saz_real_f)
                        
                        # GET_AJ_SAZONALIDADE_KG
                        aj_sazonalidade_kg_arr.append(aj_saz_kg_f)
                        
                        # GET_AJ_SAZONALIDADE_%
                        aj_sazonalidade_percent_arr.append(porcetage_aj_saz)
                        
                    if n_c_ == 5:
                        
                        # GET_AJ_SAZONALIDADE_%
                        aj_saz_perc = aj_sazonalidade_separate[2]
                        
                        
                        # GET_AJ_SAZONALIDADE_KG
                        aj_saz_kg = aj_sazonalidade_separate[3]
                        
                        
                        # GET_AJ_SAZONALIDADE_R$
                        aj_saz_real = aj_sazonalidade_separate[4]
                        
                        
                        aj_sazonalidade_real_arr.append(aj_saz_real)
                        
                        # GET_AJ_SAZONALIDADE_KG
                        aj_sazonalidade_kg_arr.append(aj_saz_kg)
                        
                        # GET_AJ_SAZONALIDADE_%
                        aj_sazonalidade_percent_arr.append(aj_saz_perc)
                        
                # GET_Aj Sexo e Peso
                if arr_filter[35] == item:
                    
                    aj_sex_pes_separate = new_str.split(", ")
                    aj_sex_pes_separate = remove_empty_spaces(aj_sex_pes_separate)
                    
                    n_c_ = len(aj_sex_pes_separate)
                    
                    if (n_c_ == 3):
                        
                        # GET_Aj Sexo e Peso_%                        
                        aj_sex_pes_f = aj_sex_pes_separate[1].replace("Aj Sexo e Peso ", "").replace("Aj Sexo e Peso", "")
                        if not aj_sex_pes_f:
                            aj_sex_pes_f = aj_sex_pes_separate[-1].split(" ")[0]
                            
                        aj_sex_pes_percent_arr.append(aj_sex_pes_f)
                        
                        # GET_Aj Sexo e Peso_kg
                        aj_sex_pes_kg_separate = aj_sex_pes_separate[-1]
                        
                        if " " in  aj_sex_pes_kg_separate:
                            
                            if len(aj_sex_pes_kg_separate.split(" ")) == 2:
                                aj_sex_pes_kg_f = aj_sex_pes_kg_separate.split(" ")[0]

                            if len(aj_sex_pes_kg_separate.split(" ")) == 3:
                                aj_sex_pes_kg_f = (aj_sex_pes_kg_separate.split(" ")[1])
                                
                        aj_sex_pes_kg_arr.append(aj_sex_pes_kg_f)
                        
                        
                        # GET_Aj Sexo e Peso_$
                        aj_sex_pes_real_separate = aj_sex_pes_separate[-1]
                
                        if " " in  aj_sex_pes_real_separate:
                            
                            if len(aj_sex_pes_real_separate.split(" ")) == 2:
                                aj_sex_pes_real_f = aj_sex_pes_real_separate.split(" ")[1]
                            if len(aj_sex_pes_real_separate.split(" ")) == 3:
                                aj_sex_pes_real_f = (aj_sex_pes_kg_separate.split(" ")[-1])
                            
                        aj_sex_pes_real_arr.append(aj_sex_pes_real_f)
                                                    
                    if (n_c_ == 4):
                        
                        # GET_Aj Sexo e Peso_%
                        aj_sex_pes_f = aj_sex_pes_separate[2]                                                    
                        aj_sex_pes_percent_arr.append(aj_sex_pes_f)
                        
                        # GET_Aj Sexo e Peso_kg
                        aj_sex_pes_kg_f =  aj_sex_pes_separate[2]
                        aj_sex_pes_kg_arr.append(aj_sex_pes_kg_f)
                        
                        # GET_Aj Sexo e Peso_$
                        aj_sex_pes_real_f =  aj_sex_pes_separate[3]
                  
                        aj_sex_pes_real_arr.append(aj_sex_pes_real_f)

                        
                    if n_c_ == 5:
                        
                        # GET_Aj Sexo e Peso_%
                        aj_sex_pes_f = aj_sex_pes_separate[2]
                        aj_sex_pes_percent_arr.append(aj_sex_pes_f)
                        
                        # GET_Aj Sexo e Peso_kg
                        aj_sex_pes_kg_f = aj_sex_pes_separate[3]  
                        aj_sex_pes_kg_arr.append(aj_sex_pes_kg_f)
                        
                        # GET_Aj Sexo e Peso_$
                        aj_sex_pes_real_f =  aj_sex_pes_separate[4]
                        aj_sex_pes_real_arr.append(aj_sex_pes_real_f)
                

                if arr_filter[36] == item:
                
                    aj_idade_separate = new_str.split(", ")
                    aj_idade_separate = remove_empty_spaces(aj_idade_separate)

                    n_c_ = len(aj_idade_separate)
                    
                    if n_c_ == 3:
                        
                        # GET_Aj Idade_%
                        aj_idade_percent_f = aj_idade_separate[1].replace("Aj Idade ", "").replace("Aj Idade", "")
                        
                        if not aj_idade_percent_f:
                            aj_idade_percent_f = aj_idade_separate[-1].split(" ")[0]                        
                        aj_idade_percent_arr.append(aj_idade_percent_f)
                        
                        # GET_Aj Idade_Kg
                        aj_idade_kg_f = aj_idade_separate[-1].split(" ")
                        
                        if len(aj_idade_kg_f) == 2:
                            aj_idade_kg_f = aj_idade_kg_f[0]
                            aj_idade_real_f = aj_idade_separate[-1].split(" ")[-1]

                        if len(aj_idade_kg_f) == 3:
                            aj_idade_kg_f = aj_idade_kg_f[1]
                            
                        aj_idade_kg_arr.append(aj_idade_kg_f)
                        
                        # GET_Aj Idade_$                        
                        aj_idade_real_separate = aj_idade_separate[-1]
                        n_c_ = len(aj_idade_real_separate.split(" "))

                        if n_c_  == 2:
                            aj_idade_real_f = aj_idade_real_separate.split(" ")[1]
                        if n_c_  == 3:
                            aj_idade_real_f = aj_idade_real_separate.split(" ")[-1]
                        aj_idade_real_arr.append(aj_idade_real_f)
                        
                    
                    if n_c_ == 4:
                        
                        # GET_Aj Idade_%
                        aj_idade_percent_f = aj_idade_separate[1].replace("Aj Idade ", "").replace("Aj Idade", "")
                        aj_idade_percent_arr.append(aj_idade_percent_f)
                        
                        # GET_Aj Idade_Kg
                        aj_idade_kg_f = aj_idade_separate
                        aj_idade_kg_f = aj_idade_kg_f[2]
                        aj_idade_kg_arr.append(aj_idade_kg_f)
                        
                        # GET_Aj Idade_$
                        aj_idade_real_f = aj_idade_separate[2]
                        aj_idade_real_arr.append(aj_idade_real_f)

                    
                    if n_c_ == 5:
                        
                        # GET_Aj Idade_%
                        aj_idade_percent_f = (aj_idade_separate[2])
                        aj_idade_percent_arr.append(aj_idade_percent_f)
                        
                        # GET_Aj Idade_Kg
                        aj_idade_kg_f = aj_idade_separate
                        aj_idade_kg_f= aj_idade_kg_f[3]
                        aj_idade_kg_arr.append(aj_idade_percent_f)
                        
                        # GET_Aj Idade_$
                        aj_idade_real_f = aj_idade_separate[-1]
                        aj_idade_real_arr.append(aj_idade_real_f)
                
                # GET_Aj_Mortalidade                
                if arr_filter[37] == item:
                    aj_mortalidade_separate = new_str.split(", ")
                    aj_mortalidade_separate = remove_empty_spaces(aj_mortalidade_separate)
                    
                    n_c_ = len(aj_mortalidade_separate)

                    
                    if n_c_ == 3:
                        
                        # Aj_Mortalidade_%
                        aj_mortalidade_f = aj_mortalidade_separate[1].replace("Aj Mortalidade ", "").replace("Aj Mortalidade", "")
                        
                        if not aj_mortalidade_f:
                            aj_mortalidade_f = aj_mortalidade_separate[2].split(" ")[0]
                        
                        aj_mortalidade_percent_arr.append(aj_mortalidade_f)
                        
                        # Aj_Mortalidade_kg
                        aj_mortalidade_kg_f = aj_mortalidade_separate[-1].split(" ")
                        
                        n_c_ = len(aj_mortalidade_kg_f)
                        
                        if n_c_ == 2:
                            aj_mortalidade_kg_f = aj_mortalidade_kg_f[0]
                        if n_c_ == 3:
                            aj_mortalidade_kg_f = aj_mortalidade_kg_f[1]
                        
                        aj_mortalidade_kg_arr.append(aj_mortalidade_kg_f)
                        
                        # Aj_Mortalidade_real
                        aj_mortalidade_real_f = aj_mortalidade_separate[-1].split(" ")[-1]                        
                        aj_mortalidade_real_arr.append(aj_mortalidade_real_f)

                    if n_c_ == 4:
                        # Aj_Mortalidade_%
                        aj_mortalidade_f = aj_mortalidade_separate[1].replace("Aj Mortalidade ", "").replace("Aj Mortalidade", "")
                        aj_mortalidade_percent_arr.append(aj_mortalidade_f)
                        
                         # Aj_Mortalidade_kg
                        aj_mortalidade_kg_f = aj_mortalidade_separate[2]
                        aj_mortalidade_kg_arr.append(aj_mortalidade_kg_f)
                        
                        # Aj_Mortalidade_real
                        aj_mortalidade_real_f = aj_mortalidade_separate[-1]
                        aj_mortalidade_real_arr.append(aj_mortalidade_real_f)

                        
                    if n_c_ == 5:
                        # Aj_Mortalidade_%
                        aj_mortalidade_f = aj_mortalidade_separate[2]
                        aj_mortalidade_percent_arr.append(aj_mortalidade_f)
                        
                        # Aj_Mortalidade_kg
                        aj_mortalidade_kg_f = aj_mortalidade_separate[3]
                        aj_mortalidade_kg_arr.append(aj_mortalidade_kg_f)
                        
                        # Aj_Mortalidade_real
                        aj_mortalidade_real_f = aj_mortalidade_separate[-1]
                        aj_mortalidade_real_arr.append(aj_mortalidade_real_f)
                
                # GET_Aj_Conv_Alimentar
                if arr_filter[38] == item:
                    # GET_Aj_Conv_Alimentar
                    aj_conv_alimentar_separate = remove_empty_spaces(new_str.split(", "))
                    n_c_ = len(aj_conv_alimentar_separate)
                    
                    
                    if n_c_ == 3:
                        
                        # GET_Aj_Conv_Alimentar_%                        
                        aj_conv_alimentar_f = aj_conv_alimentar_separate[1].replace("Aj Conv Alimentar", "").replace("Aj Conv Alimentar ", "")
                        
                        if not aj_conv_alimentar_f:
                            aj_conv_alimentar_f = aj_conv_alimentar_separate[-1].split(" ")[0]
                        
                        aj_conv_alimentar_percent_arr.append(aj_conv_alimentar_f)
                        
                        # GET_Aj_Conv_Alimentar_KG                        
                        aj_conv_alimentar_kg_f = aj_conv_alimentar_separate[-1].split(" ")
                        n_c_ = len(aj_conv_alimentar_kg_f)
                        
                        if n_c_ == 2:
                            aj_conv_alimentar_kg_f = aj_conv_alimentar_kg_f[0]                            
                        if n_c_ == 3:
                            aj_conv_alimentar_kg_f = aj_conv_alimentar_kg_f[1]
                            
                        aj_conv_alimentar_kg_arr.append(aj_conv_alimentar_kg_f)

                        # GET_Aj_Conv_Alimentar_$
                        aj_conv_alimentar_real_f = aj_conv_alimentar_separate[-1].split(" ")[-1]
                        aj_conv_alimentar_real_arr.append(aj_conv_alimentar_real_f)

                    if n_c_ == 4:
                        # GET_Aj_Conv_Alimentar_%
                        aj_conv_alimentar_f = aj_conv_alimentar_separate[1].replace("Aj Conv Alimentar", "").replace("Aj Conv Alimentar ", "")
                        aj_conv_alimentar_percent_arr.append(aj_conv_alimentar_f)
                        
                        # GET_Aj_Conv_Alimentar_KG
                        aj_conv_alimentar_kg_f = aj_conv_alimentar_separate[2]
                        aj_conv_alimentar_kg_arr.append(aj_conv_alimentar_kg_f)
                        
                        # GET_Aj_Conv_Alimentar_$
                        aj_conv_alimentar_real_f = aj_conv_alimentar_separate[-1]
                        aj_conv_alimentar_real_arr.append(aj_conv_alimentar_real_f)
                    
                    if n_c_ == 5:
                         # GET_Aj_Conv_Alimentar_%
                        aj_conv_alimentar_f = aj_conv_alimentar_separate[2]                       
                        aj_conv_alimentar_percent_arr.append(aj_conv_alimentar_f)
                        
                        # GET_Aj_Conv_Alimentar_KG
                        aj_conv_alimentar_kg_f = aj_conv_alimentar_separate[3]                        
                        aj_conv_alimentar_kg_arr.append(aj_conv_alimentar_kg_f)
                        
                        # GET_Aj_Conv_Alimentar_$
                        aj_conv_alimentar_real_f = aj_conv_alimentar_separate[-1]
                        aj_conv_alimentar_real_arr.append(aj_conv_alimentar_real_f)
                
                # GET_Aj Meritocracia (MT)
                if arr_filter[39] == item:
                    aj_meritocracia_mt_separate = new_str.split(", ")

                    # Extract the percentage value (%_Aj_Meritocracia_MT)
                    aj_meritocracia_mt_separate = remove_empty_spaces(aj_meritocracia_mt_separate)
                    n_c_ = len(aj_meritocracia_mt_separate)
                    
                    if n_c_ == 3:
                        
                        # GET_Aj Meritocracia_%
                        aj_meritocracia_mt_percent = aj_meritocracia_mt_separate[-1].split(" ")[0]
                        aj_meritocracia_mt_percent_arr.append(aj_meritocracia_mt_percent)
                        
                        # GET_Aj Meritocracia_kg
                        aj_meritocracia_mt_kg = aj_meritocracia_mt_separate[-1].split(" ")[1]
                        aj_meritocracia_mt_kg_arr.append(aj_meritocracia_mt_kg)
                        
                        aj_meritocracia_mt_real = aj_meritocracia_mt_separate[-1].split(" ")[2]
                        aj_meritocracia_mt_real_arr.append(aj_meritocracia_mt_real)
                        
                    if n_c_ == 4:
                        aj_meritocracia_mt_percent = aj_meritocracia_mt_separate[1].replace("Aj Meritocracia (MT) ", "").replace("Aj Meritocracia (MT)", "")
                        aj_meritocracia_mt_percent_arr.append(aj_meritocracia_mt_percent)
                        
                        # GET_Aj Meritocracia_kg
                        aj_meritocracia_mt_kg = aj_meritocracia_mt_separate[2]
                        aj_meritocracia_mt_kg_arr.append(aj_meritocracia_mt_kg)

                        aj_meritocracia_mt_real = aj_meritocracia_mt_separate[-1]
                        aj_meritocracia_mt_real_arr.append(aj_meritocracia_mt_real)
                        
                    if n_c_ == 5:
                        aj_meritocracia_mt_percent = aj_meritocracia_mt_separate[2]
                        aj_meritocracia_mt_percent_arr.append(aj_meritocracia_mt_percent)
                        
                        aj_meritocracia_mt_kg = aj_meritocracia_mt_separate[3]
                        aj_meritocracia_mt_kg_arr.append(aj_meritocracia_mt_kg)
                        
                        aj_meritocracia_mt_real = aj_meritocracia_mt_separate[-1]                        
                        aj_meritocracia_mt_real_arr.append(aj_meritocracia_mt_real)
                        
                # GET_Aj Calo Pata A
                if arr_filter[40] == item:
                    aj_calo_pata_separate = remove_empty_spaces(new_str.split(", "))
                    n_c_ = len(aj_calo_pata_separate)
                    
                    if n_c_ == 3:
                        # GET_Aj Calo Pata A_%
                        aj_calo_pata_percent = aj_calo_pata_separate[1].replace("Aj Calo Pata A ", "").replace("Aj Calo Pata A", "")
                        if not aj_calo_pata_percent:
                            aj_calo_pata_percent = aj_calo_pata_separate[-1].split(" ")[0]
                        
                        aj_calo_pata_a_percent_arr.append(aj_calo_pata_percent)
                        
                        
                        # GET_Aj Calo Pata A_KG
                        aj_calo_pata_kg = aj_calo_pata_separate[-1].split(" ")
                        if len(aj_calo_pata_kg) == 2:
                            aj_calo_pata_kg_f =  aj_calo_pata_kg[0]

                        if len(aj_calo_pata_kg) == 3:
                            aj_calo_pata_kg_f =  aj_calo_pata_kg[1]
                            
                        aj_calo_pata_a_kg_arr.append(aj_calo_pata_kg_f)
                        
                        # GET_Aj Calo Pata A_$
                        aj_calo_pata_real_f = aj_calo_pata_separate[-1].split(" ")[-1]
                        aj_calo_pata_a_real_arr.append(aj_calo_pata_real_f)
                      
                    if n_c_ == 4:
                        aj_calo_pata_percent = aj_calo_pata_separate[1].replace("Aj Calo Pata A ", "").replace("Aj Calo Pata A", "")
                        aj_calo_pata_a_percent_arr.append(aj_calo_pata_percent)
                        
                        aj_calo_pata_kg_f = aj_calo_pata_separate[2]
                        aj_calo_pata_a_kg_arr.append(aj_calo_pata_kg_f)
                        
                        aj_calo_pata_real_f = aj_calo_pata_separate[-1]
                        if " " in aj_calo_pata_real_f:
                            aj_calo_pata_real_f = aj_calo_pata_real_f[-1]
                            
                        aj_calo_pata_a_real_arr.append(aj_calo_pata_real_f)
                    if n_c_ == 5:
                        aj_calo_pata_percent = aj_calo_pata_separate[2]
                        aj_calo_pata_a_percent_arr.append(aj_calo_pata_percent)
                        
                        aj_calo_pata_kg_f = aj_calo_pata_separate[3]
                        aj_calo_pata_a_kg_arr.append(aj_calo_pata_kg_f)

                        aj_calo_pata_real_f = aj_calo_pata_separate[-1]
                        aj_calo_pata_a_real_arr.append(aj_calo_pata_real_f)
                
                # GET_ AJ_CONDENACOES
                if arr_filter[41] == item:
                    
                    aj_condenacoes_separate = remove_empty_spaces(new_str.split(", "))
                    n_c_ = len(aj_condenacoes_separate)
                    
                    if  n_c_ == 3:
                        # GET_ AJ_CONDENACOES_%
                        aj_condenacoes_percent_f = aj_condenacoes_separate[1].replace("Aj Condenações ","").replace("Aj Condenações","")
                        if not aj_condenacoes_percent_f:
                            aj_condenacoes_percent_f  = aj_condenacoes_separate[-1].split(" ")[0]                        
                        condenacoes_percent_arr.append(aj_condenacoes_percent_f)
                        
                        # GET_ AJ_CONDENACOES_KG
                        aj_condenacoes_kg_separate = aj_condenacoes_separate[-1].split(" ")
                        
                        if len(aj_condenacoes_kg_separate) == 2:
                            aj_condenacoes_kg_f = aj_condenacoes_kg_separate[0]                            
                            
                        if len(aj_condenacoes_kg_separate) == 3:
                            aj_condenacoes_kg_f = aj_condenacoes_kg_separate[1]
                        condenacoes_kg_arr.append(aj_condenacoes_kg_f)

                        # GET_ AJ_CONDENACOES_$
                        aj_condenacoes_real_separate = aj_condenacoes_separate[-1].split(" ")
                        if len(aj_condenacoes_real_separate) == 2:
                            aj_condenacoes_real_f = aj_condenacoes_real_separate[-1]
                        if len(aj_condenacoes_real_separate) == 3:
                            aj_condenacoes_real_f = aj_condenacoes_real_separate[-1]
                        
                        codenacoes_real_arr.append(aj_condenacoes_real_f)
                                          
                    if  n_c_ == 4:
                        
                        # GET_ AJ_CONDENACOES_%
                        aj_condenacoes_percent_f = aj_condenacoes_separate[1].replace("Aj Condenações ","").replace("Aj Condenações","")
                        if not aj_condenacoes_percent_f:
                            aj_condenacoes_percent_f  = aj_condenacoes_separate[2]                            
                        condenacoes_percent_arr.append(aj_condenacoes_percent_f)
                        
                        # GET_ AJ_CONDENACOES_KG
                        aj_condenacoes_kg_f_ = aj_condenacoes_separate[-1]                        
                        if " " not in aj_condenacoes_kg_f_:
                            aj_condenacoes_kg_f = aj_condenacoes_separate[2]                            
                        if " " in aj_condenacoes_kg_f_: 
                            aj_condenacoes_kg_f = aj_condenacoes_kg_f_[0]                            
                        condenacoes_kg_arr.append(aj_condenacoes_kg_f)
                        
                        # GET_ AJ_CONDENACOES_$
                        aj_condenacoes_real_separate = aj_condenacoes_separate[-1]
                        if " " in aj_condenacoes_real_separate:
                            aj_condenacoes_real_f = aj_condenacoes_separate[-1].split(" ")[-1]
                        else:
                            aj_condenacoes_real_f = aj_condenacoes_separate[-1]
                        
                        codenacoes_real_arr.append(aj_condenacoes_real_f)
                        
                    if  n_c_ == 5:
                        
                        # GET_ AJ_CONDENACOES_% 
                        aj_condenacoes_percent_f = aj_condenacoes_separate[2]
                        condenacoes_percent_arr.append(aj_condenacoes_percent_f)
                        
                        # GET_ AJ_CONDENACOES_KG
                        aj_condenacoes_kg_f = aj_condenacoes_separate[3]
                        condenacoes_kg_arr.append(aj_condenacoes_kg_f)
                        
                        # GET_ AJ_CONDENACOES_$
                        aj_condenacoes_real_separate = aj_condenacoes_separate[-1]
                        codenacoes_real_arr.append(aj_condenacoes_real_f)
                
                # GET_ Aj Qualidade (QT)
                if arr_filter[42] == item:
                    aj_qualidade_separate = remove_empty_spaces(new_str.split(", "))
                    n_c_ = len(aj_qualidade_separate)
                    if n_c_ == 3:
                        # GET_ Aj Qualidade (QT)_%
                        aj_qualidade_percent_f = aj_qualidade_separate[-1].split(" ")[0]
                        aj_qualidade_percent_arr.append(aj_qualidade_percent_f)
                        
                        # GET_ Aj Qualidade (QT)_kg
                        aj_qualidade_kg_f = aj_qualidade_separate[-1].split(" ")[1]
                        aj_qualidade_kg_arr.append(aj_qualidade_kg_f)
                        
                        # GET_ Aj Qualidade (QT)_$
                        aj_qualidade_real_f = aj_qualidade_separate[-1].split(" ")[-1]
                        aj_qualidade_real_arr.append(aj_qualidade_real_f)
                        
                    if n_c_ == 4:
                        # GET_ Aj Qualidade (QT)_%
                        aj_qualidade_percent_f = aj_qualidade_separate[1].replace("Aj Qualidade (QT) ", "").replace("Aj Qualidade (QT)", "")
                        
                        if not aj_qualidade_percent_f:
                            aj_qualidade_percent_f = nan
                            
                        aj_qualidade_percent_arr.append(aj_qualidade_percent_f)
                        
                        # GET_ Aj Qualidade (QT)_kg
                        aj_qualidade_kg_f = aj_qualidade_separate[2]                    
                        aj_qualidade_kg_arr.append(aj_qualidade_kg_f)
                        
                        # GET_ Aj Qualidade (QT)_$
                        aj_qualidade_real_f = aj_qualidade_separate[-1]
                        
                        if " " in aj_qualidade_real_f:
                          aj_qualidade_real_f= aj_qualidade_real_f.split(" ")[-1]
                          
                        aj_qualidade_real_arr.append(aj_qualidade_real_f)

                    if n_c_ == 5:
                        # GET_ Aj Qualidade (QT)_%
                        aj_qualidade_percent_f = aj_qualidade_separate[2]                        
                        aj_qualidade_percent_arr.append(aj_qualidade_percent_f)
                        
                        # GET_ Aj Qualidade (QT)_kg
                        aj_qualidade_kg_f = aj_qualidade_separate[3]
                        aj_qualidade_kg_arr.append(aj_qualidade_kg_f)
                        
                        # GET_ Aj Qualidade (QT)_$
                        aj_qualidade_real_f = aj_qualidade_separate[-1]
                        aj_qualidade_real_arr.append(aj_qualidade_real_f)

                # GET_AJ_ESTRUTURAL
                if arr_filter[43] == item:
                    aj_estrutural_separate = remove_empty_spaces(new_str.split(", "))
                    n_c_ = len(aj_estrutural_separate)
                    
                    if n_c_ == 3:
                        # AJ_ESTRUTURAL_%
                        aj_estrutural_percent_f = aj_estrutural_separate[1].replace("Aj Estrutural ", "").replace("Aj Estrutural", "")
                        if not aj_estrutural_percent_f:
                            aj_estrutural_percent_f = aj_estrutural_separate[-1].split(" ")[0]
                        
                        aj_estrutural_percent_arr.append(aj_estrutural_percent_f)
                        
                        # AJ_ESTRUTURAL_KG
                        aj_estrutural_kg_ = aj_estrutural_separate[-1].split(" ")
                        if len(aj_estrutural_kg_) == 2:
                            aj_estrutural_kg_f = aj_estrutural_kg_[0]
                        if len(aj_estrutural_kg_) == 3:
                            aj_estrutural_kg_f = aj_estrutural_kg_[1]
                            
                        aj_estrutural_kg_arr.append(aj_estrutural_kg_f)
                        
                        # AJ_ESTRUTURAL_$
                        aj_estrutural_real_ = aj_estrutural_separate[-1].split(" ")[-1]
                        aj_estrutural_real_arr.append(aj_estrutural_real_)
                        
                    if n_c_ == 4:
                        # AJ_ESTRUTURAL_%
                        aj_estrutural_percent_f = aj_estrutural_separate[1].replace("Aj Estrutural ", "").replace("Aj Estrutural", "")
                        aj_estrutural_percent_arr.append(aj_estrutural_percent_f)
                                                
                        # AJ_ESTRUTURAL_KG
                        aj_estrutural_kg_f = aj_estrutural_separate[2]
                        aj_estrutural_kg_arr.append(aj_estrutural_kg_f)

                        # AJ_ESTRUTURAL_$
                        aj_estrutural_real_ = aj_estrutural_separate[-1]
                        aj_estrutural_real_arr.append(aj_estrutural_real_)
                        
                    if n_c_ == 5:
                        # AJ_ESTRUTURAL_%
                        aj_estrutural_percent_f = aj_estrutural_separate[2]
                        aj_estrutural_percent_arr.append(aj_estrutural_percent_f)
                        
                        # AJ_ESTRUTURAL_KG
                        aj_estrutural_kg_ = aj_estrutural_separate[3]
                        aj_estrutural_kg_arr.append(aj_estrutural_kg_f)
                        
                        # AJ_ESTRUTURAL_$
                        aj_estrutural_real_ = aj_estrutural_separate[-1]
                        aj_estrutural_real_arr.append(aj_estrutural_real_)
                
                # GET_AJ_PROCEDIMENTOS
                if arr_filter[44] == item:
                    aj_procedimento_separate = remove_empty_spaces(new_str.split(", "))
                    
                    n_c_ = len(aj_procedimento_separate)
                    
                    if n_c_ == 3:
                        # AJ_PROCEDIMENTOS_%
                        aj_procedimento_percent = aj_procedimento_separate[1].replace("Aj Procedimento ","").replace("Aj Procedimento","")
                        if not aj_procedimento_percent:
                            aj_procedimento_percent = aj_procedimento_separate[-1].split(" ")[0]
                        aj_procedimentos_percent_arr.append(aj_procedimento_percent)
                        
                        # AJ_PROCEDIMENTOS_KG
                        aj_procedimento_kg = aj_procedimento_separate[-1].split(" ")
                        
                        if len(aj_procedimento_kg) == 2:
                            aj_procedimento_kg_f = aj_procedimento_kg[0]
                            
                        if len(aj_procedimento_kg) == 3:
                            aj_procedimento_kg_f = aj_procedimento_kg[1]
                            
                        aj_procedimentos_kg_arr.append(aj_procedimento_kg_f)
                        
                        # AJ_PROCEDIMENTOS_$
                        aj_procedimento_real = aj_procedimento_separate[-1].split(" ")[-1]
                        aj_procedimentos_real_arr.append(aj_procedimento_real)

                    if n_c_ == 4:
                        # AJ_PROCEDIMENTOS_%
                        aj_procedimento_percent = aj_procedimento_separate[1].replace("Aj Procedimento ","").replace("Aj Procedimento","")
                        aj_procedimentos_percent_arr.append(aj_procedimento_percent)
                        
                        # AJ_PROCEDIMENTOS_KG
                        aj_procedimento_kg = aj_procedimento_separate[2]
                        if " " in aj_procedimento_kg:
                            aj_procedimento_kg = aj_procedimento_kg.split(" ")[0]
                            
                        aj_procedimentos_kg_arr.append(aj_procedimento_kg)
                        
                        # AJ_PROCEDIMENTOS_$
                        aj_procedimento_real = aj_procedimento_separate[-1]
                        aj_procedimentos_real_arr.append(aj_procedimento_real)
                        
                    if n_c_ == 5:
                        # AJ_PROCEDIMENTOS_%
                        aj_procedimento_percent = aj_procedimento_separate[2]
                        aj_procedimentos_percent_arr.append(aj_procedimento_percent)

                        # AJ_PROCEDIMENTOS_KG
                        aj_procedimento_kg = aj_procedimento_separate[3]
                        aj_procedimentos_kg_arr.append(aj_procedimento_kg)
                        
                        # AJ_PROCEDIMENTOS_$
                        aj_procedimento_real = aj_procedimento_separate[-1]
                        aj_procedimentos_real_arr.append(aj_procedimento_real)

                # Aj Processos/Procedimentos (PP)
                if arr_filter[45] == item:
                    aj_processos_separate = remove_empty_spaces(new_str.split(", "))
                    n_c_ = len(aj_processos_separate)
                    if n_c_ == 3:
                        # Aj Processos/Procedimentos (PP)_%
                        aj_processos_percent = aj_processos_separate[-1].split(" ")[0]
                        aj_processos_procedimentos_pp_percent_arr.append(aj_processos_percent)
                        
                        # Aj Processos/Procedimentos (PP)_kg
                        aj_processos_kg = aj_processos_separate[-1].split(" ")[1]
                        aj_processos_procedimentos_pp_kg_arr.append(aj_processos_kg)

                        
                        # Aj Processos/Procedimentos (PP)_$
                        aj_processos_real = aj_processos_separate[-1].split(" ")[-1]
                        aj_processos_procedimentos_pp_real_arr.append(aj_processos_real)
                        
                    if n_c_ == 4:
                        # Aj Processos/Procedimentos (PP)_%
                        aj_processos_percent = aj_processos_separate[1].replace("Aj Processos/Procedimentos (PP) ", "").replace("Aj Processos/Procedimentos (PP)", "")
                        if not aj_processos_percent:
                            aj_processos_percent = nan
                            
                        aj_processos_procedimentos_pp_percent_arr.append(aj_processos_percent)
                        
                        # Aj Processos/Procedimentos (PP)_kg
                        aj_processos_kg = aj_processos_separate[2]
                        if " " in aj_processos_kg:
                            aj_processos_kg = aj_processos_kg[0]
                        aj_processos_procedimentos_pp_kg_arr.append(aj_processos_kg)
                        
                        # Aj Processos/Procedimentos (PP)_$
                        aj_processos_real = aj_processos_separate[-1]
                        aj_processos_procedimentos_pp_real_arr.append(aj_processos_real)
                        
                    if n_c_ == 5:
                        # Aj Processos/Procedimentos (PP)_%
                        aj_processos_percent = aj_processos_separate[2]
                        aj_processos_procedimentos_pp_percent_arr.append(aj_processos_percent)
                        
                        # Aj Processos/Procedimentos (PP)_kg
                        aj_processos_kg = aj_processos_separate[3]
                        aj_processos_procedimentos_pp_kg_arr.append(aj_processos_kg)
                        
                        # Aj Processos/Procedimentos (PP)_$
                        aj_processos_real = aj_processos_separate[-1]                        
                        aj_processos_procedimentos_pp_real_arr.append(aj_processos_real)    

                # GET_Resultado Lote
                if arr_filter[46] == item:
                    resultado_lote_separate = remove_empty_spaces(new_str.split(", "))
                    n_c_ = len(resultado_lote_separate)
                    if n_c_ == 3:
                        # Resultado Lote_%
                        res_lot_f = resultado_lote_separate[-1].split(" ")[0]
                        resultado_lote_percent_arr.append(res_lot_f)
                        
                        # Resultado Lote_kg
                        res_lot_kg = resultado_lote_separate[-1].split(" ")[1]
                        resultado_lote_kg_arr.append(res_lot_kg)
                        
                        # Resultado Lote_$ 
                        res_lot_real = resultado_lote_separate[-1].split(" ")[2]
                        resultado_lote_real_arr.append(res_lot_real)
                        pass
                    if n_c_ == 4:
                        # Resultado Lote_%
                        res_lot_f = resultado_lote_separate[1].replace("Resultado Lote ","").replace("Resultado Lote","")
                        resultado_lote_percent_arr.append(res_lot_f)
                        
                        # Resultado Lote_kg
                        res_lot_kg = resultado_lote_separate[2]
                        resultado_lote_kg_arr.append(res_lot_kg)
                        
                        # Resultado Lote_$
                        res_lot_real = resultado_lote_separate[-1]
                        resultado_lote_real_arr.append(res_lot_real)
                        
                        pass
                    
                    if n_c_ == 5:
                        # Resultado Lote_%
                        res_lot_f = resultado_lote_separate[2]
                        resultado_lote_percent_arr.append(res_lot_f)
                        
                        
                        
                        # Resultado Lote_kg
                        res_lot_kg = resultado_lote_separate[3]
                        resultado_lote_kg_arr.append(res_lot_kg)
                        
                        # Resultado Lote_$
                        res_lot_real = resultado_lote_separate[-1]
                        resultado_lote_real_arr.append(res_lot_real)
                
                if arr_filter[47] == item:
                    
                    renda_b_ave = remove_empty_spaces(new_str.split(", "))
                    n_c_ = len(renda_b_ave)
                    
                    if n_c_ == 3:
                        renda_b_ave_f = renda_b_ave[-1].replace("Renda Bruta/Ton", ";").replace("Renda Bruta / m2", ";").replace(" ","")
                        #Renda Bruta / Ave ; Renda Bruta/Ton ; Renda Bruta / m2
                        renda_b_ave_f = renda_b_ave_f.split(";")
                        
                        #Renda Bruta / Ave
                        renda_b_ave_f_ = renda_b_ave_f[0]
                        
                        # Renda Bruta/Ton
                        renda_b_t_f = renda_b_ave_f[1]
                        
                        #  Renda Bruta / m2
                        renda_b_m2_f = renda_b_ave_f[2]
                        
                        ave_real_arr.append(renda_b_ave_f_)
                        ton_real_arr.append(renda_b_t_f)
                        m2_real_arr.append(renda_b_m2_f)
                        
                        pass
                    if n_c_ == 4:
                        
                        #Renda Bruta / Ave
                        renda_b_ave_f_ = renda_b_ave[1].replace("Renda Bruta / Ave ","").replace(" Renda Bruta/Ton", "").replace(" ", "")

                        if "RendaBruta/Ave" in  renda_b_ave_f_:
                            renda_b_ave_f_ = renda_b_ave[2].replace(" ", "")
                        
                        ave_real_arr.append(renda_b_ave_f_)
                        
                        # Renda Bruta/Ton
                        renda_b_t_f = renda_b_ave[2]
                        ton_real_arr.append(renda_b_t_f)
                        
                        #  Renda Bruta / m2
                        renda_b_m2_f = renda_b_ave[-1].replace("Renda Bruta / m2 ", "").replace("Renda Bruta / m2", "")
                        
                        if "Renda Bruta/Ton" in renda_b_m2_f:
                            renda_b_m2_f = (renda_b_ave[-1].split("Renda Bruta / m2")[-1]).replace(" ", "")
                        
                        m2_real_arr.append(renda_b_m2_f)

                    if n_c_ == 5:
                        #Renda Bruta / Ave
                        renda_b_ave_f_ = renda_b_ave[1].replace("Renda Bruta / Ave ", "")
                        ave_real_arr.append(renda_b_ave_f_)
                        
                        # Renda Bruta/Ton
                        renda_b_t_f = renda_b_ave[3]
                        ton_real_arr.append(renda_b_t_f)
                        
                        #  Renda Bruta / m2
                        renda_b_m2_f = renda_b_ave[-1].replace("Renda Bruta / m2 ", "").replace("Renda Bruta / m2", "")
                        m2_real_arr.append(renda_b_m2_f)

                # GET FUNRURAL - Como todos nao tem, temos que atribuir o valor nan mantendo a ordem pra isso foi usado compreesao de lista
                if arr_filter[48] == item:
                    fun_rural_separate = remove_empty_spaces(new_str.split(", "))

                    funrural_arr.append(fun_rural_separate)
                    
                if arr_filter[49] == item:
                    senar_separate = remove_empty_spaces(new_str.split(", "))
                    id_s = new_str.split(", ")[0]

                    if "Imposto" in str(senar_separate) and  "SENAR" in str(senar_separate):
                        n_c1 = len(senar_separate)
                            
                        if n_c1 == 3:
                            senar_separate = (senar_separate[-1].replace("SENAR", "").replace(" ", ""))
                        if n_c1 == 4:
                            pass
                        
                        senar_arr.append({"id": id_s, "Data": senar_separate})
                        # senar_arr.append(senar_separate)
                    
                if arr_filter[50] == item:
                    
                    cnt_corrente = remove_empty_spaces(new_str.split(", "))
                    id_l = (cnt_corrente[0])
                    cnt_corrente = cnt_corrente[1].replace("Conta Corrente Produtor ", "").replace("Conta Corrente Produtor", "")
                    arr_tmp.append({"id":id_l,"Data":cnt_corrente})
                    # f = [n for n in len(arr_tmp) if arr_tmp[n] == arr_tmp[n+1]]
                    
                    # conta_corrente.append(cnt_corrente)
                    
                if arr_filter[51] == item:
                    cnt_vinculada = remove_empty_spaces(new_str.split(", "))
                    cnt_vinculada_id = (new_str.split(", ")[0])
                    
                    cnt_vinculada = cnt_vinculada[1].replace("Conta Corrente Vinculada ", "").replace("Conta Corrente Vinculada", "")
                    arr_tmp_2.append({'id':cnt_vinculada_id,'Data':cnt_vinculada})
                    
                    if not cnt_vinculada:
                        cnt_vinculada = "nan"
                        
                    # conta_vinculada.append(cnt_vinculada)

                if arr_filter[52] == item:
                    conv_aliment_s = remove_empty_spaces(new_str.split(", "))

                    n_c_ = len(conv_aliment_s)

                    # Real | Prev | Aj | Diferença

                    if n_c_ == 3:

                        nc_2 = len(conv_aliment_s[2].split(" "))

                        if nc_2 == 3:
                            # real
                            conv_aliment_real = (conv_aliment_s[1].replace("Conversão Alimentar", "").replace(" ", ""))
                            
                            # real_aj
                            conv_aliment_real_aj_prev_s = conv_aliment_s[2].split(" ")
                            conv_aliment_real_aj = (conv_aliment_real_aj_prev_s[0])
                            

                            #prevaj
                            conv_aliment_prev_aj =  conv_aliment_s[2].split(" ")[1]

                            #diferenca
                            conv_aliment_d = conv_aliment_s[2].split(" ")[2]

                            
                        if nc_2 == 4:
                            # real
                            conv_aliment_real  = (conv_aliment_s[2].split(" "))
                            conv_aliment_real =  conv_aliment_real[0]                            

                            # real_aj
                            conv_aliment_real_aj = conv_aliment_s[2].split(" ")[1]

                            # prev AJ
                            conv_aliment_prev_aj =  conv_aliment_s[2].split(" ")[2]
                            
                            #diferenca
                            conv_aliment_d = conv_aliment_s[2].split(" ")[3]

                    if n_c_ == 4:
                        # real
                        conv_aliment_real = (conv_aliment_s[1].replace("Conversão Alimentar", "").replace(" ", ""))
                        
    
                        # real_aj_prev
                        conv_aliment_real_aj_prev_s = conv_aliment_s[2].split(" ")

                        # real AJ
                        conv_aliment_real_aj = conv_aliment_real_aj_prev_s[0]

                        # prev AJ
                        conv_aliment_prev_aj =  conv_aliment_real_aj_prev_s[1]
                        
                        # Diferença
                        conv_aliment_d = conv_aliment_s[3]
                        
                    if n_c_ == 5:
                        conv_aliment_real = (conv_aliment_s[1].replace("Conversão Alimentar", "").replace(" ", ""))

                        # real AJ
                        conv_aliment_real_aj = conv_aliment_s[2]

                        # prev AJ
                        conv_aliment_prev_aj =  conv_aliment_s[3]
                        
                        # Diferença
                        conv_aliment_d = conv_aliment_s[4]
                    
                    # if n_c_ == 6:

                    conv_aliment_real_arr.append(conv_aliment_real)
                    
                    conv_aliment_real_aj_arr.append(conv_aliment_real_aj)
                    conv_aliment_prev_aj_arr.append(conv_aliment_prev_aj)
                    conv_aliment_diferenca_arr.append(conv_aliment_d)
                
                # Idade de Abate REAL | PREV aj | DIFERE|

                if arr_filter[53]  == item:
                    idade_d_abt_s = (remove_empty_spaces(new_str.split(", ")))

                    n_c_ = len(idade_d_abt_s)

                    if n_c_  == 3:
                        #real
                        idad_d_abt_real  =  (remove_empty_spaces(new_str.split(", ")))
                        idad_d_abt_real_f = (idad_d_abt_real[1].replace("Idade de Abate ", ""))
                        
                        if "Idade de Abate" in idad_d_abt_real_f:
                            idad_d_abt_real_f = (idad_d_abt_real[-1].split(" ")[0]).replace(" ", "")
                            # print(idade_d_abt_s, idad_d_abt_real_f)
                        idade_de_abate_real_arr.append(idad_d_abt_real_f)
                        
                        # print(idad_d_abt_real_f)
                        #PrevAj
                        idad_d_abt_prevaj = idad_d_abt_real[-1].split(" ")
                        idade_de_abate_real_prev_aj_arr.append(idad_d_abt_prevaj[0])

                        #Diferenca
                        idade_de_abate_real_dif_arr.append(idad_d_abt_prevaj[1])

                    if n_c_  == 4:
                        # real
                        idad_d_abt_real = (idade_d_abt_s[1].replace("Idade de Abate", ""))
                        idade_de_abate_real_arr.append(idad_d_abt_real)

                        #PrevAj
                        idad_d_abt_prevaj = idade_d_abt_s[2]
                        idade_de_abate_real_prev_aj_arr.append(idad_d_abt_prevaj)
                        
                        # diferenca
                        idad_d_abt_diferenca = idade_d_abt_s[3]
                        idade_de_abate_real_dif_arr.append(idad_d_abt_diferenca)
                        

                    # if n_c_  == 5:
                    #     print(idade_d_abt_s)
                
                if arr_filter[54] == item:
                    mrt_s = remove_empty_spaces(new_str.split(", "))
                    if (not "Aj" in str(mrt_s)):
                        n_c = len(mrt_s)
                        if n_c == 3:

                            nc_2 = len(mrt_s[2].split(" "))

                            if nc_2 == 3:
                                # real 
                                mrt_f = (mrt_s[1].replace("Mortalidade", " "))

                                # real aj
                                mrt_real_aj = mrt_s[2].split(" ")[0]
                                mrt_real_aj = mrt_real_aj.replace(" ", "")

                                # prev
                                mrt_aj_f = mrt_s[2].split(" ")[1]
                                mrt_aj_f = mrt_aj_f.replace(" ", "")

                                # diferenca
                                mrt_dife = mrt_s[2].split(" ")
                                mrt_dife = mrt_dife[2].replace(" ", "")
                            
                            if nc_2 == 4:
                                #  # real
                                mrt_f = mrt_s[2]
                                mrt_f = (mrt_f.split(" ")[0])


                                # # real AJ
                                mrt_real_aj = mrt_s[2]
                                mrt_real_aj = mrt_real_aj.split(" ")[1]
                                
                                
                                # # prev Aj
                                mrt_aj_f = mrt_s[2].split(" ")
                                mrt_aj_f = mrt_aj_f[2]

                                # # DIFERENCA
                                mrt_dife = mrt_s[-1].split(" ")
                                mrt_dife = mrt_dife[3]
                                
                        if n_c == 4:
                            # real
                            mrt_f = mrt_s[1].replace("Mortalidade", "").replace(" ","")

                            # real AJ e PREV 
                            mrt_aj_prev =  remove_empty_spaces(mrt_s[2].split(" "))

                            # real AJ
                            mrt_real_aj = mrt_aj_prev[0]
                            
                            # prev Aj
                            mrt_aj_f = mrt_aj_prev[1]
                            
                            
                            # DIFERENCA
                            mrt_dife = mrt_s[-1]

                            pass
                        if n_c == 5:
                            # real
                            mrt_f = mrt_s[1].replace("Mortalidade", "").replace(" ","")
                            
                            # ['994010-5700492391', 'Mortalidade 7,7941', '8,3821', '4,2132', '4,169']
                                        # 0                            1        2        3         4
                            #realAj
                            mrt_real_aj = mrt_s[2].replace(" ","")

                            
                            # prev aj
                            mrt_aj_f = mrt_s[3].replace(" ", "")

                            # Diferenca
                            mrt_dife = mrt_s[-1]

        
                        mortalidade_real_aj_arr.append(mrt_real_aj)
                        mortalidade_real_arr.append(mrt_f)
                        mortalidade_prev.append(mrt_aj_f)
                        mortalidade_diferenca.append(mrt_dife)

                if arr_filter[55] == item:
                    per_cl_pta_s = remove_empty_spaces(new_str.split(", "))
                    n_c = len(per_cl_pta_s)
                    # real, prev, diferenca
                    if n_c == 3:
                        nc_2 = len(per_cl_pta_s[2].split(" "))
                        if nc_2 == 2:

                            # real
                            clpta_f = (per_cl_pta_s[1].replace("% Calo de Pata A", "").replace(" ",""))

                            # prev
                            clpta_prev_f = per_cl_pta_s[2].split(" ")[0]

                            #diferenca
                            clpta_dif_f =  per_cl_pta_s[2].split(" ")[1]

                        if nc_2 == 3:

                            # real
                            clpta_f = per_cl_pta_s[2].split(" ")[0]

                            # prev
                            clpta_prev_f = per_cl_pta_s[2].split(" ")[1]

                            #diferenca
                            clpta_dif_f = per_cl_pta_s[2].split(" ")[2]
                            
                    if n_c == 4:

                        # real
                        clpta_f = (per_cl_pta_s[1].replace("% Calo de Pata A", "").replace(" ", ""))
                        
                        # prev
                        clpta_prev_f = per_cl_pta_s[2].replace(" ", "")

                        #diferenca 
                        clpta_dif_f = per_cl_pta_s[-1].replace(" ", "")

                    if n_c == 5:
                        pass
                    percent_calo_real_arr.append(clpta_f)
                    percent_calo_prev_arr.append(clpta_prev_f)
                    percent_calo_dife_arr.append(clpta_dif_f)

                if arr_filter[56] == item:
                    p_arran_r_s = (remove_empty_spaces(new_str.split(", ")))
                    n_c1 = len(p_arran_r_s)

                    if n_c1 == 3:
                        n_c2 = len(p_arran_r_s[2].split(" "))

                        if n_c2 == 2:
                            # real
                            p_arran_r_f = p_arran_r_s[1].replace("% Arranhaduras", "").replace("% Arranhaduras/Lesão", "").replace("/Lesão", "").replace(" ", "")

                            # prev AJ
                            p_arran_prev = p_arran_r_s[2].split(" ")[0]

                            # diferenca
                            p_arran_diferenca = p_arran_r_s[2].split(" ")[1]

                        if n_c2 == 3:
                            # real
                            p_arran_r_f = p_arran_r_s[2].split(" ")[0]

                            # prev AJ
                            p_arran_prev = p_arran_r_s[2].split(" ")[1]
                           
                            # diferenca
                            p_arran_diferenca = p_arran_r_s[2].split(" ")[2]

                    if n_c1 == 4:
                        # real
                        p_arran_r_f = (p_arran_r_s[1].replace("% Arranhaduras", "").replace("% Arranhaduras/Lesão", "").replace("/Lesão", "").replace(" ", ""))

                        # prev aj
                        p_arran_prev = p_arran_r_s[2]

                        #dife
                        p_arran_diferenca = p_arran_r_s[3]

                    if n_c1 == 5:
                        pass
                    
                    percent_arranhaduras_real_arr.append(p_arran_r_f)
                    percent_arranhaduras_prevaj_arr.append(p_arran_prev)
                    percent_arranhaduras_diferenca_arr.append(p_arran_diferenca)
                    
                if arr_filter[57] == item:

                    
                    papo_cheio_s = (remove_empty_spaces(new_str.split(", ")))
                    n_c1 = len(papo_cheio_s)
                    
                    
                    if n_c1 == 3:
                        
                        nc_2 = len(papo_cheio_s[2].split(" "))

                        if nc_2 == 2:

                            # real
                            papo_cheio_real = (papo_cheio_s[1].replace("% Papo Cheio", "").replace(" ", ""))

                            # prev
                            papo_cheio_prev = papo_cheio_s[2].split(" ")[0]

                            #diferenca
                            papo_cheio_dife =  papo_cheio_s[2].split(" ")[1]

                        if nc_2 == 3:
                            # real
                            papo_cheio_real = (papo_cheio_s[2].split(" ")[0])

                            # # prev
                            papo_cheio_prev = papo_cheio_s[2].split(" ")[1]

                            # #diferenca
                            papo_cheio_dife =  papo_cheio_s[2].split(" ")[2]


                    if n_c1 == 4:
                        # real
                        papo_cheio_real = (papo_cheio_s[1].replace("% Papo Cheio", "").replace(" ", ""))


                        # prev
                        papo_cheio_prev = papo_cheio_s[2]

                        #diferenca
                        papo_cheio_dife =  papo_cheio_s[3]
                
                    percent_papo_cheio_real_arr.append(papo_cheio_real)
                    percent_papo_cheio_prev_arr.append(papo_cheio_prev)
                    percent_papo_cheio_diferenca_arr.append(papo_cheio_dife)
                
                if arr_filter[58] == item:

                    p_codenacao_s = (remove_empty_spaces(new_str.split(", ")))
                    n_c1 = len(p_codenacao_s)

                    if n_c1 == 3:
                        p_codenacao_real_s = p_codenacao_s[2].split(" ")
                        nc_2_ = len(p_codenacao_real_s)
                        if nc_2_ == 2:
                            # real
                            p_codenacao_real = (p_codenacao_s[1].replace("% Condenação ", "").replace(" ", ""))
                            
                            #prev
                            p_codenacao_prev = p_codenacao_s[2].split(" ")[0]
                            
                            #diferenca
                            p_codenacao_diferenca = p_codenacao_s[2].split(" ")[1]

                        if nc_2_ == 3:
                           # real
                            p_codenacao_real = (p_codenacao_s[2].split(" "))[0]

                            #prev
                            p_codenacao_prev = p_codenacao_s[2].split(" ")[1]
                            
                            #diferenca
                            p_codenacao_diferenca = p_codenacao_s[2].split(" ")[2]

                    if n_c1 == 4:
                        # real
                        p_codenacao_real = p_codenacao_s[1].replace("% Condenação ", "").replace(" ", "")

                        #prev
                        p_codenacao_prev = p_codenacao_s[2]

                        #diferenca
                        p_codenacao_diferenca  = p_codenacao_s[3]


                    percent_codenacao_real_arr.append(p_codenacao_real)
                    percent_codenacao_prev_arr.append(p_codenacao_prev)
                    percent_codenacao_diferenca_arr.append(p_codenacao_diferenca)
                
                if arr_filter[59] == item:
                    centro_ = remove_empty_spaces(new_str.split(", "))
                    nc_ = len(centro_)
                    if nc_ == 2:
                        centro_s = (centro_[-1].split(" "))[1]
                        centro_s = centro_s.replace(" ", "")
                        centro_f = centro_s
                
                    if nc_ == 3:
                        centro_s = centro_[1]
                        centro_s = centro_s.replace("Centro", "").replace(" ", "")
                        centro_f = centro_s
                        
                    centro_arr.append(centro_f)
                    
    # metodo usando compreessao de lista para FUNRURAL           
    id_para_valor = {item[0]: item[2] for item in funrural_arr}
    funrural_arr_f = [id_para_valor.get(id, 'nan') for id in id_uni]
    
    
    # A partir do GET_percentual_basico pega Get_Kg_carne e pega $R_Base
    for eval_ in mod_2:
        if len(eval_) == 3:
            carne_final = eval_[2].split(" ")[1]
            real_base = eval_[2].split(" ")[-1]
            real_base_arr.append(real_base)
            
            carne_base_arr.append(carne_final)
            
        if len(eval_) == 4:
            carne_base_arr.append(eval_[2])
            real_base = eval_[-1]
            real_base_arr.append(real_base)
            
        if len(eval_) == 5:
            carne_base_arr.append(eval_[3])
            real_base = eval_[-1]
            real_base_arr.append(real_base)
            
    
    # SE O ID PRINCIAPL NAO ESTIVER NA LISTA DE ID'S DO ARRAY DE DADOS TEMPORARIO
    
    # for id_ in key_arr:
    #     if id_ not in arr_tmp:
    #         print("ID: ", id_)
    
    # print(len(conv_aliment_real_arr))
    # print("Quant. Itens in array 1:", len(aj_sazonalidade_percent_arr))
    # print("Quant. Itens in array 2:", len(aj_sazonalidade_kg_arr))
    # #grava os dados para a nova tabela
    # arr_pedido = list(dict.fromkeys(mod))
    
        
    for id in range(len(arr_tmp)):
        try:
            if id+1 == len(arr_tmp):
                id=len(arr_tmp)
            else:
                id=id+1

            if arr_tmp[id]['id']  == arr_tmp[int(id-1)]['id']:
                conta_corrente_arr.append(arr_tmp[(id)])
            if not arr_tmp[id]['id'] in [ id_['id']  for id_ in conta_corrente_arr]:
                conta_corrente_arr.append(arr_tmp[id])
        except IndexError:
            pass
        
    for id in range(len(arr_tmp_2)):
        try:
            if id+1 == len(arr_tmp_2):
                id=len(arr_tmp_2)
            else:
                id=id+1
            
            if arr_tmp_2[id]['id']  == arr_tmp_2[int(id-1)]['id']:
                conta_vinculada.append(arr_tmp_2[(id)])
                
            if not arr_tmp_2[id]['id'] in [ id_['id']  for id_ in conta_vinculada]:
                conta_vinculada.append(arr_tmp_2[id])
        except IndexError:
            pass

    conta_corrente_arr = processar_dicionarios(key_arr, conta_corrente_arr)
    conta_vinculada = processar_dicionarios(key_arr, conta_vinculada)
    senar_arr =  processar_dicionarios(key_arr, senar_arr)
    
    new_dataFrame["CHAVE"] = key_arr
    new_dataFrame["CLIFOR"] =  clifor_arr
    new_dataFrame["INTEGRADO"] = name_arr
    new_dataFrame["MUNICIPIO"] = arr_municipio
    new_dataFrame["TECNICO"] = tecnico_arr
    new_dataFrame["AREA_ALOJ"] = arr_area_aloj
    new_dataFrame["TELEFONE"] = telefone_arr
    new_dataFrame["AVIARIO"] = aviario_arr
    new_dataFrame["EMAIL"] = email_arr
    new_dataFrame["T_VENTILACAO"] = t_vent_arr
    new_dataFrame["TIPO_PRODUTO"] = arr_categoria
    new_dataFrame["LINHAGEM"] = arr_linhagem
    new_dataFrame["KG_M2"] = kgm2_arr
    new_dataFrame["MATERIAL_GENETICO"] = material_arr
    new_dataFrame["AVE_M2"] = ave_m2_arr
    new_dataFrame["QUANT_ALOJADO"] = arr_quant_alojado
    new_dataFrame["DATA_ALOJ"] = arr_data_aloj
    new_dataFrame["QUANT_ABATE"] = qabate_arr
    new_dataFrame["MORTE_TOTAL"] = mort_total_arr
    new_dataFrame["QUANTIDADE_MORTOS"] = quant_mortes_arr
    new_dataFrame["QUANTIDADE_ELIMINADOS"] = quant_eliminados_arr
    new_dataFrame["DATA_ABATE"] = arr_data_abate
    new_dataFrame["IDADE_ABATE"] = idade_abate_arr
    new_dataFrame["PM_PINTO"] = arr_peso_medio
    new_dataFrame["AVES_FALTANTES"] = aves_faltantes_arr
    new_dataFrame["PESO_MEDIO"] = peso_medio_f_arr
    new_dataFrame["GPD"]=gpd_arr
    new_dataFrame["PESO_TOTAL"] = peso_total_arr
    new_dataFrame["CAAF"] = caaf_arr
    new_dataFrame["RACAO_CONSUMIDA"] = racao_c_arr
    new_dataFrame["VALOR_KG_FRANGO"] = valor_kg_f_arr
    new_dataFrame["VALOR_KG_RACAO"] = valor_kg_racao_arr
    new_dataFrame["VALOR_DO_PINTO"] = valor_pinto_real_arr
    new_dataFrame["PERCENTUAL_BASICO"] = percentual_basico_arr
    new_dataFrame["KG_CARNE_BASE"] = carne_base_arr
    new_dataFrame["R$_BASE"] = real_base_arr
    
    
    new_dataFrame['%_AJ_ESCALA_PROD'] = aj_porcent_arr
    new_dataFrame['KG_AJ_ESCALA_PROD'] = aj_kg_arr
    new_dataFrame["R$_AJ_ESCALA_PROD"] = aj_real_arr
    
    new_dataFrame["%_SAZONALIDADE"] = aj_sazonalidade_percent_arr
    new_dataFrame["KG_SAZONALIDADE"] = aj_sazonalidade_kg_arr
    new_dataFrame["R$_SAZONALIDADE"] = aj_sazonalidade_real_arr
    
    new_dataFrame["%_AJ_SEXO_PESO"] = aj_sex_pes_percent_arr
    new_dataFrame["KG_AJ_SEXO_PESO"] = aj_sex_pes_kg_arr
    new_dataFrame["R$_AJ_SEXO_PESO"] = aj_sex_pes_real_arr
    
    new_dataFrame["%_AJ_IDADE"] = aj_idade_percent_arr
    new_dataFrame["KG_AJ_IDADE"] = aj_idade_kg_arr 
    new_dataFrame["R$_AJ_IDADE"] =  aj_idade_real_arr
    
    new_dataFrame["%_AJ_MORTALIDADE"] =  aj_mortalidade_percent_arr
    new_dataFrame["KG_AJ_MORTALIDADE"] =  aj_mortalidade_kg_arr
    new_dataFrame["R$_AJ_MORTALIDADE"] =  aj_mortalidade_real_arr

    new_dataFrame["%_CONV_ALIMENTAR"] =  aj_conv_alimentar_percent_arr
    new_dataFrame["KG_CONV_ALIMENTAR"] =  aj_conv_alimentar_kg_arr
    new_dataFrame["R$_CONV_ALIMENTAR"] =  aj_conv_alimentar_real_arr
    
    # new_dataFrame["LOTE"] = arr_pedido # nao usado

    new_dataFrame["%_AJ_MERITOCRACIA_MT"] = aj_meritocracia_mt_percent_arr
    new_dataFrame["KG_AJ_MERITOCRACIA_MT"] = aj_meritocracia_mt_kg_arr
    new_dataFrame["R$_AJ_MERITOCRACIA_MT"] = aj_meritocracia_mt_real_arr

    new_dataFrame["%_AJ_CALO_PATA_A"] = aj_calo_pata_a_percent_arr
    new_dataFrame["KG_AJ_CALO_PATA_A"] = aj_calo_pata_a_kg_arr
    new_dataFrame["R$_AJ_CALO_PATA_A"] = aj_calo_pata_a_real_arr

    new_dataFrame["%_CONDENACOES"] = condenacoes_percent_arr
    new_dataFrame["KG_CONDENACOES"] = condenacoes_kg_arr
    new_dataFrame["R$_CONDENACOES"] = codenacoes_real_arr

    new_dataFrame["%_AJ_QUALIDADE_QT"] = aj_qualidade_percent_arr
    new_dataFrame["KG_AJ_QUALIDADE_QT"] = aj_qualidade_kg_arr
    new_dataFrame["R$_AJ_QUALIDADE_QT"] = aj_qualidade_real_arr

    new_dataFrame["%_AJ_ESTRUTURAL"] = aj_estrutural_percent_arr
    new_dataFrame["KG_AJ_ESTRUTURAL"] = aj_estrutural_kg_arr
    new_dataFrame["R$_AJ_ESTRUTURAL"] = aj_estrutural_real_arr
    
    new_dataFrame["%_AJ_PROCEDIMENTOS"] = aj_procedimentos_percent_arr
    new_dataFrame["KG_AJ_PROCEDIMENTOS"] = aj_procedimentos_kg_arr
    new_dataFrame["R$_AJ_PROCEDIMENTOS"] = aj_procedimentos_real_arr

    new_dataFrame["%_AJ_PROCESSOS_PROCEDIMENTOS_PP"] = aj_processos_procedimentos_pp_percent_arr
    new_dataFrame["KG_AJ_PROCESSOS_PROCEDIMENTOS_PP"] = aj_processos_procedimentos_pp_kg_arr
    new_dataFrame["R$_AJ_PROCESSOS_PROCEDIMENTOS_PP"] = aj_processos_procedimentos_pp_real_arr

    new_dataFrame["%_RESULTADO_LOTE"] = resultado_lote_percent_arr
    new_dataFrame["KG_RESULTADO_LOTE"] = resultado_lote_kg_arr
    new_dataFrame["R$_RESULTADO_LOTE"] = resultado_lote_real_arr
    
    new_dataFrame["R$_AVE"] = ave_real_arr
    new_dataFrame["R$_TON"] = ton_real_arr
    new_dataFrame["R$_M2"] = m2_real_arr
    new_dataFrame["FUNRURAL"] = funrural_arr_f
    new_dataFrame["SENAR"] = senar_arr
    new_dataFrame["CONTA_CORRENTE"] = conta_corrente_arr
    new_dataFrame["CONTA_VINCULADA"] = conta_vinculada;
    
    new_dataFrame["CONVERSAO_ALIMENTAR_REAL"] = conv_aliment_real_arr
    new_dataFrame["CONVERSAO_ALIMENTAR_AJ"] = conv_aliment_real_aj_arr
    new_dataFrame["CONVERSAO_ALIMENTAR_PREV_AJ"] = conv_aliment_prev_aj_arr
    new_dataFrame["CONVERSAO_ALIMENTAR_DIFERENCA"] = conv_aliment_diferenca_arr
    
    # Idade de Abate REAL | PREV aj | DIFERE|
    new_dataFrame["IDADE_DE_ABATE_REAL"] = idade_de_abate_real_arr
    new_dataFrame["IDADE_DE_ABATE_PREV_AJ"] = idade_de_abate_real_prev_aj_arr
    new_dataFrame["IDADE_DE_ABATE_DIFERENCA"] = idade_de_abate_real_dif_arr

    new_dataFrame["PESO_MEDIO_REAL"] = peso_medio_f_arr
    new_dataFrame["PESO_MEDIO_PREV_AJ"] = peso_medio_prevaj_arr 
    new_dataFrame["PESO_MEDIO_DIFERENCA"] =peso_medio_diferenca_arr

    new_dataFrame["MORTALIDADE_REAL"] = mortalidade_real_arr
    new_dataFrame["MORTALIDADE_REAL_AJ"] = mortalidade_real_aj_arr
    new_dataFrame["MORTALIDADE_PREV_AJ"]  = mortalidade_prev
    new_dataFrame["MORTALIDADE_DIFERENCA"] =  mortalidade_diferenca

    new_dataFrame["%_CALO_PATA_REAL"]  =  percent_calo_real_arr
    new_dataFrame["%_CALO_PATA_PREV"] = percent_calo_prev_arr
    new_dataFrame["%_CALO_PATA_REAL_DIFERENCA"]  = percent_calo_dife_arr

    new_dataFrame["%_ARRANHADURAS_REAL"]  = percent_arranhaduras_real_arr
    new_dataFrame["%_ARRANHADURAS_PREV_AJ"]  = percent_arranhaduras_prevaj_arr
    new_dataFrame["%_ARRANHADURAS_DIFERENCA"]  = percent_arranhaduras_diferenca_arr

    new_dataFrame["%_PAPO_CHEIO_REAL"]  = percent_papo_cheio_real_arr
    new_dataFrame["%_PAPO_CHEIO_PREV"]  = percent_papo_cheio_prev_arr
    new_dataFrame["%_PAPO_CHEIO_DIFERENCA"]  = percent_papo_cheio_diferenca_arr
    
    new_dataFrame["%_CODENACAO_REAL"] = percent_codenacao_real_arr
    new_dataFrame["%_CODENACAO_PREV"] = percent_codenacao_prev_arr
    new_dataFrame["%_CODENACAO_DIFERENCA"] = percent_codenacao_diferenca_arr 
    new_dataFrame["CENTRO"] = centro_arr


    new_dataFrame.to_csv(f"{dir_s[0]}/filter_tabela_avigrand{get_date_now()}.csv", mode="w", index=False)

    print("Filtragem Completa")
    print("Arquivo filter_tabela.csv salvo na pasta ArquivosCSV!"+"\n")

    
#A funcao abaixo recebe 2 argumentos um string e um inteiro
# A string e a tabela que quer filtrar o inteiro significa a linha limite, ate que linha sera lido.

# search_term("ORGEM DO LOTE", 30, ["Pinto", "data", "Data", "10.0", "10,00"])


#A funcao recebe um argumento ARRAY
#A linha que sera filtrada com base no termo

# exemplo de uso:line_filter(["Pinto", "data", "Data", "10.0", "10,00"])

# create_table_csv(arr_filter)

separate_()


