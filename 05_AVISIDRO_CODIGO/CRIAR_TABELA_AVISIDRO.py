from math import nan
from os import remove, replace
from traceback import print_tb
import pandas as pd
import re, ast, os
from tqdm import tqdm
from datetime import datetime

def create_dirs(dirs):
    """Create directories if they don't exist."""
    for dir_ in dirs:
        if not os.path.exists(dir_):
            os.mkdir(dir_)

def get_date_now():
    date_now = datetime.now()
    d = str(date_now.strftime("""_%d_%m_%Y"""))
    return d

file=['Colunas_Criadas_CSV']
create_dirs(file)

file_execel = r"Arquivos_Extraidos_CSV/dados_extraidos_avisidro_07_03_2025.csv"

# df_2 = pd.read_csv(file_execel, encoding="utf-8", index_col=0)   
df = pd.read_csv(file_execel, encoding="utf-8")
new_dataFrame= pd.DataFrame()


def remove_last_space(s):
    return re.sub(r' +$', ' ', s).strip()

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

def remove_chars_s_points(input_str):
    chars_to_remove = ["[", "\"", "'", "nan", "]"]
    for char in chars_to_remove:
        input_str = input_str.replace(char, "")
    return input_str

def converter_para_float(numero_str):
    numero_str = numero_str.replace(',', '.')
    return float(numero_str)
            
    #ITERA SOBRE O NOVO DATA FRAME FILTRADO

def find_number_id(id_):
    # id_ = ast.literal_eval(id_)
    # id_ = id_[0]    
    padrao = r'\d+-\d+'
    id_f = re.match(padrao, str(id_))
    return id_f

def remover_duplicatas(lista):
    lista_sem_duplicatas = []
    [lista_sem_duplicatas.append(item) for item in lista if item not in lista_sem_duplicatas]
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


integrado_arr = []
integrado_nome_arr = []

id_uni = []
endereco_arr = []
municipio_arr = []
cgc_arr = []
lote_arr = []
dtma_arr = []
hma_arr = []
aa_arr = []
sexo_arr = []
linhagem_arr = []
ajs_lnhg_arr = []
psc_arr = []
cap_arr = []
car_arr = []
cra_arr = []
dp_arr = []
pf_arr = []
vrac_arr = []

pbp_percente_arr = []
pbp_kg_arr = []
pbp_real_arr = []
pcp_arr = []

avc_percente_arr = []
avc_kg_arr = []
avc_real_arr = []

acd_percent_arr = []
acd_kg_arr = []
acd_real_arr = []

acp_percent_arr = []
acp_kg_arr = []
acp_real_arr = []

acl_percent_arr = []
acl_kg_arr = []
acl_real_arr = []

rbl_percent_arr = []
rbl_kg_arr = []
rbl_real_arr = []


vrb_arr = []
vnf_arr = []
vtd_arr = []

dkm_arr =  []
adp_arr = []
qa_arr = []
dal_arr = []

qad_arr = []
qabt_arr = []

pma_arr = []
dtma_arr_d = []
prbd_arr = []
homa_arr = []
rcsmd_arr = []
pmrl_arr = []
tpo_arr = []
gpd_arr = []
pmpj_arr = []
ajs_pp_arr = []
iee_arr = []
iep_arr = []
pm_arr = []
avc_t_arr = []
pmr_pmp_arr = []
nacp_arr = []
npc_arr = []

pcpp_arr = []
mrt_prv_arr = []
pcrl_arr  = []
pcpr_arr = []
mr_arr = []
dc_pr_arr = []
dc_pr_arr = []
dcp_pr_arr = []
dpxr_arr = []
idade_abate_arr = []
viabilidade_arr = []
avcl_novo_arr = []


def dicio_obj(id:str, Data:str)  -> dict:
    return {
        "id": id,
        "Data": Data,
    }
    

def main():
    integrado_nome_arr = []
    
    print("Filtrando aguarde...")
    for index, row in df.iterrows():
        line_item = str(row.iloc[0])

        
        new_str = re.sub(r"\[|\]", "", line_item)
        new_str = remove_empty_spaces(remove_chars(new_str).split(", "))
       
        # print(new_str)
        len_newStr = len(new_str)

        if find_number_id(new_str[0]):
            id_uni.append(new_str[0])
            id_l = new_str[0]
            
        if ("Integrado" or "integrado") in str(new_str):
            n_c = len(new_str)
            
            # separe_id= remove_empty_spaces(remove_chars(str(df.loc[index+2][0])).split(", "))[0]

        # Deu a louca id_ e tipo id_integrado :#
            if n_c == 1:
                id_ = new_str[0]
                id_ = re.sub(r'\d+$', '', id_)
                # print(id_)
                line = (df.loc[(index+2)])
                # print(remove_chars(line).split(", "))
                # print(line)
                # print(separe_id)

            if n_c == 2:
                id_ =  new_str[1]
                id_ = re.sub(r'\d+$', '', id_)
                line = (df.loc[(index+2)])
                # print(remove_chars(line).split(", "))

            if n_c == 3:
                id_ = new_str[1]
                line = (df.loc[(index+2)])
                # print(remove_chars(line).split(", "))
                # print(id_)
                
            if n_c == 4:
                print("tem no 4: ", new_str)
                pass

            line_s = (remove_chars(line[0]).split(", ")[0])

            if find_number_id(line_s):
                
                s_p = (line_s+", "+id_)
                
                s_p = (remove_last_space(s_p))
                s_p_s = s_p.replace("Integrado", "").replace("integrado", "")
                # s_p_s = s_p_s.split(", ")[1]
                s_integrado_f =((s_p_s).split("-")[0].replace(" ", ""))
                integrado_nome = s_p_s.split(', ')[1].split("-")[1]
                
                idl_data = {
                    'id': s_p_s.split(', ')[0], 
                    'Data': s_integrado_f, 
                    }
                
                                
                idl_name = {
                    'id': s_p_s.split(', ')[0], 
                    'Data': integrado_nome,
                    }
                
    
                integrado_arr.append(idl_data)
                integrado_nome_arr.append(idl_name)
            
        if "Endereço" in str(new_str):
                # print(endereco_f)
            l_id = (new_str[0])
            endereco_f = (new_str[1].replace(" Técnico ", ", ").replace("Endereço ", ""))
            endereco_f = endereco_f.split(", ")[0]

            if not endereco_f:
                endereco_f = nan

            endereco_f_ = dicio_obj(l_id, str(endereco_f))
            endereco_arr.append(endereco_f_)

        if "Municipio" in str(new_str):
            l_id = (new_str[0])
            muni_separate = (new_str[1]).replace("Dist", ",").split(", ")
            muni_f = muni_separate[0].replace("Municipio ", "").replace(" ", "")
            
            muni_f = dicio_obj(l_id, muni_f)
            
            municipio_arr.append(muni_f)
         
        if "CPF/CGC" in str(new_str):
            l_id = (new_str[0])

            cgc_f = find_numbers(new_str[1])[0].replace(" ", "")
            cgc_f = dicio_obj(l_id, cgc_f)
            cgc_arr.append(cgc_f)
            
        if "Lote" and "Qtde Alojada" in str(new_str):
            l_id = (new_str[0])
            
            lote_separate = new_str[1].replace("Qtde Alojada", ",").replace("Qtde Alojada ", ",").split(", ")
            lote_s_f = lote_separate[0].replace("Lote", "").replace("Lote ", "").replace(" ", "")
            
            lote_s_f = dicio_obj(l_id, lote_s_f)
            lote_arr.append(lote_s_f)

        if "Data Média Alojto" in str(new_str):
            l_id = (new_str[0])
            
            dtma_f = new_str[1].replace("Peso Médio Alojado", ",").split(", ")[0].replace("Data Média Alojto ", "").replace("Data Média Alojto", "")
            dtma_f = dicio_obj(l_id, str(dtma_f))
            dtma_arr.append(dtma_f)
            
        if "Hora Média Alojto" in str(new_str):
            l_id = (new_str[0])
            
            hma_f = remove_chars_s_points(line_item).replace("Hora média Abate:", ",").split(", ")[1].replace("Hora Média Alojto:", "").replace(" ", "")
            hma_f = dicio_obj(l_id, str(hma_f))
            
            hma_arr.append(hma_f)
            
        if "Área Alojada" in str(new_str):
            l_id = (new_str[0])
            
            arl_s = new_str[1].replace("Área Alojada ", "").replace("Área Alojada", "")
            arl_s = dicio_obj(id_l, arl_s)
            
            aa_arr.append(arl_s)
            
        if "Sexo" in str(new_str):
            l_id = (new_str[0])
            
            sexo_s = new_str[1].replace(" Idade Abate ", ", ").replace(" Idade Abate", ",")
            sexo_s = sexo_s.split(", ")[0].replace("Sexo", "").replace("Sexo ", "").replace(" ", "")
            
            sexo_s = dicio_obj(l_id, sexo_s)
            sexo_arr.append(sexo_s)
            
        if "Linhagem" in str(new_str):
            l_id = (new_str[0])
            
            lnh_s = new_str[1].replace("Tipo Produto", ", ").split(",")[0]
            lnh_s = lnh_s.replace("Linhagem ", "").replace("Linhagem", "")
            lnh_s = dicio_obj(l_id, str(lnh_s))
            
            linhagem_arr.append(lnh_s)
            
        if "Ajs Lnhg" in str(new_str):
            l_id = (new_str[0])
            
            ajs_s = new_str[1].replace("Ajs Peso Pinto", ",").split(", ")[0]
            ajs_s = ajs_s.replace("Ajs Lnhg. ", "").replace("Ajs Lnhg.", "")
            ajs_s = converter_para_float(ajs_s)
            
            ajs_s = dicio_obj(l_id, str(ajs_s))
            
            ajs_lnhg_arr.append(ajs_s)
            
        if "Prev Semanal de Conv." in str(new_str):
            l_id = new_str[0]
            prv_sema_f = new_str[1].replace("No Aves Condenadas", ",").split(", ")[0].replace("Prev Semanal de Conv. ", "")
            prv_sema_f = dicio_obj(l_id, prv_sema_f)
            psc_arr.append(prv_sema_f)
            
        if "Conv. Ajustada Prev" in str(new_str):
            l_id = new_str[0]
                    
            cavp_ = new_str
            cavp_ = (cavp_[1].replace("No Aves Condenadas Parcial", ", ").split(", ")[0].replace("Conv. Ajustada Prev", "").replace(" ", ""))
            cavp_ = dicio_obj(l_id, cavp_)
            cap_arr.append(cavp_)
            
        if "Conv. Alimentar Real" in str(new_str):
            l_id = new_str[0]
            
            car_f = new_str[1].replace("Pc Condenações - Previsto ", ", ").split(", ")[0].replace("Conv. Alimentar Real", "").replace(" ", "")
            car_f = dicio_obj(l_id, car_f)
            car_arr.append(car_f)
          
        if "Conv. Real Ajustada" in str(new_str):
            l_id = new_str[0]
            
            cra_f =new_str[1].replace("Pc Condenações - Real", ", ").split(", ")[0].replace("Conv. Real Ajustada", "").replace(" ", "")
            cra_f = dicio_obj(l_id, cra_f)
            cra_arr.append(cra_f)
            
        if "Difça (PrevXReal)" in str(new_str):
            l_id = new_str[0]
            
            dp_f = (new_str[1].replace("Difça Cond (Prev-Real)", ", ").split(", ")[0].replace("Difça (PrevXReal)", "").replace(" ", ""))
            dp_f = dicio_obj(l_id, dp_f)
            dp_arr.append(dp_f)
            
        if "PF - Preço do Kg do Frango" in str(new_str):
            l_id = new_str[0]
            
            pf_f = new_str[1].replace("PF - Preço do Kg do Frango", "").replace(" ", "")
            pf_f = dicio_obj(l_id, pf_f)
            pf_arr.append(pf_f)
            
        if "VRac - Vlr das Rações" in str(new_str):
            # print(new_str)
            l_id = new_str[0]
            
            vrac_f = new_str[1].replace("% Kg R$ R$/Cab", "").replace("VRac - Vlr das Rações", "").replace(" ", "")
            vrac_f = vrac_f.replace("%KgR$", "").replace("%", "")
            
            vrac_f = dicio_obj(l_id, vrac_f)
            vrac_arr.append(vrac_f)
            
        if "Percentual Básico de Partilha" in str(new_str) or "Percentual Basico de Partilha" in str(new_str):

            pbp_sep = remove_empty_spaces(new_str)
            n_c = len(pbp_sep)
            
            pbp_sep = remove_empty_spaces(pbp_sep[1].replace("Percentual Básico de Partilha", "").split(" "))
            l_id = new_str[0]
            

            if n_c == 2:
                # # %
                pbp_sep_s = (new_str[1].replace("Percentual Básico de Partilha ", "").split(" "))
                # print(pbp_sep_s[0])
                pbp_sep_s = pbp_sep_s[0]
                # print(new_str)
            
                # # kg
                pbp_sep_k = (new_str[1].replace("Percentual Básico de Partilha ", "").split(" "))
                pbp_sep_k_s_ = (pbp_sep_k[1])
                
                # # $
                pbp_sep_r = (new_str[1].replace("Percentual Básico de Partilha ", "").split(" "))
                pbp_sep_r_s = (pbp_sep_r[2])
                

            if n_c == 3:
                 # # %
                pbp_sep_s2 = (new_str[1].replace("Percentual Básico de Partilha ", "").split(" "))
                pbp_sep_s = pbp_sep_s2[0]

                # # kg
                pbp_sep_k_s_ = pbp_sep_s2[1]

                # # $
                pbp_sep_r_s = pbp_sep_s2[2]
            

            if n_c == 4:
                # # %
                pbp_sep_s3 = new_str[1].replace("Percentual Básico de Partilha ", "")
                pbp_sep_s = pbp_sep_s3
                
                # # KG
                # Se caso der b.o preste ATENCAO TOTAL n_c

                n_c = (new_str[2].split(" "))
                pbp_sep_k_s2 = (new_str[2].split(" "))

                if len(n_c) == 2:
                    pbp_sep_k_s_ = (pbp_sep_k_s2[0])

                if len(n_c) == 3:
                    pbp_sep_k_s_ = (pbp_sep_k_s2[1])

                if "Percentual Básico de Partilha" in pbp_sep_s3:                    
                    pbp_sep_s = (new_str[2].split(" ")[0])

            
            

            # # %
            pbp_sep_s_ = dicio_obj(id_l, pbp_sep_s)
            pbp_percente_arr.append(pbp_sep_s_)

            # # kg
            pbp_sep_k_s__ = dicio_obj(id_l, pbp_sep_k_s_)
            pbp_kg_arr.append(pbp_sep_k_s__)
            
            # # $
            pbp_sep_r_s_ = dicio_obj(id_l, pbp_sep_r_s)
            pbp_real_arr.append(pbp_sep_r_s_)
            
            # print("Quant. itens: ", len(pbp_real_arr))

        if "Avaliação Conversão" in str(new_str):
            avc_f = remove_empty_spaces(new_str[1].replace("Avaliação Conversão", "").split(" "))
            l_id = new_str[0]
            n_c = len(avc_f)

            if n_c == 3:
                # # %
                avc_percent_f=(avc_f[0])

                 # # kg
                avc_kg_f=(avc_f[1])
           
                 # # $
                avc_real_f=(avc_f[2])

            if n_c == 4:

                # # %
                avc_percent_f=(avc_f[0])
                
                 # # kg
                avc_kg_f=(avc_f[1])

                 # # $
                avc_real_f=(avc_f[2])
            
            # d_j = {"id":l_id, "Data":avc_percent_f}
           
            
            # avc_percent_f = dicio_obj(l_id, avc_percent_f)
            d_j = dicio_obj(l_id, avc_percent_f)
            avc_kg_f_ = dicio_obj(l_id, avc_kg_f)
            avc_real_f_ = dicio_obj(l_id, avc_real_f)
            
            avc_percente_arr.append(d_j)
            
            avc_kg_arr.append(avc_kg_f_)           
            avc_real_arr.append(avc_real_f_)
            
        if "Avaliação Condenação" in str(new_str):
            acd_s = remove_empty_spaces(new_str[1].replace("Avaliação Condenação", "").split(" "))
            l_id = new_str[0]
            
            n_c = len(acd_s)

            if n_c == 3:
            # # %           
                acd_p_f = acd_s[0]

            # # kg
                acd_kg_f = acd_s[1]
            
            # # $
                acd_r_f = acd_s[2]
                
            if n_c == 4:
                # # %                 
                acd_p_f = acd_s[0]

                # #  kg
                acd_kg_f = acd_s[1]
                
                # # $
                acd_r_f = acd_s[2]
            
            acd_p_f_ = dicio_obj(l_id, acd_p_f)
            acd_kg_f_= dicio_obj(l_id, acd_kg_f)
            acd_r_f_= dicio_obj(l_id, acd_r_f)
            
            acd_percent_arr.append(acd_p_f_)
            # # kg
            acd_kg_arr.append(acd_kg_f_)
            # # $
            acd_real_arr.append(acd_r_f_)
            
        if "Avaliação Calo de Patas" in str(new_str):
            
            acp_sep = remove_empty_spaces(new_str[1].replace("Avaliação Calo de Patas", "").split(" "))
            l_id = new_str[0]
            
            nc_ = (len(acp_sep))
            if nc_ == 3:

                # %
                acp_p_f = acp_sep[0]

                #kg
                acp_kg_f  = acp_sep[1]

                # $
                acp_r_f  = acp_sep[2]

            if nc_ == 4:

                # %
                acp_p_f = acp_sep[0]

                #kg
                acp_kg_f  = acp_sep[1]

                # $
                acp_r_f  = acp_sep[2]
                           
            # %
            acp_p_f_ = dicio_obj(l_id, acp_p_f)
            acp_percent_arr.append(acp_p_f_)
            # # kg
            acp_kg_f_ = dicio_obj(l_id, acp_kg_f)
            acp_kg_arr.append(acp_kg_f_)
            # # $
            acp_r_f_ = dicio_obj(l_id, acp_r_f)
            acp_real_arr.append(acp_r_f_)
            
        if "Avaliação Check-List" in str(new_str):
            avcl_n = "nan"
            avc_cl_separate = new_str
            # print(avc_cl_separate)
            n_c1 =  len(new_str)
            l_id = new_str[0]

            #      # $
            #     acl_real_f = acl_sep[3]

            if n_c1 == 2:

                avc_cl_separate = (remove_empty_spaces(new_str[1].replace("Avaliação Check-List", "").split(" ")))
                
                n_c2 = len(avc_cl_separate)

                if n_c2 == 4:
                     # # %
                    acl_percent = (avc_cl_separate[0])

                    # # KG
                    acl_kg_f = avc_cl_separate[1] 
                    
                    # # $
                    acl_r_f = avc_cl_separate[2]

                if n_c2 == 5:
                     # # NOVO
                    avcl_n = avc_cl_separate[0]
                     # # %
                    acl_percent = avc_cl_separate[1]
                      # # KG
                    acl_kg_f = avc_cl_separate[2] 
                    # # $
                    acl_r_f = avc_cl_separate[3]

            if n_c1 == 3:
                avc_cl_separate_ = (remove_empty_spaces(new_str[1].replace("Avaliação Check-List", "").split(" ")))
                
                n_c2 = len(avc_cl_separate_)

                if n_c2 == 3:
                    # # %
                    acl_percent = avc_cl_separate_[0]
                    
                      # # KG
                    acl_kg_f = avc_cl_separate_[1] 
                    
                    # # $
                    acl_r_f = avc_cl_separate[2]

                if n_c2 == 4:
                    # # NOVO
                    avcl_n = (avc_cl_separate_[0])

                    # # %
                    acl_percent = avc_cl_separate_[1]
                    # # KG
                    acl_kg_f = avc_cl_separate_[2] 
                    
                    # # $
                    acl_r_f = avc_cl_separate_[3]

            if n_c1 == 4:
                avc_cl_separate__ = remove_empty_spaces(new_str)
                avc_cl_separate__n  = avc_cl_separate__[1].split("Avaliação Check-List")

                
                # # NOVO
                avc_cl_separate__n = (remove_empty_spaces(avc_cl_separate__n))
                if not avc_cl_separate__n:
                    avcl_n = nan
                else:
                    avcl_n = remove_empty_spaces(avc_cl_separate__n[0].split(" "))
                    avcl_n = avcl_n[0]
                    

                # # %
                acl_percent_s = remove_empty_spaces(avc_cl_separate__[1].split("Avaliação Check-List"))
                # acl_percent_s = remove_empty_spaces(acl_percent_s[0].split(" "))
                n_c2 = len(acl_percent_s)
                
                if len(acl_percent_s) == 1:
                    acl_percent = new_str[1]
                    acl_percent = remove_empty_spaces(acl_percent.split("Avaliação Check-List"))
                    acl_percent = remove_empty_spaces(acl_percent[0].split(' '))
                    acl_percent = acl_percent[-1]
                    # % 
                    if "Novo" in acl_percent:
                        acl_percent = (new_str[2].split(" ")[0])
                    # acl_percent_s = acl_percent[0]
                    
                # # KG
                acl_kg_f = avc_cl_separate__
                acl_kg_f = acl_kg_f[2].split(" ")
                acl_kg_f = acl_kg_f[0]

                if len(acl_kg_f) == 3:
                   acl_kg_f = acl_kg_f[1]

                # # $
                acl_r_f_s = avc_cl_separate__
                acl_r_f_s = (acl_r_f_s[2].split(" "))
                acl_r_f = acl_r_f_s[1]
                

                if len(acl_r_f_s) == 3:
                    acl_r_f =  acl_r_f_s[-1]

            
            # if n_c1 == 5:
            #     print(new_str)
            #     pass.a
            avcl_n_ = dicio_obj(l_id, avcl_n)
            acl_percent_ = dicio_obj(l_id, acl_percent)
            acl_kg_f_ = dicio_obj(l_id, acl_kg_f)
            acl_r_f_ = dicio_obj(l_id, acl_r_f)
            
            avcl_novo_arr.append(avcl_n_)
            acl_percent_arr.append(acl_percent_)
            acl_kg_arr.append(acl_kg_f_)
            acl_real_arr.append(acl_r_f_)
        
        if "Resultado Bruto do Lote" in str(new_str):
            rbl_s = remove_empty_spaces(new_str[1].replace("Resultado Bruto do Lote", "").split(" "))
            
            l_id = new_str[0]
            
            n_c1 = len(rbl_s)

            if n_c1 == 3:
                # # %
                rbl_p_f = rbl_s[0]


                # # kg
                rbl_s_kg = rbl_s[1]
                
                # #$
                rbl_s_r = (rbl_s[2])


            if n_c1 == 4:
                # # %
                rbl_p_f = rbl_s[0]

                # # kg
                rbl_s_kg = rbl_s[1]

                #  # $
                rbl_s_r = (rbl_s[2])

            rbl_p_f_ = dicio_obj(l_id, rbl_p_f)
            rbl_s_kg_ = dicio_obj(l_id, rbl_s_kg)
            rbl_s_r_ = dicio_obj(l_id, rbl_s_r)
            
            
            rbl_percent_arr.append(rbl_p_f_)
            rbl_kg_arr.append(rbl_s_kg_)
            rbl_real_arr.append(rbl_s_r_)
        
        if "Valor Renda Bruta" in str(new_str):
            l_id = new_str[0]
            
            vrb_s = remove_empty_spaces(new_str[1].replace("Valor Renda Bruta", "").split(" "))
            n_c = len(vrb_s)
            
            if n_c == 1:
                vrb_f = (vrb_s[0])
                
            if n_c == 2:
                vrb_f = (vrb_s[0])         
            
            vrb_f_ = dicio_obj(l_id, vrb_f)
            vrb_arr.append(vrb_f_)
            
        if "Valor Total a Depositar" in str(new_str):
            vtd_s = remove_empty_spaces(new_str[1].replace("Valor Total a Depositar", "").split(" "))
            l_id = new_str[0]
            n_c = len(vtd_s)
            
            if n_c == 1:
                vtd_f = vtd_s[0]
                
            if n_c == 2:
                vtd_f = vtd_s[0]
                
            
            vtd_f_ = dicio_obj(l_id, vtd_f)
            vtd_arr.append(vtd_f_)
            
        if "Valor NF" in str(new_str):
            l_id = new_str[0]
            
            nf_f_s = None
            n_c1 = len(new_str)
            l_id = new_str[0]
            

            if n_c1 == 2:
                
                nf_f_s = remove_empty_spaces(new_str[1].split("Valor NF"))[0].split(" ")
                nf_f_s = remove_empty_spaces(nf_f_s)
                nf_f_s = nf_f_s[0]

            nf_f_s_ = dicio_obj(id_l, nf_f_s)
            vnf_arr.append(nf_f_s_)

        if "Dist Km" in str(new_str):
            l_id = new_str[0]
            dk_s = remove_empty_spaces(new_str)[1]
            # n_c1 =  len(dk_s)
            dk_s_f = dk_s.split("Dist Km")[-1].replace(" ", "")
            dk_s_f = dk_s_f.replace("NoGranja", ", ")
            dk_s_f = dk_s_f.split(", ")
            dk_s_f = dk_s_f[0]
            if not dk_s_f:
                dk_s_f = (new_str[2]).replace(" ", "")

            # dkm_f = dk_s[0]
            dk_s_f_ = dicio_obj(l_id, dk_s_f)
            dkm_arr.append(dk_s_f_)
            
        if "Área disp" in str(new_str):
            l_id = new_str[0]
            
            adp_f = (new_str[1].split("Área disp")[-1])
            
            adp_f_ = dicio_obj(l_id, adp_f)
            adp_arr.append(adp_f_)
            
        if "Qtde Alojada" in str(new_str):
            l_id = new_str[0]
            qa_s_f = (new_str[1].split("Data Acerto do Lote")[0].split("Qtde Alojada")[-1])
            qa_s_f_ = dicio_obj(id_l, qa_s_f)
            qa_arr.append(qa_s_f_)
            
        if "Data Acerto do Lote" in str(new_str):
            l_id = new_str[0]
            dal_s_f = (remove_empty_spaces(new_str))

            dal_s_f = dal_s_f[1].split("Data Acerto do Lote")
            dal_s_f = (dal_s_f[-1])
            dal_s_f = remove_empty_spaces(dal_s_f.split(" "))
            
            if dal_s_f:
               dal_s_f = (dal_s_f[0].replace(" ", ""))
               
            if not dal_s_f:
                dal_s_f = (new_str[2])

            dal_s_f_ = dicio_obj(l_id, dal_s_f)
            dal_arr.append(dal_s_f_)
        
        # # "Qtde Abatida"

        if "Qtde Abatida" and "Data Acerto do Lote" in str(new_str):
            l_id = new_str[0]
            n_c = len(new_str)
            if n_c == 2:
                qabt_f = new_str[1].split("Qtde Abatida")[-1].replace(" ", "")

            if n_c == 3:
                qabt_f = new_str[-1].replace(" ", "").replace("QtdeAbatida", "")            
            
            qabt_f_ = dicio_obj(l_id, str(qabt_f))
            qabt_arr.append(qabt_f_)
        
        if "Peso Médio Alojado" in str(new_str):
            l_id = new_str[0]
            pma_s_f = (new_str[1].split("Peso Médio Alojado")[-1].split("Data Média Abate")[0]).replace(" ", "")
            pma_s_f_ = dicio_obj(l_id, pma_s_f)
            pma_arr.append(pma_s_f_)
            
        if "Data Média Abate" in str(new_str):
            l_id = new_str[0]
            dmab_s_ = remove_empty_spaces(new_str)
            dmab_s_f = (dmab_s_[1].split("Data Média Abate"))
            
            nc1 = len(dmab_s_)

            if nc1 == 1:
                dmab_s_f = (dmab_s_f[2])
                # print(dmab_s_f)

            if nc1 == 2:
                dmab_s_f = (remove_empty_spaces(dmab_s_[1].split("Data Média Abate")[1].split(" ")))
                dmab_s_f = dmab_s_f[0]
            
            if nc1 == 3:

                dmab_s__ = (dmab_s_[1].split("Data Média Abate")[-1])
                dmab_s_f = dmab_s__.replace("Peso Recebido", "").replace(" ", "")

            if nc1 == 4:
                dmab_s_f = (dmab_s_[2].replace("Data Média Abate", "").replace(" ", ""))

            dmab_s_f_ = dicio_obj(l_id, dmab_s_f)
            dtma_arr_d.append(dmab_s_f_)

        if "Peso Recebido" in str(new_str):
            l_id = new_str[0]
            
            n_c = len(new_str)
            if n_c == 2:
                prbd_s_f = remove_empty_spaces(new_str[1].split("Peso Recebido"))[-1]
                               
            if n_c == 3:
                prbd_s_f = (new_str[-1]).replace("Peso Recebido", "").replace(" ", "")
                
            if n_c == 4:
                prbd_s_f = (new_str[-1].replace("Peso Recebido", "").replace(" ", ""))

            prbd_s_f_ = dicio_obj(l_id, prbd_s_f)
            prbd_arr.append(prbd_s_f_)
            
        if "Hora média Abate" in str(new_str):
            l_id = new_str[0]
            n_c_ = len(new_str[1:])
            
            if n_c_ == 1:
                s_ = (remove_chars_s_points(line_item).split("Hora média Abate:")[-1].split(" "))
                hmabt_f = remove_empty_spaces(s_)
                hmabt_f =hmabt_f[0]
                
            if n_c_ == 2:
                s_d = remove_empty_spaces(remove_chars_s_points(line_item).split(", ")[1].split("Hora média Abate:")[-1].split(" "))
                hmabt_f = s_d[0]
                # homa_arr.append(hmabt_f) 
                pass
            
            if n_c_ == 3:
                # print(remove_chars_s_points(line_item))
                # hmabt_f = s_d[0])
                pass
            # if n_c_ == 4:
            #     pass
            # if n_c_ == 5:
            #     # print(line_item)
            #     pass
            hmabt_f_ = dicio_obj(l_id, hmabt_f)
            homa_arr.append(hmabt_f_)
            
        if "Ração Consumida" in str(new_str):
            l_id = new_str[0]
            nc_ = len(new_str)
            if nc_ == 2:
                rcsmd_s_f = (new_str[1].split("Ração Consumida")[-1].replace(" ", ""))
                
            if nc_ == 3:
                rcsmd_s_f = (new_str[-1].split("Ração Consumida")[-1].replace(" ", ""))
                # print(rcsmd_s_f)
                
            if nc_ == 4:
                rcsmd_s_f = (remove_empty_spaces(new_str[-1].split("Ração Consumida"))[0]).replace(" ", "")
                
            # if nc_ == 5:
            #     print(new_str)
            # rcsmd_s
            rcsmd_s_f_ = dicio_obj(l_id, rcsmd_s_f)
            rcsmd_arr.append(rcsmd_s_f_)
        
        if "Peso Médio Real" in str(new_str):
            l_id = new_str[0]
            nc_ = len(new_str)
            # if nc_ == 2:
            #     print(new_str)
            #     pass
            if nc_ == 3:
                pmrl_s_f = (new_str[-1].replace("Peso Médio Real", "").replace(" ", ""))
                
            if nc_ == 4:
                pmrl_s_f = (new_str[-1].replace("Peso Médio Real", "").replace(" ", ""))
            # if nc_ == 5:
            pmrl_s_f_ = dicio_obj(l_id, pmrl_s_f)
            pmrl_arr.append(pmrl_s_f_)
            
        
        if "Tipo Produto" and "Linhagem" in str(new_str):
            l_id = new_str[0]
            tpo_s_f = (remove_empty_spaces(new_str[1].split("Tipo Produto")[1].split(" "))[0])
            tpo_s_f_ = dicio_obj(l_id, tpo_s_f)
            tpo_arr.append(tpo_s_f_)
            
        if "GPD" and "Linhagem" in str(new_str):
            gpd_s_f = (remove_empty_spaces(new_str))
            l_id = new_str[0]
            n_c__ = len(gpd_s_f)

            if n_c__ == 3:
                gpd_f = remove_empty_spaces(gpd_s_f[1].split("GPD")[1].split(" "))
                gpd_f = (gpd_f[0])

            if n_c__ == 4:
                
                if (find_numbers(new_str[2].split(" ")[1])):
                   gpd_f = (new_str[2].split(" ")[1])
                   
                if len(new_str[1].split("GPD")) == 2:
                    gpd_f = (new_str[1].split("GPD")[1])
                
            # print(n_c__)
            # if n_c__ == 6:
            #     print(gpd_s_f)
            #     pass
            # print()
            gpd_f_ = dicio_obj(l_id, gpd_f)
            gpd_arr.append(gpd_f_)
        
        if "Peso Médio Projetado" in str(new_str):
            l_id = new_str[0]
            nc_ = len(new_str)
            if nc_ == 2:                
                pass            
            if nc_ == 3:
                pmpj_s_f = (new_str[-1].replace("Peso Médio Projetado", "").replace(" ", ""))
                
            if nc_ == 4:
                pmpj_s_f = (new_str[-1].replace("Peso Médio Projetado", "").replace(" ", ""))
            
            pmpj_s_f_ = dicio_obj(l_id, pmpj_s_f)
            pmpj_arr.append(pmpj_s_f_)

        if "Ajs Peso Pinto" in str(new_str):
            l_id = new_str[0]
            ajs_pp_s_f= (remove_empty_spaces(new_str[1].split("Ajs Peso Pinto")[-1].split(" "))[0])
            ajs_pp_s_f_ = dicio_obj(l_id, ajs_pp_s_f)
            
            ajs_pp_arr.append(ajs_pp_s_f_)
            
        if "IEE" in str(new_str):
            l_id = new_str[0]
            iee_s_f = remove_empty_spaces(new_str[1].split("IEE")[-1].split(" "))[0]
            iee_s_f_ = dicio_obj(l_id, iee_s_f)
            iee_arr.append(iee_s_f_)
            
        if "IEP" in str(new_str):
            l_id = new_str[0]
            iep_s_f = remove_empty_spaces((new_str[1].split("IEP"))[-1].split(" "))[0]
            iep_s_f_ = dicio_obj(l_id, iep_s_f)
            iep_arr.append(iep_s_f_)
            
        if "PM Real - PM Projetado" in str(new_str):
            l_id = new_str[0]
            pm_s = dicio_obj(l_id, new_str[-1])
            
            pm_arr.append(pm_s)
            
        if "Aves Condenadas Total" in str(new_str):
           avc_t_s_f =  remove_empty_spaces(new_str[1].split("No Aves Condenadas Total"))[-1].replace(" ","")
           avc_t_arr.append(avc_t_s_f)
        
        if "PM Real - PM Projetado" in str(new_str):
            pmr_pmp = new_str[-1]
            # print(pmr_pmp)
            pmr_pmp_ = dicio_obj(l_id, pmr_pmp)
            pmr_pmp_arr.append(pmr_pmp_)
            
        if "No Aves Condenadas Parcial" in str(new_str):
            l_id = new_str[0]
            nacp_s_f = remove_empty_spaces(new_str)
            n__c = len(nacp_s_f)

            if n__c == 3:
                nacp_f = (remove_empty_spaces(nacp_s_f[1].split("No Aves Condenadas Parcial")[1].split(" "))[0].replace(" ",""))

            if n__c == 4:
                nacp_f = (nacp_s_f[1].split("No Aves Condenadas Parcial")[-1])
                if not nacp_f:
                    nacp_f = remove_empty_spaces(nacp_s_f[2].split("No de Patas Condenadas"))[0]

            # if n__c == 5:
            #     print(nacp_s_f)
            #     pass
            nacp_f_ = dicio_obj(l_id, nacp_f)
            nacp_arr.append(nacp_f_)
            
            
        if "No de Patas Condenadas" in str(new_str):
            l_id = new_str[0]
            npc_s_f = (remove_empty_spaces(new_str))
            nc_ = len(npc_s_f)
            if nc_ == 3:
                npc_f = (npc_s_f[1].split("No de Patas Condenadas")[-1].replace("Viabilidade", "").replace(" ", ""))
                if not npc_f:
                    npc_f = (npc_s_f[-1].split("Viabilidade")[0].replace(" ", ""))

            if nc_ == 4:
                npc_f = (npc_s_f[2].split("No de Patas Condenadas")[0])
                if not npc_f:
                    if find_numbers(npc_s_f[-1].split(" ")[0]):
                        npc_f = (npc_s_f[-1].split(" ")[0])
            npc_f_ = dicio_obj(l_id, npc_f)
            npc_arr.append(npc_f_)
            
        if "Pc Condenações - Previsto" in str(new_str):
            l_id = new_str[0]
            pcp_s_f = (remove_empty_spaces(new_str))
            pcp_s_f = (pcp_s_f[1].split('Pc Condenações - Previsto')[1]).split(" ")
            pcp_s_f = remove_empty_spaces(pcp_s_f)
            nc3 = (len(pcp_s_f[:1]))
            if nc3 == 0:
                pcp_s_f = (remove_empty_spaces(new_str)[2]).split(" ")[0]
            if nc3 == 1:
                pcp_s_f = (pcp_s_f[0])
            
            pcp_s_f_ = dicio_obj(l_id, pcp_s_f)
            pcp_arr.append(pcp_s_f_)
            
        if "Pc Calo de Pata - Previsto" in str(new_str):
            l_id = new_str[0]
            pcpp_s = remove_empty_spaces(new_str)
            pcpp_s = pcpp_s[1].split("Pc Calo de Pata - Previsto")
            pcpp_s = remove_empty_spaces(pcpp_s)
            pcpp_f = (remove_empty_spaces(pcpp_s[-1].split(" "))[0]).replace(" ", "")
            pcpp_f_ = dicio_obj(l_id, pcpp_f)
            pcpp_arr.append(pcpp_f_)
            
        if "Mort. Prev" in str(new_str):
            l_id = new_str[0]
            # mrt_prv_s_f =  (remove_empty_spaces(new_str[-1])
            mrt_prv_s_f  = (remove_empty_spaces(new_str[-1].split(" "))[-1])
            mrt_prv_s_f_ = dicio_obj(l_id, mrt_prv_s_f)
            mrt_prv_arr.append(mrt_prv_s_f_)
            
            
        if "Pc Condenações - Real" in str(new_str):
            l_id = new_str[0]
            
            pcrl_s_f = remove_empty_spaces(new_str)[1]
            pcrl_s_f = remove_empty_spaces((pcrl_s_f.split("Pc Condenações - Real")[-1]).split(" "))[0:1]

            if len(pcrl_s_f) != 0:
                pcrl_s_f = pcrl_s_f[0]
            else:
                pcrl_s_f = (remove_empty_spaces(new_str)[2].split(" ")[0])
                
            pcrl_s_f_ = dicio_obj(l_id, pcrl_s_f)
            pcrl_arr.append(pcrl_s_f_)

            
        if "Pc Calo de Pata - Real" in str(new_str):
            l_id = new_str[0]
            pcpr_s_f = remove_empty_spaces(new_str)
            pcpr_s_f = (pcpr_s_f[1].split("Pc Calo de Pata - Real"))
            pcpr_s_f =  remove_empty_spaces(pcpr_s_f[-1].split(" "))
            
            pcpr__f =  remove_empty_spaces(pcpr_s_f)
            # print(pcpr__f)
            n_c = len(pcpr__f)

            if n_c == 0:
                if find_numbers(remove_empty_spaces(new_str)[-1].split(" ")[0]):
                    pcpr__f = (remove_empty_spaces(new_str)[-1].split(" ")[0])
                   
                else:
                    pcpr__f = (remove_empty_spaces(new_str)[(-1-1)])
                    
            if n_c == 1:
                pcpr__f = (pcpr__f[0])

            if n_c == 3:
                pcpr__f = (pcpr__f[0])

            if n_c == 8 or  n_c == 9 :
                if  find_numbers(str(new_str[-1].split(" ")[0])):
                    pcpr__f = (new_str[-1].split(" ")[0])
                    
                else:
                    pcpr__f = (new_str[(-1-1)].split(" ")[-1])

            pcpr__f_ = dicio_obj(l_id, pcpr__f)
            pcpr_arr.append(pcpr__f_)
            
        
        if "Mort. Real" in str(new_str):
            l_id = new_str[0]
            n_c_ = len(new_str)
            # print(n_c_)
            if n_c_ == 3:
                mr_s_f = remove_empty_spaces(new_str[-1].split("Mort. Real"))[0].replace(" ", "")
                
            elif n_c_ == 4:
                mr_s_f = (new_str[-1].split("Mort. Real")[1].replace(" ", ""))
            mr_s_f_ = dicio_obj(l_id, mr_s_f)
            mr_arr.append(mr_s_f_)

        if "Difça Cond (Prev-Real)" in str(new_str):
            l_id = new_str[0]
            dc_pr_s_f = remove_empty_spaces(new_str)
            nc4 = len(dc_pr_s_f)

            if nc4 == 3:
                dc_pr_s_f = remove_empty_spaces(dc_pr_s_f[1].split("Difça Cond (Prev-Real)")[-1].split(" "))
                dc_pr_s_f = (dc_pr_s_f[0].replace(" ", ""))

            if nc4 == 4:
                dc_pr_s_f = (dc_pr_s_f[1].split("Difça Cond (Prev-Real)")[-1].replace("Difça Calo Pata (Prev-Real)", "").replace(" ", ""))
                if not dc_pr_s_f:
                    dc_pr_s_f = ((new_str)[2].split(" ")[0])
                    
            dc_pr_s_f_ =  dicio_obj(l_id, dc_pr_s_f)
            dc_pr_arr.append(dc_pr_s_f_)
        
        
        if "Difça Calo Pata (Prev-Real)" in str(new_str):
            l_id = new_str[0]
            dcp_s_f = remove_empty_spaces(new_str)
            nc5 = len(dcp_s_f)

            if nc5 == 3:
                dcp_f =  (dcp_s_f[1].split("Difça Calo Pata (Prev-Real)")[-1].replace("Difça (PrevXReal)", " "))
                if not dcp_f:
                    dcp_f = (dcp_s_f[-1].split(" ")[0])

            if nc5 == 4:
                dcp_f = (remove_empty_spaces(dcp_s_f[-1].replace("Difça (PrevXReal)", "").split(" "))[0])

            if nc5 == 5:
                pass
            
            dcp_f_ = dicio_obj(l_id, dcp_f)
            dcp_pr_arr.append(dcp_f_)
            
        if "Difça (PrevXReal)" in str(new_str):
            l_id = new_str[0]
            dpxr_s_f = remove_empty_spaces(new_str[1].split("Difça (PrevXReal)")[1].split(" "))[0]
            dpxr_s_f_ = dicio_obj(l_id, dpxr_s_f)
            dpxr_arr.append(dpxr_s_f_)
            

        if "Idade Abate" in str(new_str):
            l_id = new_str[0]
            idade_abate = remove_empty_spaces(new_str[1].split("Idade Abate")[-1].split(" "))[0].replace(" ", "")
            idade_abate_ = dicio_obj(l_id, idade_abate)
            
            idade_abate_arr.append(idade_abate_)

        if "Viabilidade" in str(new_str):
            l_id = new_str[0]
            n_c = len(new_str)
            # if n_c == 2:
            #     print(new_str)
            if n_c == 3:
                vbd_f = remove_empty_spaces(new_str[-1].replace("Viabilidade", "").split(" "))[-1]
                

            if n_c == 4:
                vbd_f = (new_str[-1].split("Viabilidade")[1].replace(" ", ""))
                
            # if n_c == 5:
            #     print(new_str)
            vbd_f_ = dicio_obj(l_id, vbd_f)
            viabilidade_arr.append(vbd_f_)
    
    # integrado_id_arr = remover_duplicatas(integrado_id_arr)
    # integrado_nome_arr = [x.split("-")[2] for x in integrado_arr_f ]
            
    # print(len(integrado_nome_arr))

    id_uni_f = remover_duplicatas(id_uni)
    integrado_id_arr = processar_dicionarios(id_uni_f ,integrado_arr)
    integrado_nome_arr =  processar_dicionarios(id_uni_f, integrado_nome_arr)
    endereco_arr_ = processar_dicionarios(id_uni_f, endereco_arr)
    municipio_arr_ = processar_dicionarios(id_uni_f, municipio_arr)
    cgc_arr_ = processar_dicionarios(id_uni_f, cgc_arr)
    lote_arr_ = processar_dicionarios(id_uni_f, lote_arr)
    dtma_arr_ = processar_dicionarios(id_uni_f, dtma_arr)
    hma_arr_ = processar_dicionarios(id_uni_f, hma_arr)
    aa_arr_ = processar_dicionarios(id_uni_f, aa_arr)
    sexo_arr_ = processar_dicionarios(id_uni_f, sexo_arr)
    linhagem_arr_ = processar_dicionarios(id_uni_f, linhagem_arr)
    ajs_lnhg_arr_ = processar_dicionarios(id_uni_f, ajs_lnhg_arr)
    psc_arr_ = processar_dicionarios(id_uni_f, psc_arr)
    cap_arr_ = processar_dicionarios(id_uni_f, cap_arr)
    car_arr_ = processar_dicionarios(id_uni_f, car_arr)
    cra_arr_ = processar_dicionarios(id_uni_f, cra_arr)
    dp_arr_ = processar_dicionarios(id_uni_f, dp_arr)
    pf_arr_ = processar_dicionarios(id_uni_f, pf_arr)
    vrac_arr_ = processar_dicionarios(id_uni_f, vrac_arr)
    pbp_percente_arr_ = processar_dicionarios(id_uni_f, pbp_percente_arr)
    pbp_kg_arr_ = processar_dicionarios(id_uni_f, pbp_kg_arr)
    pbp_real_arr_ = processar_dicionarios(id_uni_f, pbp_real_arr)
    
    # --------------------------------------------------------------------------------------------
    avc_percente_arr_ = processar_dicionarios(id_uni_f, avc_percente_arr)
    avc_kg_arr_ =  processar_dicionarios(id_uni_f, avc_kg_arr)
    avc_real_arr_ =  processar_dicionarios(id_uni_f, avc_real_arr)
    acd_percent_arr_ = processar_dicionarios(id_uni_f, acd_percent_arr)
    acd_kg_arr_ = processar_dicionarios(id_uni_f, acd_kg_arr)
    acd_real_arr_ = processar_dicionarios(id_uni_f, acd_real_arr)
    
    acp_percent_arr_ = processar_dicionarios(id_uni_f, acp_percent_arr)
    acp_kg_arr_ = processar_dicionarios(id_uni_f, acp_kg_arr)
    acp_real_arr_ = processar_dicionarios(id_uni_f, acp_real_arr)
    acl_percent_arr_ = processar_dicionarios(id_uni_f, acl_percent_arr)
    
    acl_kg_arr_ = processar_dicionarios(id_uni_f, acl_kg_arr)
    acl_real_arr_ = processar_dicionarios(id_uni_f, acl_real_arr)
    avcl_novo_arr_ = processar_dicionarios(id_uni_f, avcl_novo_arr)
    
    rbl_percent_arr_ = processar_dicionarios(id_uni_f, rbl_percent_arr)
    rbl_kg_arr_ = processar_dicionarios(id_uni_f, rbl_kg_arr)
    rbl_real_arr_ = processar_dicionarios(id_uni_f, rbl_real_arr)
    vrb_arr_ = processar_dicionarios(id_uni_f, vrb_arr)
    vnf_arr_ = processar_dicionarios(id_uni_f, vnf_arr)
    
    vtd_arr_ =  processar_dicionarios(id_uni_f, vtd_arr)
    dkm_arr_ = processar_dicionarios(id_uni_f, dkm_arr)
    
    adp_arr_ = processar_dicionarios(id_uni_f, adp_arr)
    qa_arr_ = processar_dicionarios(id_uni_f, qa_arr)
    dal_arr_ = processar_dicionarios(id_uni_f, dal_arr)
    qabt_arr_ = processar_dicionarios(id_uni_f, qabt_arr)
    pma_arr_ = processar_dicionarios(id_uni_f, pma_arr)
    dtma_arr_d_ = processar_dicionarios(id_uni_f, dtma_arr_d)
    prbd_arr_ = processar_dicionarios(id_uni_f, prbd_arr)
    homa_arr_ = processar_dicionarios(id_uni_f, homa_arr)
    rcsmd_arr_ = processar_dicionarios(id_uni_f, rcsmd_arr)
    pmrl_arr_ = processar_dicionarios(id_uni_f, pmrl_arr)
    tpo_arr_ =  processar_dicionarios(id_uni_f, tpo_arr)
    gpd_arr_ = processar_dicionarios(id_uni_f, gpd_arr)
    pmpj_arr_ = processar_dicionarios(id_uni_f, pmpj_arr)
    ajs_pp_arr_ = processar_dicionarios(id_uni_f, ajs_pp_arr)
    iee_arr_ = processar_dicionarios(id_uni_f, iee_arr)
    iep_arr_ = processar_dicionarios(id_uni_f, iep_arr)
    pm_arr_ = processar_dicionarios(id_uni_f, pm_arr)
    nacp_arr_ = processar_dicionarios(id_uni_f, nacp_arr)
    npc_arr_ = processar_dicionarios(id_uni_f, npc_arr)
    pcp_arr_ =  processar_dicionarios(id_uni_f, pcp_arr)
    pcpp_arr_ =  processar_dicionarios(id_uni_f, pcpp_arr)
    pcrl_arr_ = processar_dicionarios(id_uni_f, pcrl_arr)
    mrt_prv_arr_  = processar_dicionarios(id_uni_f, mrt_prv_arr)
    pcpr_arr_ = processar_dicionarios(id_uni_f, pcpr_arr)
    mr_arr_ = processar_dicionarios(id_uni_f, mr_arr)
    dc_pr_arr_ = processar_dicionarios(id_uni_f, dc_pr_arr)
    dcp_pr_arr_ = processar_dicionarios(id_uni_f, dcp_pr_arr)
    dpxr_arr_ = processar_dicionarios(id_uni_f, dpxr_arr)
    idade_abate_arr_ = processar_dicionarios(id_uni_f, idade_abate_arr)
    viabilidade_arr_ = processar_dicionarios(id_uni_f, viabilidade_arr)
    
    # integrado_arr_f = list(set(integrado_arr))

    # print(len(integrado_arr))    
    # print(len(id_uni_f))
    # print(len(municipio_arr))

    # print('depois:', len(integrado_arr_f))
    
    new_dataFrame["CHAVE"] = id_uni_f
    
    new_dataFrame["CLIFFOR"] = integrado_id_arr
    # new_dataFrame["INTEGRADO_NOME"] = integrado_nome_arr
    # new_dataFrame["ENDEREÇO"] = endereco_arr_
    # new_dataFrame["MUNICIPIO"] = municipio_arr_
    # new_dataFrame["CPF/CGC"] = cgc_arr_
    # new_dataFrame["LOTE"] = lote_arr_
    # new_dataFrame["DATA_MEDIA_ALOJTO"] = dtma_arr_
    # new_dataFrame["HORA_MEDIA_ALOJTO"] = hma_arr_
    # new_dataFrame["AREA_ALOJADA"] = aa_arr_
    # new_dataFrame["SEXO"] = sexo_arr_
    # new_dataFrame["LINHAGEM"] = linhagem_arr_
    # new_dataFrame["AJS_LNHG"] = ajs_lnhg_arr_
    # new_dataFrame["PREV_SEMANAL_DE_CONV"] = psc_arr_
    # new_dataFrame["CONV_AJUSTADA_PREV"] = cap_arr_
    
    # new_dataFrame["CONV_ALIMENTAR_REAL"] = car_arr_
    
    # new_dataFrame["CONV_REAL_AJUSTADA"] = cra_arr_

    # new_dataFrame["DIFCA_PREVXREAL"] = dp_arr_
    # new_dataFrame["PF_PRECO_DO_KG_DO_FRANGO"] = pf_arr_
    # new_dataFrame["VRAC_VLR_DAS_RACOES"] = vrac_arr_
    
    # new_dataFrame["PERCENTUAL_BASICO_DE_PARTILHA_%"] = pbp_percente_arr_
    # new_dataFrame["PERCENTUAL_BASICO_DE_PARTILHA_KG"] = pbp_kg_arr_
    # new_dataFrame["PERCENTUAL_BASICO_DE_PARTILHA_$"] = pbp_real_arr_
    
    # new_dataFrame["AVALIACAO_CONVERSAO_%"] = avc_percente_arr_
    # new_dataFrame["AVALIACAO_CONVERSAO_KG"] = avc_kg_arr_
    # new_dataFrame["AVALIACAO_CONVERSAO_$"] = avc_real_arr_

    # new_dataFrame["AVALIACAO_CONDENACAO_%"] = acd_percent_arr_
    # new_dataFrame["AVALIACAO_CONDENACAO_KG"] = acd_kg_arr_
    # new_dataFrame["AVALIACAO_CONDENACAO_$"] = acd_real_arr_
    
    # new_dataFrame["AVALIACAO_CALO_DE_PATAS_%"] = acp_percent_arr_
    # new_dataFrame["AVALIACAO_CALO_DE_PATAS_KG"] = acp_kg_arr_
    # new_dataFrame["AVALIACAO_CALO_DE_PATAS_$"] = acp_real_arr_
    
    # new_dataFrame["AVALIACAO_CHECK_LIST_%"] = acl_percent_arr_

    # new_dataFrame["AVALIACAO_CHECK_LIST_KG"] = acl_kg_arr_
    # new_dataFrame["AVALIACAO_CHECK_LIST_$"] = acl_real_arr_
    # new_dataFrame["AVALIACAO_CHECK_LIST_NOVO"] = avcl_novo_arr_
        
    # new_dataFrame["RESULTADO_BRUTO_DO_LOTE_%"] = rbl_percent_arr_
    # new_dataFrame["RESULTADO_BRUTO_DO_LOTE_KG"] = rbl_kg_arr_
    # new_dataFrame["RESULTADO_BRUTO_DO_LOTE_$"] = rbl_real_arr_
    
    # new_dataFrame["VALOR_RENDA_BRUTA_CREDITO"] = vrb_arr_
    
    # new_dataFrame["VALOR_NF"] = vnf_arr_
    # new_dataFrame["VALOR_TOTAL_A_DEPOSITAR"] = vtd_arr_
    
    # new_dataFrame["DIST_KM"] = dkm_arr_
    # new_dataFrame["AREA_DISP"] = adp_arr_
    
    # new_dataFrame["QTDE_ALOJADA"] = qa_arr_
    
    # new_dataFrame["DATA_ACERTO_DO_LOTE"] = dal_arr_
    # new_dataFrame["QTDE_ABATIDA"] = qabt_arr_
    # new_dataFrame["PESO_MEDIO_ALOJADO"] = pma_arr_
    # new_dataFrame["DATA_MEDIA_ABATE"] = dtma_arr_d_
    # new_dataFrame["PESO_RECEBIDO"] = prbd_arr_
    # new_dataFrame['HORA_MEDIA_ABATE'] = homa_arr_
    
    # new_dataFrame['RACAO_CONSUMIDA'] = rcsmd_arr_
     
    # new_dataFrame['PESO_MEDIO_REAL'] = pmrl_arr_
    
    
    # new_dataFrame['TIPO_PRODUTO'] = tpo_arr_
    # new_dataFrame["GPD"] = gpd_arr_
    
    # new_dataFrame["PESO_MEDIO_PROJETADO"] = pmpj_arr_
    
    # new_dataFrame["AJS_PESO_PINTO"] = ajs_pp_arr_
    # new_dataFrame["IEE"] = iee_arr_
    
    # new_dataFrame["IEP"] = iep_arr_
    # new_dataFrame['PM_REAL_PM_PROJETADO'] = pm_arr_
    # new_dataFrame['AVES_CONDENADAS_TOTAL'] = avc_percente_arr_
    # new_dataFrame["N_AVES_CONDENADAS_PARCIAL"] = nacp_arr_
    
    # new_dataFrame["N_PATAS_CONDENADAS"] = npc_arr_
    # new_dataFrame["PC_CODENACOES_PREVISTO"] = pcp_arr_
    
    # new_dataFrame["PC_CALO_DE_PATA_PREVISTO"] = pcpp_arr_
    
    # new_dataFrame["MORT_PREV"] = mrt_prv_arr_
    # new_dataFrame["PC_CODENACOES_REAL"] = pcrl_arr_
    # new_dataFrame["PC_CALO_DE_PATA_REAL"] = pcpr_arr_
    # new_dataFrame["MORT_REAL"] = mr_arr_
    # new_dataFrame["DIFCA_COND_PREV_REAL"] = dc_pr_arr_
    # new_dataFrame["DIFCA_CALO_PATA_PREV_REAL"] = dcp_pr_arr_
    # new_dataFrame["DIFCA_PREV_X_REAL"] = dpxr_arr_
    # new_dataFrame["IDADE_ABATE"] = idade_abate_arr_
    new_dataFrame["VIABILIDADE"] = viabilidade_arr_

    # lista_c = [(va, id) for id, va in zip(dc_pr_arr, id_uni_f)]

    # print(homa_arr[-1] , id_uni_f)[])

    # print(df_2)
    print("Salvando arquivo...")
    new_dataFrame.to_csv(f"{file[0]}/avisidro_tabela{get_date_now()}.csv", mode="w", index=False)
    input("Arquivo salvo com sucesso...")

main()