from hmac import new
from math import nan, sin
from os import remove
import pandas as pd
import re, ast, os
from tqdm import tqdm
from datetime import datetime

global avcl_n

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

file_execel = r"Arquivos_Extraidos_CSV/dados_extraidos_avisidro.csv"

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

def str_to_list(s: str):
    s = re.sub(r'^\[|\]$', '', s)  # remove colchetes
    partes = [p.strip(" '\"") for p in s.split(",") if p.strip() != ""]
    return partes



def dicio_obj(id:str, Data:str)  -> dict:
    return {
        "id": id,
        "Data": Data,
    }




def main():
    id_uni = []

    ft_arr = []
    fp_arr = []
    arsl_ft_arr = []
    aspect_repug_arr = []
    septicemia_arr = []
    caquexia_arr = []
    sindrome_ascite_arr = []
    lesao_pele_arr = []
    contaminacao_b_arr = []
    aerosaculite_arr = []
    artrite_arr = []
    celulite_arr = []
    lesao_de_pele_arr = []
    integrado_nome_arr = []
    lesao_pele_arr_251 = []
    lesao_inflamatoria_arr = []
    sindrome_ascite_arr_  = []
    lesao_trau_antiga_arr = []
    
    print("Filtrando aguarde...")
    for index, row in df.iterrows():
        line_item = str(row.iloc[0])

        
        new_str = re.sub(r"\[|\]", "", line_item)
        new_str = remove_empty_spaces(remove_chars(new_str).split(", "))
       
        len_newStr = len(new_str)

        if find_number_id(new_str[0]):
            id_uni.append(new_str[0])
            id_l = new_str[0]
            
        # nan = volte para ajustar 
        if "FT" in line_item:
            # print(new_str)
            ft_sep = remove_empty_spaces(new_str[1].replace("FT", "").split(" "))
            ft_f = (ft_sep)[0]
            if "Condenação" in ft_sep:
                ft_f_c = df.loc[int(index)-5][0]
                ft_f_c = ast.literal_eval(ft_f_c)[1]
                if "FT" in ft_f_c:
                    ft_f = ft_f_c.replace("FT", "").strip().split(" ")[0]
                # ajuste apartir daqui
            ft_arr.append(dicio_obj(id_l, ft_f))
        
        
        if "FP" in line_item:
            fp_f = None
            fp_s = remove_empty_spaces(new_str[1].split("FP"))
            # fp_s = type(fp_s)
            len_nc = len(fp_s)
            if len_nc == 2:
                fp_f = (fp_s[1].strip().split(" "))[0]
                # print(fp_f)
                if not find_numbers(fp_f):
                    fp_f_s = ast.literal_eval(df.loc[int(index)-5][0])
                    fp_f_s = fp_f_s[1].strip()
                    fp_f_s = remove_empty_spaces(fp_f_s.split("FP"))
                    if len(fp_f_s) == 2:
                        fp_f = (fp_f_s[1].strip())
                    if len(fp_f_s) == 1:
                        if find_numbers(ast.literal_eval(df.loc[int(index)-5][0])[-1]):
                           fp_f = (ast.literal_eval(df.loc[int(index)-5][0])[-1]).split(" ")[0]
                if find_numbers(fp_f):
                    fp_f = fp_f
                    fp_arr.append(dicio_obj(id_l, fp_f))
                    
                    # print(fp_f)
                
            # if fp_f:
            fp_arr.append(dicio_obj(id_l, fp_f))
            
            # fp_arr = [dicio_obj(id_l, x) for x in fp_arr if x is not None]
        
        if "Aerossaculite" in line_item:
            arsl_s_ft  = remove_empty_spaces(new_str[1].split(" "))
            if arsl_s_ft[0] == "103":
                arsl_s_ft = remove_empty_spaces((new_str[1].replace("Aerossaculite", '')).split(" "))
                arsl_f_ft = arsl_s_ft[1]
            
                arsl_ft_arr.append(dicio_obj(id_l, arsl_f_ft))
                
        if "Aspecto Repugnante" in line_item or "Aspecto Repugte" in line_item:
            aspr_s = (new_str[1]).replace("Aspecto Repugte","Aspecto Repugnante").replace("%","% " ).strip()
            if "112" in aspr_s:
                aspr_s = remove_empty_spaces(aspr_s.split("112 Aspecto Repugnante"))
                nc_ = len(aspr_s)
                if nc_ == 2:
                    aspr_f = remove_empty_spaces(aspr_s[1].split(" "))[0]
                if nc_ == 1:
                    aspr_f = (remove_empty_spaces(aspr_s[0].split(' '))[0])
                
                aspect_repug_arr.append(dicio_obj(id_l, aspr_f))
        
        if "Septicemia" in line_item:
            spta_s = new_str[1].replace("%","% ").strip()
            if "130" in spta_s:
                spta_s = remove_empty_spaces(spta_s.split("130 Septicemia"))
                nc_ = len(spta_s)
                if nc_ == 2:
                   spta_f =  remove_empty_spaces(spta_s[1].split(" "))[0]
                if nc_ == 1:
                   spta_f = remove_empty_spaces(spta_s[0].split(" "))[0]

                septicemia_arr.append(dicio_obj(id_l, spta_f))
            
        if "Caquexia" in line_item:
            cqx_s = new_str[1].replace("%","% ").strip()
            if "105" in cqx_s:
                cqx_s = remove_empty_spaces(cqx_s.split("105 Caquexia"))
                nc = len(cqx_s)
                if nc == 2:
                    cqx_f = remove_empty_spaces(cqx_s[1].split(" "))[0]
                if nc == 1:
                    cqx_f =  remove_empty_spaces(cqx_s[0].split(" "))[0]

                caquexia_arr.append(dicio_obj(id_l, cqx_f))

        
        if "Sindrome Ascite" in line_item:
            sda_s = new_str
            nc = len(sda_s)
            if nc == 1:
                print(sda_s)
            if nc == 2:
                sda_s = remove_empty_spaces(sda_s[1].split("157 Sindrome Ascite"))
                sda_s = remove_empty_spaces(sda_s[0].split(" "))
                if len(sda_s) == 1:
                    sda_f = remove_empty_spaces(new_str[1].split("157 Sindrome Ascite")[1].split(" "))[0]
                if len(sda_s) ==  2:
                    sda_f = (sda_s[0])
            if nc == 3:
                sda_f = remove_empty_spaces(sda_s[-1].split(" "))[0]
                if "%" in sda_f:
                    sda_s = (sda_s[1].replace("Sindrome Ascite", "").replace("157", "").replace("256", ""))
                    sda_f = remove_empty_spaces(sda_s.split(" "))
                    if not sda_f:
                        sda_f = "nan"
                    sda_f = sda_f[0]


            sindrome_ascite_arr.append(dicio_obj(id_l, sda_f))
        
        # Ajuste
        if "Lesão de pele" in line_item:
            ldp_s = new_str[1]
            if  "151" in ldp_s:
                ldp_s = (ldp_s.replace("%", "% "))
                ldp_s = remove_empty_spaces(ldp_s.split("151 Lesão de pele"))
                if len(ldp_s) == 1:
                    ldp_f = remove_empty_spaces(ldp_s[0].split(" "))[0]
                    # print(ldp_f)
                    # if len(ldp_f) > 2:
                    #     if "151" in ldp_f:
                    #        ldp_f = ldp_f[0]
                    # ldp_f = ldp_f[0]
                if len(ldp_s) == 2:
                    ldp_f = (remove_empty_spaces(ldp_s[1].split(" "))[0])
                lesao_pele_arr.append(dicio_obj(id_l, ldp_f))
                
        if "Contaminação Gastrointestinal e B" in line_item:
            cgb_s = new_str[1].replace("%", "% ")
            if "179" in  cgb_s:
                cgb_s = remove_empty_spaces(cgb_s.split("179g Contaminação Gastrointestinal e B"))
                nc_ = len(cgb_s)
                if nc_ == 2:
                    cgb_f = remove_empty_spaces(cgb_s[1].split(" "))[0]
                if nc_ == 1:
                    cgb_f = remove_empty_spaces(cgb_s[0].split(" "))[0]
                contaminacao_b_arr.append(dicio_obj(id_l, cgb_f))
        
        if "Aerossaculite" in line_item:
            aero_sec_s = new_str
            if len(aero_sec_s) == 2:
                # print(aero_sec_s)
                pass
            if len(aero_sec_s) == 3:
                aero_sec_s = aero_sec_s[1].replace("%", "% ")
                if "220" in aero_sec_s:
                    aero_sec_f = (aero_sec_s.split("Aerossaculite")[-1])
                    if not aero_sec_f:
                        aero_sec_f = (remove_empty_spaces(new_str[-1].split(" ")))[0]
                    
            if len(aero_sec_s) == 4:
                aero_sec_f = (aero_sec_s[2])
                if "Aerossaculite" in (aero_sec_f):
                    aero_sec_f = remove_empty_spaces(aero_sec_s[3].split(" "))[0]
            if len(aero_sec_s) == 5:
                if "220 Aerossaculite" in str(aero_sec_s):
                    aero_sec_f = remove_empty_spaces(aero_sec_s)[3]
                    
            aerosaculite_arr.append(dicio_obj(id_l, aero_sec_f))
        
        if "Artrite" in line_item:
            art_s = new_str
            if len(art_s) == 2:
                # print(art_s)
                pass
            if len(art_s) == 3:
                art_s  = remove_empty_spaces( art_s[1].split("Artrite") )
                # print(art_s)
                if len(art_s) == 2:
                    art_s = remove_empty_spaces(art_s[-1].split(" "))
                    if len(art_s) == 1:
                        art_f = art_s[0]
                    if len(art_s) > 1:
                        art_f = "nan"
                    artrite_arr.append(dicio_obj(id_l, art_f))
                    
            if len(art_s) == 4:
               if "226 Artrite" in line_item:
                    art_f = (art_s[2])
                    if "226 Artrite" in art_f:
                        art_f = remove_empty_spaces(art_s[3].split(" "))[0]
                    else:
                        art_f = "nan"
                    artrite_arr.append(dicio_obj(id_l, art_f))
            
            if len(art_s) == 5:
                art_f = (art_s[3])
                if find_numbers(art_f):
                    art_f = (art_s[3])
                else:
                    art_f = "nan"
                artrite_arr.append(dicio_obj(id_l, art_f))
                
        if "Celulite" in line_item:
            celulite_s = line_item.replace("%", "% ")
            if "245 Celulite" in celulite_s:
                celulite_s = new_str
                nc_ = len(celulite_s)
                if nc_ == 2:
                    celulite_f = (celulite_s)
                if nc_ == 3:
                    celulite_f = (celulite_s[1].split("245 Celulite")[-1])
                    if not celulite_f:
                        celulite_f = remove_empty_spaces(celulite_s[2].split(" "))[0]
                if nc_ == 4:
                    celulite_f = celulite_s[2] 
                    if "Celulite" in str(celulite_f):
                        celulite_f = remove_empty_spaces(celulite_s[-1].split(" "))[0]
                if nc_ == 5:
                    celulite_f = (celulite_s[-1-1])
                    # print(celulite_f)
                celulite_arr.append(dicio_obj(id_l, celulite_f))
        
        if "Lesão de pele" in line_item:
            ldp_s = line_item.replace("%", "% ")
            if "251 Lesão de pele" in ldp_s or "251 Lesao de pele" in ldp_s:
                ldp_s = new_str
                nc_ = len(ldp_s)
                if nc_ == 2:
                    ldp_s = ldp_s[1].split("Lesão de pele")
                    ldp_f = remove_empty_spaces(ldp_s)[-1]
                if nc_ == 3:
                    ldp_s = ldp_s[-1]
                    ldp_f = remove_empty_spaces(ldp_s.split(" "))[0]
                    if "%" in ldp_f:
                        ldp_f = new_str[1].split("de pele")[-1]
                    
                    if len(ldp_f) == 1:
                        ldp_f = new_str
                        ldp_f = remove_empty_spaces(ldp_f[1].split("Lesão de pele"))
                        ldp_f = ldp_f[-1]
                        if "%" in ldp_f:
                            ldp_f =  new_str[-1]
                        ldp_f = ldp_f.strip()

                if nc_ == 4:
                    ldp_s = (new_str[2])
                    if "251 Lesão de pele" in ldp_s:
                        ldp_s = new_str[-1].split(" ")
                        ldp_f = ldp_s[0]
                        
                if nc_ == 5:
                    ldp_f = (new_str[-1-1])
                    
                lesao_pele_arr_251.append(dicio_obj(id_l, ldp_f))
        
        if "Lesão Inflamatória" in line_item:
            lia_s = new_str
            nc_l = len(lia_s)
            
            if nc_l == 2:
                lia_s = (lia_s[1]).replace("%", "% ")
                lia_f = remove_empty_spaces(lia_s.split("Inflamatória"))[-1]
                lia_f = lia_f
            if nc_l == 3:
                lia_s = new_str[-1].split(" ")
                lia_f = lia_s[0]
                if "%" in lia_f:
                    lia_f = new_str[1].split("Inflamatória")[-1]
                    if not lia_f:
                        lia_f = "nan"
            if nc_l == 4:
                lia_f = new_str[2]
                if "253 Lesão Inflamatória" in lia_f:
                    lia_f =  new_str[-1]
                    
            lesao_inflamatoria_arr.append(dicio_obj(id_l, lia_f))
        
        if "Sindrome Ascite" in  line_item:
            if "256 Sindrome Ascite" in line_item:
                sda_s = new_str
                nc_ = len(sda_s)
                
                
                if nc_ == 2:
                    sda_s = sda_s[1].replace("%", "% ")
                    sda_f = (sda_s).split("256 Sindrome Ascite")[-1]
                    sda_f = sda_f.strip()
                    
                if nc_ == 3:
                    sda_s = remove_empty_spaces(sda_s[-1].split(" "))
                    if not "%" in sda_s[0]:
                        sda_f = sda_s
                    else:
                        sda_f = new_str[1].split("Ascite")[-1]
                        sda_f = sda_f.strip()
                        
                if nc_ == 4:
                    sda_f = new_str[2]
                    
                # if nc_ == 5:
                #     sda_s = new_str
                #     print(sda_s)
                    
                # if nc_ == 6:
                #     pass
                sindrome_ascite_arr_.append(dicio_obj(id_l, sda_f))
        
        if "Lesão Traumática Antiga" in line_item:
            lta_s = new_str
            nc_ = len(lta_s)
            
            if nc_  == 2:
                lta_s = (lta_s[1]).split("Antiga")[-1]
                lta_f  = remove_empty_spaces((lta_s).split(' '))
                lta_f = lta_f[0]
                
            if nc_  == 3:
                lta_f = (lta_s)[2]
                lta_f = lta_f.split(" ")[0]
                if not "%" in  lta_f:
                    lta_f = lta_f
                else:
                    lta_f = new_str[1].split("Antiga")
                    lta_f = remove_empty_spaces((lta_f)[1].split(" "))
                    if not lta_f:
                        lta_f = "nan"
                        
                    lta_f = lta_f[0]
                    if not lta_f or "%" in lta_f:
                        lta_f = "nan"
            if nc_  == 4:
                lta_s = new_str[2]

            lesao_trau_antiga_arr.append(dicio_obj(id_l, lta_f))


    id_uni_f = remover_duplicatas(id_uni)
    
    # integrado_arr_f = list(set(integrado_arr))
    print(len(contaminacao_b_arr))
    
    ft_arr_r = processar_dicionarios(id_uni_f, ft_arr)
    fp_arr_ = processar_dicionarios(id_uni_f, fp_arr)
    arsl_ft_arr = processar_dicionarios(id_uni_f, arsl_ft_arr)
    aspect_repug_arr = processar_dicionarios(id_uni_f, aspect_repug_arr)
    septicemia_arr = processar_dicionarios(id_uni_f, septicemia_arr)
    caquexia_arr = processar_dicionarios(id_uni_f, caquexia_arr)
    sindrome_ascite_arr = processar_dicionarios(id_uni_f, sindrome_ascite_arr) 
    lesao_pele_arr = processar_dicionarios(id_uni_f, lesao_pele_arr)
    contaminacao_b_arr = processar_dicionarios(id_uni_f, contaminacao_b_arr)
    aerosaculite_arr = processar_dicionarios(id_uni_f, aerosaculite_arr)
    artrite_arr = processar_dicionarios(id_uni_f, artrite_arr)
    celulite_arr = processar_dicionarios(id_uni_f, celulite_arr)
    lesao_pele_arr_251 = processar_dicionarios(id_uni_f, lesao_pele_arr_251)
    lesao_inflamatoria_arr = processar_dicionarios(id_uni_f, lesao_inflamatoria_arr)
    sindrome_ascite_arr_ = processar_dicionarios(id_uni_f, sindrome_ascite_arr_)
    lesao_trau_antiga_arr = processar_dicionarios(id_uni_f, lesao_trau_antiga_arr)
    
    
    
    
    
    
    
    
    
    new_dataFrame["CHAVE"] = id_uni_f
    new_dataFrame["FT"] = ft_arr_r
    new_dataFrame["FP"] = fp_arr_
    new_dataFrame["Aerossaculite_FT"] = arsl_ft_arr
    new_dataFrame["Aspecto Repugnante"] = aspect_repug_arr
    new_dataFrame["Septicemia"] = septicemia_arr
    new_dataFrame["caqueixa"] = caquexia_arr
    new_dataFrame["Sindrome Ascite"] = sindrome_ascite_arr
    
    new_dataFrame["Lesão de pele"] = lesao_pele_arr

    new_dataFrame["Contaminação Gastrointestinal e B"] = contaminacao_b_arr
    
    new_dataFrame["Aerossaculite"] = aerosaculite_arr
    new_dataFrame["Artrite"] = artrite_arr
    new_dataFrame["Celulite"] = celulite_arr
    new_dataFrame["LESAO_DE_PELE_251"] = lesao_pele_arr_251
    new_dataFrame["LESAO_INFLAMATORIA"] = lesao_inflamatoria_arr
    new_dataFrame["SIDROME_ASCIT"] = sindrome_ascite_arr
    new_dataFrame["LESAO_TRAUMATICA_ANTIGA"] = lesao_trau_antiga_arr
    
    
    # lista_c = [(va, id) for id, va in zip(dc_pr_arr, id_uni_f)]


    print("Salvando arquivo...")
    new_dataFrame.to_csv(f"{file[0]}/sif_tabela{get_date_now()}.csv", mode="w", index=False)
    input("Arquivo salvo com sucesso...")

main()