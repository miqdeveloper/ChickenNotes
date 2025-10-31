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
    abscesso_ft_101_arr= []
    artrite_ft_102_arr = []
    ascite_ft_104_arr = []
    celulite_ft_108_arr = []
    colibacilose_ft_109_arr  = []
    dermatose_ft_110_arr = []
    neoplasia_ft_117_arr = []
    tumores_ft_131_arr = []
    pericardite_ft_132_arr = []
    lesao_de_pele_ft_151_arr = []
    sindrome_hemorragica_ft_152_arr = []
    lesao_inflamatoria_ft_153_arr = []
    calo_no_peito_ft_159_arr  = []
    contaminacao_nao_gi_ft_163_arr = []
    contaminacao_gastrointestinal_e_b_ft_179_arr = []
    estados_anormais_ou_patologicos_ft_194_arr = []
    contaminacao_papo_pendular_ft_688_arr = []
    contusao_ft_765_arr = []
    arsl_ft_arr = []
    contusao_fratura_ft_636_arr = []
    miopatia_ft_170_arr = []
    lesao_traumatica_antiga_ft_135_arr = []
    processo_inflamatorio_ft_114_arr = []
    canibalismo_ft_106_arr = []
    necrose_caseosa_ft_139_arr  = []
    
    
    
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
            ft_f = (line_item)
            
            pattern = re.compile(
                r"FT\s+(\d{1,3}(?:\.\d{3})*)\s+([\d.,]+%)",
                flags=re.IGNORECASE
            )

            match = pattern.search(ft_f)
            if match:
                qtde_ = (match.group(1) or "nan")   # ex: "798"
                cond_ = (match.group(2) or "nan")   # ex: "1,589%"
                # print("qtde_:", qtde_)
                # print("cond_:", cond_)
            ft_arr.append(dicio_obj(id_l, qtde_))

        if "FP" in line_item:
            fp_f = line_item.replace(", ", "").replace("nan", "").replace(",nan", "")
            pattern = re.compile(
                r"""
                FP                   # literal FP
                (?:'|n*a*n*)*        # pode ter aspas ou 'nan' depois
                [^0-9]*              # ignora até número
                ('?(\d{1,3}(?:\.\d{3})*|\d+(?:,\d+)?))  # grupo 2: quantidade (com ou sem aspas)
                (?:'|n*a*n*)*[^0-9]* # ignora aspas, nan, espaços
                ('?(\d+(?:[.,]\d+)?%))                  # grupo 4: percentual (com ou sem aspas)
                """,
                flags=re.IGNORECASE | re.VERBOSE
            )
            
            pattern_colado = re.compile(
                r"FP'?(\d{1,3}(?:\.\d{3})*|\d+(?:,\d+)?)'(\d+(?:[.,]\d+)?%)",
                flags=re.IGNORECASE
            )
            
            match = pattern.search(fp_f)

            if match:
                qtde_ = match.group(1) # Ex: 3.393
                cond_ = match.group(2) or "nan"  # Ex: 27,032%

            if not match:
                # if "30295607777-2306" in (line_item):
                match = pattern_colado.search(fp_f.replace(",nan", "").replace("nan", ""))
                if match:
                    qtde_ = match.group(1)
                    cond_ = match.group(2)
                if not match:
                    if  "30295614994-2301" in line_item:
                        print(fp_f, line_item)
            fp_arr.append(dicio_obj(id_l, qtde_))
            
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
                # print(sda_s)
                pass
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
        # 226 Artrite FP 
        if "Artrite" in line_item:
            art_s = line_item
            art_s = str (art_s)
            
            if "226 Artrite" in art_s:
                # print(art_s)
                parts = re.findall(r"'([^']*)'", art_s)
                # print(parts) 
                qtde_, cond_ = 0, "nan"

                for i, chunk in enumerate(parts):
                    if "226 Artrite" in chunk:
                        # print(chunk)
                        # tentar achar número e percentual no mesmo trecho
                        m_same = re.search(
                            r'226\s+Artrite\s+(\d{1,3}(?:\.\d{3})*)\s+(\d+(?:[.,]\d+)?%)',
                            chunk,
                            re.IGNORECASE
                        )
                        if m_same:
                            qtde_ = m_same.group(1)
                            cond_ = m_same.group(2)
                        else:
                            # tentar no próximo trecho
                            if i + 1 < len(parts):
                                prox = parts[i + 1]
                                m_next = re.search(
                                    r'(\d{1,3}(?:\.\d{3})*)\s+(\d+(?:[.,]\d+)?%)',
                                    prox
                                )
                                if m_next:
                                    qtde_ = m_next.group(1)
                                    cond_ = m_next.group(2)
                        break  # encontrou, sai do loop

                # normalizar a quantidade
                qtde_norm = (qtde_) if qtde_ != 0 else 0

                artrite_arr.append(dicio_obj(id_l, str(qtde_norm)))
                
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
                sda_s = line_item
                parts = re.findall(r"'([^']*)'", sda_s)
                qtde_, cond_ = 0, "nan"

                for i, chunk in enumerate(parts):
                    if "256 Sindrome Ascite" in chunk:
                        # procurar dentro do mesmo trecho
                        m_same = re.search(
                            r'256\s+Sindrome\s+Ascite\s+(\d{1,3}(?:\.\d{3})*)\s+(\d+(?:[.,]\d+)?%)',
                            chunk, re.IGNORECASE
                        )
                        if m_same:
                            qtde_ = m_same.group(1)
                            cond_ = m_same.group(2)
                        else:
                            # tentar no próximo pedaço
                            if i + 1 < len(parts):
                                prox = parts[i + 1]
                                m_next = re.search(
                                    r'(\d{1,3}(?:\.\d{3})*)\s+(\d+(?:[.,]\d+)?%)',
                                    prox
                                )
                                if m_next:
                                    qtde_ = m_next.group(1)
                                    cond_ = m_next.group(2)
                        break

                qtde_norm = int(qtde_.replace('.', '')) if qtde_ != 0 else 0
                sindrome_ascite_arr.append(dicio_obj(id_l, str(qtde_norm)))
                # sindrome_ascite_arr_.append(dicio_obj(id_l, sda_f))
        
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
        
        # precisa ajustar todos acima com qtde - poercetagem
        if "Abcesso" in line_item:
            abs_s = new_str[1].replace('%', '% ').strip()
            if "101 Abc" in abs_s:
                abs_s = remove_empty_spaces(abs_s.split("101 Abcesso"))[0].strip()
                abs_f = abs_s.split(" ")[0]
            #     nc_ = len(abs_s)
                if "%" in abs_f:
                    abs_f = "nan"
                abs_f = abs_f.strip()
                abscesso_ft_101_arr.append(dicio_obj(id_l, abs_f))
        
        if "Artrite" in line_item:
            art_s = new_str[1].replace('%', '% ').strip()
            if '102 Artrite' in art_s:
                art_s = art_s.replace('102 Artrite', '').strip()
                art_s = art_s.split(' ')
                art_f = art_s[0]
                if "%" in art_f:
                    art_f = "nan"
                art_f = art_f.strip()
                artrite_ft_102_arr.append(dicio_obj(id_l, art_f))

        # FT
        if "Ascite" in line_item:
            # 
            line_item = line_item.replace('%', '% ').strip()
            m = re.search(r"(?i)\b104\s+Ascite\s+(\d{1,3}(?:\.\d{3})*)\s+[\d.,]+%", line_item)
            
            if m:
                valor = int(m.group(1).replace('.', ''))  # -> 99
                ascite_ft_104_arr.append(dicio_obj(id_l, str(valor)))
        
        # 108 Celulite FT  - separar apartir daqui pra cima 
        if "Celulite" in line_item: 
            if "108 Celulite" in line_item:
                clt_s = line_item.replace('%', '% ').strip()
                m = re.search(r'(?i)\b108\s+Celulite\s+(?:(\d{1,3}(?:\.\d{3})*)\s+)?([\d.,]+%)', clt_s)
                if m:
                         # texto fixo em uma var
                    qtde_  = int((m.group(1) or "0").replace('.', ''))  # número após o rótulo (0 se faltar)
                    cond_  = m.group(2)                               # percentual com '%', ex.: '0,003%'

                    celulite_ft_108_arr.append(dicio_obj(id_l, str(qtde_))) 
        
        
        # 109 Colibacilose FT 
        if "Colibacilose " in line_item:
            if "109 Colibacilose" in line_item:
                clb_s = line_item.replace('%', '% ').strip()
                
                match = re.search(r"(\d+)\s+([\d,]+%)", clb_s)

                if match:
                    qtde_ = match.group(1)     # '5'
                    cond_ = match.group(2)     # '0,006%'
                    
                    colibacilose_ft_109_arr.append(dicio_obj(id_l, qtde_))
        
        # 110 Dermatose FT
        if "Dermatose" in line_item:
            if "110 Dermatose" in line_item:
                dmts_s = line_item.replace('%', '% ').strip()
                match = re.search(r"110\s+Dermatose\s+(\d+)\s+([\d,]+%)", dmts_s)
                if match:
                    qtde_d = str(match.group(1))   # Ex: 14
                    cond_d = match.group(2)   # Ex: 0,018%
                    dermatose_ft_110_arr.append(dicio_obj(id_l, qtde_d))
        
        # 117 Neoplasia FT
        if "Neoplasia" in line_item:
            if "117 Neoplasia" in line_item:
                npls_s = line_item.replace('%', '% ').strip()
                match = re.search(r"117\s+Neoplasia\s+(\d+)\s+([\d,]+%)", npls_s)
                if match:
                    qtde_n = str(match.group(1))   # Ex: 14
                    cond_n = match.group(2)   # Ex: 0,018%
                    neoplasia_ft_117_arr.append(dicio_obj(id_l, qtde_n))
        
        # 131 Tumores FT
        if "Tumores" in line_item:
            if "131 Tumores" in line_item:
                tmr_s = line_item.replace('%', '% ').strip()
                
                m = re.search(r'(?i)\b131\s+Tumores\s+(?:(\d{1,3}(?:\.\d{3})*)\s+)?([\d.,]+%)', tmr_s)
                if m:
                    qtde_ = int((m.group(1) or "0").replace('.', ''))   # número após o código
                    cond_ = m.group(2)                                 # percentual ex: '0,003%'
                else:
                    qtde_, cond_ = 0, "nan"

                tumores_ft_131_arr.append(dicio_obj(id_l, str(qtde_)))
        
        # 132 Pericardite FT
        if "Pericardite" in line_item:
            if "132 Pericardite" in line_item:
                per_s = line_item.replace('%', '% ').strip()
                match = re.search(r'(?i)\b132\s+Pericardite\s+(?:(\d{1,3}(?:\.\d{3})*)\s+)?([\d.,]+%)', per_s)
                                
                if match:
                    qtde_p = str(match.group(1) or "0").replace('.', '')   # Ex: 14 
                    cond_p = match.group(2)   # Ex: 0,018%
                    pericardite_ft_132_arr.append(dicio_obj(id_l, qtde_p))
                else:
                    qtde_p, cond_p = 0, "nan"
    
        # 151 Lesão de pele FT
        if "Lesão de pele" in line_item or "Lesao de pele" in line_item:
            if "151 Lesão de pele" in line_item or "151 Lesao de pele" in line_item:
                line_item = line_item.replace("Lesão de pele", "Lesao de pele")
                ldp_s = line_item.replace('%', '% ').strip()
                match = re.search(r'(?i)\b151\s+Lesao de pele\s+(?:(\d{1,3}(?:\.\d{3})*)\s+)?([\d.,]+%)', ldp_s)
                if match:
                    qtde_ = int((match.group(1) or "0").replace('.', '')) # número após o código
                    cond_ = match.group(2)                                 # percentual ex: '0,003%'
                else:
                    qtde_, cond_ = 0, "nan"

                lesao_de_pele_ft_151_arr.append(dicio_obj(id_l, str(qtde_)))
        
        # 152 Sindrome Hemorragica FT
        if "Sindrome Hemorragica" in line_item or "Síndrome Hemorrágica" in line_item:
            if "152 Sindrome Hemorragica" in line_item or "152 Síndrome Hemorrágica" in line_item:
                line_item = line_item.replace("Síndrome Hemorrágica", "Sindrome Hemorragica")
                sh_s = line_item.replace('%', '% ').strip()
                match = re.search(r'(?i)\b152\s+Sindrome Hemorragica\s+(?:(\d{1,3}(?:\.\d{3})*)\s+)?([\d.,]+%)', sh_s)
                if match:
                    qtde_ = int((match.group(1) or "0").replace('.', '')) # número após o código
                    cond_ = match.group(2)                                 # percentual ex: '0,003%'
                else:
                    qtde_, cond_ = 0, "nan"

                sindrome_hemorragica_ft_152_arr.append(dicio_obj(id_l, str(qtde_)))
                
        if "Lesão Inflamatória" in line_item or "Lesao Inflamatoria" in line_item:
            if "153 Lesão Inflamatória" in line_item or "153 Lesao Inflamatoria" in line_item:
                line_item = line_item.replace("Lesão Inflamatória", "Lesao Inflamatoria")
                li_s = line_item.replace('%', '% ').strip()
                match = re.search(r'(?i)\b153\s+Lesao Inflamatoria\s+(?:(\d{1,3}(?:\.\d{3})*)\s+)?([\d.,]+%)', li_s)
                if match:
                    qtde_ = int((match.group(1) or "0").replace('.', '')) # número após o código
                    cond_ = match.group(2)                                 # percentual ex: '0,003%'
                else:
                    qtde_, cond_ = 0, "nan"

                lesao_inflamatoria_ft_153_arr.append(dicio_obj(id_l, str(qtde_)))            
        
        # 163 Contaminação FT
        if "Contaminação" in line_item:
            if "163 Contaminação" in line_item:
                cn_gi_s = line_item.replace('%', '% ').strip()
                match = re.search(r'(?i)\b163\s+Contaminação Não GI\s+(?:(\d{1,3}(?:\.\d{3})*)\s+)?([\d.,]+%)', cn_gi_s)
                if match:
                    qtde_ = int((match.group(1) or "0").replace('.', '')) # número após o código
                    cond_ = match.group(2)                                 # percentual ex: '0,003%'
                else:
                    qtde_, cond_ = 0, "nan"

                contaminacao_nao_gi_ft_163_arr.append(dicio_obj(id_l, str(qtde_)))
        
        # 179 Contaminação Gastrointestinal e B FT
        if "Contaminação Gastrointestinal e B" in line_item:
            if "179 Contaminação Gastrointestinal" in line_item:
                cgb_s = line_item.replace('%', '% ').strip()
                match = re.search(r'(?i)\b179\s+Contaminação Gastrointestinal e B\s+(?:(\d{1,3}(?:\.\d{3})*)\s+)?([\d.,]+%)', cgb_s)
                if match:
                    qtde_ = str((match.group(1) or "0").replace('.', '')) # número após o código
                    # print( qtde_, line_item)
                    cond_ = match.group(2)                                 # percentual ex: '0,003%'
                else:
                    qtde_, cond_ = 0, "nan"

                contaminacao_gastrointestinal_e_b_ft_179_arr.append(dicio_obj(id_l, str(qtde_)))

        # 194 Estados Anormais ou Patológicos ft
        if "194 Estados Anormais ou Patológicos" in line_item:
            if "194 Estados Anormais ou Patológicos" in line_item:
                eaop_s = line_item.replace('%', '% ').strip()
                match = re.search(r'(?i)\b194\s+Estados Anormais ou Patológicos\s+(?:(\d{1,3}(?:\.\d{3})*)\s+)?([\d.,]+%)', eaop_s)
                if match:
                    qtde_ = int((match.group(1) or "0").replace('.', '')) # número após o código
                    # print( qtde_, line_item)
                    cond_ = match.group(2)                                 # percentual ex: '0,003%'
                else:
                    qtde_, cond_ = 0, "nan"

                estados_anormais_ou_patologicos_ft_194_arr.append(dicio_obj(id_l, str(qtde_)))
    
        if "Contusão" in line_item:
            cta_s = new_str
            if "765 Contusão" in line_item:
                # s = line_item.replace('%', '% ').strip()
                cta_s = new_str[1].replace('%', '% ').strip()
                cta_s = remove_empty_spaces(cta_s.split("765 Contusão"))
                cta_f = None
                # print(cta_s)
                if not cta_s:
                    cta_f  = remove_empty_spaces(new_str[2].split(" "))[0]
                else:
                    cta_f = (cta_s[0]).strip()
                
                contusao_ft_765_arr.append(dicio_obj(id_l, str(cta_f)))

        # miopatia_ft_170
        if "Miopatia" in line_item:
            if "170 Miopatia" in line_item:
                miop_s = line_item.replace('%', '% ').strip()
                match = re.search(r'(?i)\b170\s+Miopatia\s+(?:(\d{1,3}(?:\.\d{3})*)\s+)?([\d.,]+%)', miop_s)
                if match:
                    qtde_ = int((match.group(1) or "0").replace('.', '')) # número após o código
                    # print( qtde_, line_item)
                    cond_ = match.group(2)                                 # percentual ex: '0,003%'
                    miopatia_ft_170_arr.append(dicio_obj(id_l, str(qtde_)))
        
        # 636 Contusão/Fratura
        if "Contusão/Fratura" in line_item:
            if "636 Contusão/Fratura" in line_item or "636 Contusao/Fratura" in line_item:
                cft_s = line_item.replace('%', '% ').strip()
                match = re.search(r'(?i)\b636\s+Contusão/Fratura\s+(?:(\d{1,3}(?:\.\d{3})*)\s+)?([\d.,]+%)', cft_s)
                if match:
                    qtde_ = int((match.group(1) or "0").replace('.', '')) # número após o código
                    cond_ = match.group(2)                                 # percentual ex: '0,003%'
                    contusao_fratura_ft_636_arr.append(dicio_obj(id_l, str(qtde_)))
            
        if "Calo no Peito" in line_item:
            cnp_s  = line_item.replace('%', '% ').strip()
            if "159 Calo no Peito" in line_item:
                pattern_cnp = re.compile(
                r"159\s*Calo\s+no\s+Peito"          # texto fixo
                r"(?:\s+([\d.,]+)\s*%)?"            # grupo 1: percentual à direita (opcional)
                # r"(?:.*?\s+(\d{1,3}(?:\.\d{3})*|\d+(?:,\d+)?))?"  # grupo 2: número isolado (ex: 220, 253, 148)
                , flags=re.IGNORECASE
            )
            
                match = pattern_cnp.search(cnp_s)
                if match:
                    qtde_ = match.group(1)
                calo_no_peito_ft_159_arr.append(dicio_obj(id_l, str(qtde_)))
                    # conde_ = match.group(2)
                    # print(qtde_, cnp_s)
                # 
        
        # 688 Contaminação Papo Pendular - FT
        if "688 Contaminação Papo Pendular" in line_item or "Contaminaçao Papo Pendular"  in line_item or "Contaminacao Papo Pendular" in line_item:
            cpp_s = line_item.replace('%', '% ')
            # print(cpp_s)

            pattern_cpp = re.compile(
    r"(?:\b\d{1,3}[.,]\d{1,3}%\s*)?"                # opcional: percentual antes do texto
    r"688\s+Contamina[çc][aã]o\s+Papo\s+Pendular"   # texto fixo com cedilha e variantes
    r"(?:\s+0,?0*%\s*)?"                            # opcional: '0,000%' logo após o texto
    r"(?:[^0-9]+)?"                                 # ignora palavras como 'Artrite', 'Lesão', etc.
    r"(\d{1,4}(?:[.,]\d{3})*|\d+)"                  # grupo 1: número após o 0,000% (ex: 253)
    r"(?:\s*[^\d%]+)?"                              # ignora palavras entre o número e o percentual
    r"([\d.,]+%)",                                  # grupo 2: percentual (ex: 3,389%)
    flags=re.IGNORECASE
)
            m = pattern_cpp.search(cpp_s)
            if m:
                qtde_ = m.group(1)
                # print( qtde_, cpp_s)
                cond_ = m.group(2)
            contaminacao_papo_pendular_ft_688_arr.append(dicio_obj(id_l, str(qtde_)))
        
        #  lesao_traumatica_antiga_ft_135_arr  
        if "135 Lesão Traumática Antiga" in line_item:
            ltma_ = line_item.replace('%', '% ')
            pattern_135 = re.compile(
                r"(?:\b\d{1,3}[.,]\d{1,3}%\s*)?"                   # opcional: percentual antes do texto
                r"135\s+Les[aã]o\s+Traum[aá]tica\s+Antiga"         # texto fixo com acentos ou sem
                r"(?:\s+0,?0*%\s*)?"                               # opcional: '0,000%' logo após o texto
                r".*?"                                             # qualquer coisa intermediária
                r"(\d{1,4}(?:[.,]\d{3})*|\d+)"                     # grupo 1: quantidade (ex: 37, 88, 1)
                r"\s+([\d.,]+%)",                                  # grupo 2: percentual (ex: 0,046%)
                flags=re.IGNORECASE
            )
            
            m = pattern_135.search(ltma_)
            if m:
                qtde_ = m.group(1)
                cond_ = m.group(2)
            lesao_traumatica_antiga_ft_135_arr.append(dicio_obj(id_l, str(qtde_)))
        
        # processo_inflamatorio_ft_114_arr
        if "114 Processo Inflamatório" in line_item:
            pio_s = line_item.replace('%', '% ')
            
            pattern_114 = re.compile(
                
            r"(?:\b\d{1,3}[.,]\d{1,3}%\s*)?"                # opcional: percentual antes do texto
            r"114\s+Processo\s+Inflamat[oó]rio"             # texto fixo com acento opcional
            r"(?:\s+0,?0*%\s*)?"                            # opcional: '0,000%' logo após o texto
            r".*?"                                          # qualquer coisa intermediária
            r"(\d{1,4}(?:[.,]\d{3})*|\d+)"                  # grupo 1: quantidade (ex: 40, 7, 20)
            r"\s+([\d.,]+%)",                               # grupo 2: percentual (ex: 0,051%)
            flags=re.IGNORECASE
        )
            m=pattern_114.search(pio_s)
            if m:
                qtde_ = m.group(1) or "nan"
                cond_ = m.group(2) or "nan"
                
            processo_inflamatorio_ft_114_arr.append(dicio_obj(id_l, str(qtde_)))
        
        # canibalismo_ft_106_arr
        if "106 Canibalismo" in line_item:
            cnb_s = line_item.replace('%', '% ')
            pattern_106 = re.compile(
                r"106\s+Canibalismo\s+"
                r"(?:"                              
                    r"(\d{1,4})\s+([\d.,]+%)"        # CASO 1: 106 Canibalismo 318 0,450%
                    r"|"
                    r"(0,?0*%)"                      # CASO 2: 106 Canibalismo 0,000%
                r")",
                flags=re.IGNORECASE
            )
            m = pattern_106.search(cnb_s)
            if m:
                qtde_ = m.group(1) or "nan"
                cond_ = m.group(2) or "nan"
                
            canibalismo_ft_106_arr.append(dicio_obj(id_l, str(qtde_)))
        
        # necrose_caseosa_ft_139_arr
        if "139 Necrose Caseosa" in line_item:
            nc_s = line_item.replace('%', '% ')
            pattern_139 = re.compile(
            r"139\s+Necrose\s+Caseosa\s+"
            r"(?:"                       
                r"(\d{1,4})\s+([\d.,]+%)"  # CASO 1: 139 Necrose Caseosa 12 0,045%
                r"|"
                r"(0,?0*%)"                # CASO 2: 139 Necrose Caseosa 0,000%
            r")",
            flags=re.IGNORECASE
        )
            m=pattern_139.search(nc_s)
            if m:
                qtde_ = m.group(1) or "nan"
                cond_ = m.group(2) or "nan"
            necrose_caseosa_ft_139_arr.append(dicio_obj(id_l, str(qtde_)))
        
        if "" in line_item:
            pass

    
    
    
    
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
    abscesso_ft_101_arr = processar_dicionarios(id_uni_f, abscesso_ft_101_arr)
    artrite_ft_102_arr = processar_dicionarios(id_uni_f, artrite_ft_102_arr)
    ascite_ft_104_arr = processar_dicionarios(id_uni_f, ascite_ft_104_arr)
    celulite_ft_108_arr = processar_dicionarios(id_uni_f, celulite_ft_108_arr)
    colibacilose_ft_109_arr  = processar_dicionarios(id_uni_f, colibacilose_ft_109_arr)
    dermatose_ft_110_arr    = processar_dicionarios(id_uni_f, dermatose_ft_110_arr)
    neoplasia_ft_117_arr = processar_dicionarios(id_uni_f, neoplasia_ft_117_arr)
    tumores_ft_131_arr = processar_dicionarios(id_uni_f, tumores_ft_131_arr)
    pericardite_ft_132_arr = processar_dicionarios(id_uni_f, pericardite_ft_132_arr)
    lesao_de_pele_ft_151_arr = processar_dicionarios(id_uni_f, lesao_de_pele_ft_151_arr)
    sindrome_hemorragica_ft_152_arr = processar_dicionarios(id_uni_f, sindrome_hemorragica_ft_152_arr)
    lesao_inflamatoria_ft_153_arr   = processar_dicionarios(id_uni_f, lesao_inflamatoria_ft_153_arr)
    contaminacao_nao_gi_ft_163_arr  = processar_dicionarios(id_uni_f, contaminacao_nao_gi_ft_163_arr)
    contaminacao_gastrointestinal_e_b_ft_179_arr = processar_dicionarios(id_uni_f, contaminacao_gastrointestinal_e_b_ft_179_arr)
    estados_anormais_ou_patologicos_ft_194_arr = processar_dicionarios(id_uni_f, estados_anormais_ou_patologicos_ft_194_arr)
    contusao_ft_765_arr = processar_dicionarios(id_uni_f, contusao_ft_765_arr)
    miopatia_ft_170_arr = processar_dicionarios(id_uni_f, miopatia_ft_170_arr)
    contusao_fratura_ft_636_arr = processar_dicionarios(id_uni_f, contusao_fratura_ft_636_arr)
    calo_no_peito_ft_159_arr =  processar_dicionarios(id_uni_f, calo_no_peito_ft_159_arr)
    contaminacao_papo_pendular_ft_688_arr = processar_dicionarios(id_uni_f, contaminacao_papo_pendular_ft_688_arr)
    lesao_traumatica_antiga_ft_135_arr = processar_dicionarios(id_uni_f, lesao_traumatica_antiga_ft_135_arr)
    processo_inflamatorio_ft_114_arr = processar_dicionarios(id_uni_f, processo_inflamatorio_ft_114_arr)
    canibalismo_ft_106_arr = processar_dicionarios(id_uni_f, canibalismo_ft_106_arr)
    necrose_caseosa_ft_139_arr = processar_dicionarios(id_uni_f, necrose_caseosa_ft_139_arr)
    
    
    
    new_dataFrame["CHAVE"] = id_uni_f
    new_dataFrame["FT"] = ft_arr_r
    new_dataFrame["FP"] = fp_arr_

    new_dataFrame["ASPECTO_REPUGNANTE_FT_112"] = aspect_repug_arr
    new_dataFrame["SEPTICEMIA_FT_130"] = septicemia_arr
    new_dataFrame["CAQUEXIA_FT_105"] = caquexia_arr
    new_dataFrame["SINDROME_ASCITE_FT_157"] = sindrome_ascite_arr

    new_dataFrame["LESAO_DE_PELE_FP_251"] = lesao_pele_arr
    new_dataFrame["CONTAMINACAO_GASTROINTESTINAL_E_B_FP_179"] = contaminacao_b_arr

    
    new_dataFrame["AEROSSACULITE_FP_220"]  = aerosaculite_arr
    new_dataFrame["ARTRITE_FP_226"] = artrite_arr
    
    
    new_dataFrame["CELULITE_FP_245"] = celulite_arr
    new_dataFrame["LESAO_DE_PELE_FP_251"] = lesao_pele_arr_251
    new_dataFrame["LESAO_INFLAMATORIA_FP_253"] = lesao_inflamatoria_arr
    new_dataFrame["SINDROME_ASCITE_FP_256"] = sindrome_ascite_arr     # (antes: "SIDROME_ASCIT")
    new_dataFrame["LESAO_TRAUMATICA_ANTIGA_FP_299"] = lesao_trau_antiga_arr
    new_dataFrame["ABSCESSO_FT_101"]                       = abscesso_ft_101_arr
    new_dataFrame["ARTRITE_FT_102"]                        = artrite_ft_102_arr
    new_dataFrame["ASCITE_FT_104"]                         = ascite_ft_104_arr
    new_dataFrame["CELULITE_FT_108"]                       = celulite_ft_108_arr
    new_dataFrame["COLIBACILOSE_FT_109"]                   = colibacilose_ft_109_arr
    new_dataFrame["DERMATOSE_FT_110"]                      = dermatose_ft_110_arr
    new_dataFrame["NEOPLASIA_FT_117"]                      = neoplasia_ft_117_arr
    new_dataFrame["TUMORES_FT_131"]                        = tumores_ft_131_arr
    new_dataFrame["PERICARDITE_FT_132"]                    = pericardite_ft_132_arr
    new_dataFrame["LESAO_DE_PELE_FT_151"]                  = lesao_de_pele_ft_151_arr
    new_dataFrame["SINDROME_HEMORRAGICA_FT_152"]           = sindrome_hemorragica_ft_152_arr
    new_dataFrame["LESAO_INFLAMATORIA_FT_153"]             = lesao_inflamatoria_ft_153_arr
    new_dataFrame["CALO_NO_PEITO_FT_159"]                  = calo_no_peito_ft_159_arr
    new_dataFrame["CONTAMINACAO_NAO_GI_FT_163"]            = contaminacao_nao_gi_ft_163_arr
    new_dataFrame["CONTAMINACAO_GASTROINTESTINAL_E_B_FT_179"] = contaminacao_gastrointestinal_e_b_ft_179_arr
    new_dataFrame["ESTADOS_ANORMAIS_OU_PATOLOGICOS_FT_194"]   = estados_anormais_ou_patologicos_ft_194_arr
    
    new_dataFrame["CONTAMINACAO_PAPO_PENDULAR_FT_688"]     = contaminacao_papo_pendular_ft_688_arr
    new_dataFrame["CONTUSAO_FT_765"]                       = contusao_ft_765_arr
    new_dataFrame["AEROSSACULITE_FT_103"] = arsl_ft_arr
    new_dataFrame["MIOPATIA_FT_170"]                       = miopatia_ft_170_arr
    new_dataFrame["CONTUSAO_FRATURA_FT_636"]               = contusao_fratura_ft_636_arr




    new_dataFrame["LESAO_TRAUMATICA_ANTIGA_FT_135"]        = lesao_traumatica_antiga_ft_135_arr
    new_dataFrame["PROCESSO_INFLAMATORIO_FT_114"]          = processo_inflamatorio_ft_114_arr
    new_dataFrame["CANIBALISMO_FT_106"]                    = canibalismo_ft_106_arr
    new_dataFrame["NECROSE_CASEOSA_FT_139"]                = necrose_caseosa_ft_139_arr
    
    
    




    # lista_c = [(va, id) for id, va in zip(dc_pr_arr, id_uni_f)]


    print("Salvando arquivo...")
    new_dataFrame.to_csv(f"{file[0]}/sif_tabela{get_date_now()}.csv", mode="w", index=False)
    # input("Arquivo salvo com sucesso...")

main()




# # chaves principais
# new_dataFrame["CHAVE"] = id_uni_f
# new_dataFrame["FT"]    = ft_arr_r
# new_dataFrame["FP"]    = fp_arr_

# # ============================
# # FT (TOTAL) — arrays únicos
# # ============================


# # ============================
# # FP (PARCIAL) — arrays únicos
# # ============================
# new_dataFrame["CANIBALISMO_FP_206"]                    = canibalismo_fp_206_arr
# new_dataFrame["AEROSSACULITE_FP_220"]                  = aerossaculite_fp_220_arr -ok 
# new_dataFrame["PERICARDITE_FP_224"]                    = pericardite_fp_224_arr 
# new_dataFrame["ABSCESSO_FP_225"]                       = abscesso_fp_225_arr
# new_dataFrame["ARTRITE_FP_226"]                        = artrite_fp_226_arr -ok 
# new_dataFrame["CELULITE_FP_245"]                       = celulite_fp_245_arr -ok 
# new_dataFrame["PROCESSO_INFLAMATORIO_FP_248"]          = processo_inflamatorio_fp_248_arr
# new_dataFrame["LESAO_DE_PELE_FP_251"]                  = lesao_de_pele_fp_251_arr -ok 
# new_dataFrame["LESAO_INFLAMATORIA_FP_253"]             = lesao_inflamatoria_fp_253_arr -ok 
# new_dataFrame["SINDROME_ASCITE_FP_256"]                = sindrome_ascite_fp_256_arr  -ok 
# new_dataFrame["CALO_NO_PEITO_FP_260"]                  = calo_no_peito_fp_260_arr
# new_dataFrame["ESTADOS_ANORMAIS_OU_PATOLOGICOS_FP_294"]= estados_anormais_ou_patologicos_fp_294_arr
# new_dataFrame["LESAO_TRAUMATICA_ANTIGA_FP_299"]        = lesao_traumatica_antiga_fp_299_arr  -ok 
# new_dataFrame["DERMATOSE_FP_235"]                      = dermatose_fp_235_arr
# new_dataFrame["MIOPATIA_FP_271"]                       = miopatia_fp_271_arr
# new_dataFrame["SALPINGITE_FP_244"]                     = salpingite_fp_244_arr
# new_dataFrame["LESAO_TRAUMATICA_RECENTE_FP_296"]       = lesao_traumatica_recente_fp_296_arr
# new_dataFrame["TENDINITE_FP_249"]                      = tendinite_fp_249_arr
