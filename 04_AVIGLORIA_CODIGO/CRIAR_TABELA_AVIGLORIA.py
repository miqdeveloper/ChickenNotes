from calendar import c
from collections import OrderedDict
from glob import glob
from hmac import new
from math import e, nan
from os import remove
import pandas as pd
import re, ast

from warnings import simplefilter
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

file_execel = "ArquivosCSV/TABELA_01.csv"

# df_2 = pd.read_csv(file_execel, encoding="utf-8", index_col=0)   
df = pd.read_csv(file_execel, encoding="utf-8")

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
    pattern = r'^\d+-\d+$'
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
    
id_unic_arr = []
id_unic_s = []
key_integrado_arr =  []
nome_integrado_arr = []
alojamento_arr = []
abate_arr = []
vencimento_arr = []
instalacao_arr  = []
sexo_arr = []
linhagem_arr = []
qtd_alojada_arr = []
dt_emissao_arr = []
premix_arr = []
avicola_arr = []
idade_matriz_arr = []
n_camas_arr = []

total_e_arr = []
total_d_arr = []
total_l_arr = []
total_c_arr = []


liquido_arr = []
peso_med_arr = []
idade_med_arr = []
mortalidade_arr = []
conversao_ali_arr = []
conversao_ali_ajustada_arr = []
densidade_arr = []
ganho_diario_arr = []
kcal_arr = []
diferenca_sobre_aves_arr =  []

cat_s_tabela_arr = []
cat_s_dia_arr = []
cat_s_ps_real_arr = []

mortalidade_tabela_ii_arr = []
mortalidade_tabela_ii_dia_arr = []
mortalidade_tabela_ii_ps_real_arr = []

gpd_tabela_iii_arr = []
gpd_tabela_iii_dia_arr = []
gpd_tabela_iii_ps_real_arr = []

taxa_liquida_arr = []
taxa_liquida_dia_arr = []
taxa_liquida_ps_real_arr = []
valor_por_cabeça_arr = []

custo_fomento_arr = []

custo_carregamento_arr_tmp = []
custo_carregamento_arr_f = []

industrializacao_racao_arr_tmp = []
q_tec_racao_arr = []

transporte_frango_vivo_arr = []
extorno_icms_arr = []

ret_b_calc_arr = []
ret_valor_arr = []

valor_bruto_arr = []
bonificacao_ch_arr = []
bonificacao_ch_p_arr = []

bonificacao_arr = []
descontos_arr = []

imposto_f_arr = []
odc_arr = []

cms_arr = []

valor_liquido_arr = []

total_dois_arr = []

test_arr = []

new_dataFrame= pd.DataFrame()

for index, row in df.iterrows():
    #row[0] - se refere ao nome do arquivo - #row[1] -  se refere ao conteudo da linha  
    line_item, content_line = row[0], row[1]
    new_string = remove_chars_s_points(content_line)
    
    # print(new_string)

    separe_id_ = remove_chars(line_item)
    # separe_id_ = separe_id_.replace("=", "").replace(" ", "")
    if "-" in separe_id_:
        if filter_pattern(separe_id_):
            id_unic_s.append(separe_id_)
    
    # # Nome Integrado
    if "Relatório de Fechamento do Contrato" in new_string or "Relatorio de Fechamento do Contrato" in new_string or " Relat rio de Fechamento do Contrato" in new_string or " Fechamento do Contrato" in new_string or "Relat(cid:243)rio de Fechamento do Contrato" in new_string:
        nis_s = (new_string)
        
        nis_s = nis_s.replace("Relatório de Fechamento do Contrato", "Relatorio de Fechamento do Contrato").replace("Relat(cid:243)rio de Fechamento do Contrato", "Relatorio de Fechamento do Contrato").split("Relatorio de Fechamento do Contrato")
        nome_integrado_final = nis_s[1].replace(":", "").split("-")[1]
        
        chave_integrado_final = nis_s[1].replace(":", "").split("-")[0].replace(" ", "")

        # CHAVE DO INTEGRADO
        key_integrado_arr.append(chave_integrado_final.replace(" ", ""))
        nome_integrado_arr.append(nome_integrado_final)

    if "Alojamento" in new_string:
        alj_s = (new_string.split("Sexo")[0])
        alj_f = (alj_s.replace("Alojamento: ", "").replace(" ", ""))
        
        alojamento_arr.append(alj_f)
        
        
    if "Abate" in new_string:
        abt_s = new_string
        # print(abt_s)
        abt_f = (abt_s.split('Linhagem:')[0].replace(' ', ''))
        abt_f = abt_f.replace('Abate:', '')
        abate_arr.append(abt_f)

    if "Vencimento" in new_string:
        vncm_s = new_string
        vncm_f = (vncm_s.split('Qtdade.')[0]).replace("Vencimento:", "").replace(" ", "")
        vencimento_arr.append(vncm_f)
        
    if "Instalacao" in new_string:
        instl_s = new_string
        instl_f = instl_s.replace("Instalacao:", "")
        instalacao_arr.append(instl_f)

    if "Sexo" in new_string:
        sxo_s = new_string
        sxo_s = (sxo_s.split('Premix:')[0])
        sxo_f = sxo_s.split('Sexo:')
        # print(sxo_f)
        sxo_f_ = sxo_f
        nc = len(sxo_f_)
        if nc == 2:
            sxo_f = sxo_f_[1].replace(" ", "")
        if nc == 1:
            sxo_f = sxo_f_[0].replace(" ", "")
        
    #     # print(sxo_f)
        sexo_arr.append(sxo_f)

    if "Linhagem" in new_string:
        
        lnhg_s = new_string
        nc = len(lnhg_s.split("Linhagem:")) #.replace("Avícola:", "Avicola:").split("Avicola:"))[0]
        
        lnhg_f = (lnhg_s.split("Linhagem:")[1].replace("Avícola:", "Avicola:").replace("Av(cid:237)cola:", "Avicola:").split("Avicola:"))[0]
        
        # if nc == 1:
        #     print(lnhg_f.replace("Avícola:", "Avicola:").split("Avicola:"))
        #     pass
        # if nc == 2:
        #     # print(lnhg_s)
        #     pass
        # if nc == 3:
        #     print(lnhg_s)
        linhagem_arr.append(lnhg_f)
        
    if "Qtdade. Aloja" in new_string:

        qtd_alj = (new_string.split("Idade Matriz:")[0])
        qtd_alj_f = qtd_alj.split("Qtdade. Alojada:")[-1].replace(" ", "")
        qtd_alojada_arr.append(qtd_alj_f)
    
    if "Data Emissão" in new_string or "Data Emissao" in new_string or "Data Emissªo" in new_string :
        de_s_f = new_string.replace("Data Emissão", "Data Emissao").replace("Data Emissao", "Data Emissao").replace("Data Emissªo", "Data Emissao")
        de_s_f = (de_s_f.split("Data Emissao")[-1]).replace(": ", "")
        dt_emissao_arr.append(de_s_f)

    if "Premix" in new_string:
        premix_s = new_string.split("Premix: ")[-1]
        premix_s = premix_s.split("Visitas: ")[0]
        premix_arr.append(premix_s)

    if "Avícola" in new_string or "Avicola" in new_string or "Av(cid:237)cola" in new_string: 
        # print(new_string)
        avicola_s_f = (new_string.replace("Avícola:", "Avicola:").replace("Av(cid:237)cola", "Avicola").split("Avicola:")[-1])
        avicola_arr.append(avicola_s_f)
    
    if "Idade Matriz" in new_string:
        idmz_s = (new_string.replace("No Camas:", " ").replace(f"N” Camas:", "").replace(f"Nº Camas:", "").split("Idade Matriz")[-1])
        idmz_s = remove_empty_spaces(idmz_s.split(" "))[1:]
        n_c = len(idmz_s)

        if n_c == 1:
            idmz_s = idmz_s[0]
        if n_c == 2:
            idmz_s = idmz_s[0]
        if n_c == 3:
            idmz_s = idmz_s[0]
            # print(idmz_s)
        if not idmz_s:
            idmz_s = None

        idade_matriz_arr.append(idmz_s)

    if "Camas" in new_string:

        n_camas = new_string
        # print(n_camas)
        n_camas_s  = n_camas[-1].replace("No Camas:", ", ").replace("N” Camas:", ", ").replace("Nº Camas:", ", ").replace(":", " ")
        # print(n_camas_s)
        # n_camas_s = n_camas_s.split(", ")[-1]
        n_camas_f = n_camas_s.replace(" ", "")

        if not n_camas_f:
            n_camas_f = "nan"
        # print(n_camas_f)
        n_camas_arr.append(n_camas_f)
    
    if "Total............." in new_string:
        total_s = (new_string)
        # print(line_item, total_s)
        total_s = total_s.split("|")[-1]
        total_s = total_s.replace("Total", "").replace(" ", "  ").replace("..............", "").replace(":", "")
        total_s = remove_empty_spaces(total_s.split("  "))
        
        # total Envio
        total_e = total_s[0] 
        # Total Devolução 
        total_d = total_s[1]
        # Total Liquido 
        total_l = total_s[2]
        # Total Cons/fase
        total_c = total_s[3]
        
        # total_s = total_s.replace("Total.............:", "").replace(" ", "")
        total_e_arr.append(total_e)
        total_d_arr.append(total_d)
        total_l_arr.append(total_l)
        total_c_arr.append(total_c)
        
    # FILTRA O VALOR LIQUIDO
    if "Total Bonif" in new_string:
        liquido_s = (new_string)
        liquido_s = liquido_s.replace("Líquido", "Liquido").replace("L(cid:237)quido:","Liquido").split("Liquido")[-1]
        liquido_s = liquido_s.replace(":", "").replace("|", "").replace(" ", "")
        #identifica valores com ## no pdf 
        if "#" in liquido_s:
            liquido_s = liquido_s.replace("####", "nan").replace("###", "nan").replace("#####", "nan").replace("#", "nan")
        #     print(content_line, line_item)
        
        liquido_arr.append(liquido_s)
    
    if "Peso Médio" in new_string or "Peso Medio" in new_string or "Peso M" in new_string:
        psmedio_s = new_string
        
        # psmedio_s = (psmedio_s.split(", ")[-1])
        psmedio_s = psmedio_s.replace("Peso Médio", "Peso Medio").replace("Peso MØdio", "Peso Medio")
        psmedio_s = psmedio_s.split("Peso Medio")
        
        
        psmedio_s = psmedio_s[-1].replace(":", "").replace(".", "").replace(" ", "")
        
        # psmedio_f = remove_points(psmedio_s)
        # print(new_string)
        # psmedio_f = psmedio_s.replace(" ", "")
        # if not psmedio_f:
        #     print("NAO TEM:", new_string)
        # psmedio_f = {"id":psmedio_s_, "value": psmedio_f}
        
        # psmedio_f = psmedio_f["value"]
        # print(new_string ,psmedio_f["value"])
        
        peso_med_arr.append(psmedio_s)
        
    if ("Idade Média" in new_string or "Idade MØdia" in new_string or "Idade Media" in new_string):
        id_m_s_id = new_string        
        id_m_s = new_string.split(", ")
        id_m_s = id_m_s[-1].replace("Idade MØdia", "Idade Media").replace("Idade Média", "Idade Media") 
        id_m_s = id_m_s.split("Idade Media")
        id_m_s = remove_points(id_m_s[-1]).replace(" ", "")
        
        idade_med_arr.append(id_m_s)
        
    if "Mortalidade......" in new_string:
        mtd_s = new_string
        mtd_s_ = new_string.split("Mortalidade")[-1]
        mtd_s = mtd_s_
        # print(mtd_s)
        mtd_s = mtd_s.replace(".", "").replace(":", "")
        
        
        mortalidade_arr.append(mtd_s)
        
    if "Conversão Alimentar...." in new_string or "Conversao Alimentar...." in new_string or "Conversªo Alimentar....." in new_string :
        cns_arr = new_string
        cns_arr_ = cns_arr[0]
        
        cns_arr = cns_arr.replace("Conversªo Alimentar","").replace("Conversão Alimentar", "ConversaoAlimentar").replace("Conversao Alimentar", "ConversaoAlimentar").split("ConversaoAlimentar")
        # print(cns_arr[-1])
        cns_arr_ = str(remove_points(cns_arr[-1])).split(" ")[-1]
        conversao_ali_arr.append(cns_arr_)
           
    if "Conversão Alimentar Ajustada" in new_string or "Conversao Alimentar Ajustada:" in new_string or "Conversªo Alimentar Ajustada" in new_string:
        caa_s = new_string.replace("Conversªo Alimentar Ajustada", "Conversao Alimentar Ajustada").replace("Conversão Alimentar Ajustada", "Conversao Alimentar Ajustada")
        caa_s = new_string.split("Conversão Alimentar Ajustada")[-1].split("Conversao Alimentar Ajustada:")[-1]
        caa_s = caa_s.replace(":", "").strip()
        caa_s = separar_numeros(caa_s)
        
        conversao_ali_ajustada_arr.append(caa_s)
        
    if "Densidade" in new_string:
        dsn_s = new_string
        dsn_s = dsn_s
        dsn_s = dsn_s.split("Densidade")[-1]
        dsn_s = dsn_s.replace(".", "").replace(":", "").replace(" ", "")
        densidade_arr.append(dsn_s)
    
    if "Ganho Diário" in new_string or "Ganho Diario" in new_string or "Ganho DiÆrio" in new_string or "Ganho Di(cid:237)rio" in new_string:
        
        gdar_s = new_string
        # gdar_s_ = gdar_s[0]
        gdar_s = gdar_s.replace("Ganho Diário", "GanhoDiario").replace("Ganho Di(cid:237)rio","GanhoDiario").replace("Ganho DiÆrio", "GanhoDiario")
        gdar_s = gdar_s.replace("Ganho Diario", "GanhoDiario").split("GanhoDiario")[-1]
        gdar_s = remove_points(gdar_s).replace(" ", "")
        
        
        ganho_diario_arr.append(gdar_s)
        
    if "K/Cal" in new_string:
        
        kcal_s = new_string
        kcal_s = kcal_s.split("K/Cal")[-1]
        kcal_s = remove_points(kcal_s).replace(" ", "")
        kcal_arr.append(kcal_s)
        
    if "Diferença / Sobra Aves...." in new_string or "Diferenca / Sobra Aves...." in new_string or "Diferença/Sobra Aves...." in new_string or "Diferença/SobraAves...." in new_string or "Diferen(cid:231)a / Sobra Aves" in new_string:
        
        dsa_s = new_string
        dsa_s = dsa_s.replace("Diferença / Sobra Aves", "Diferenca/SobraAves").replace("Diferenca / Sobra Aves", "Diferenca/SobraAves").replace("Diferença/Sobra Aves", "Diferenca/SobraAves").replace("Diferença/SobraAves", "Diferenca/SobraAves").replace("Diferen(cid:231)a / Sobra Aves", "Diferenca/SobraAves")
        dsa_s = dsa_s.split("Diferenca/SobraAves")[-1]
        dsa_s = remove_points(dsa_s).replace(" ", "")
        
        diferenca_sobre_aves_arr.append(dsa_s)
    
    # FILTRA C.A. TABELA I and Dia and PS. REAAL
    if "C.A. Tabela I....." in new_string or "C.A. Tabela I...........:" in new_string or "C.A. Tabela I" in new_string:
        
        cat_s_tabela = None
        cat_s_dia = None
        cat_s_ps_real = None 
        
        cat_s = new_string.replace("-", "").replace("|", "").replace(":", " ").replace("C.A. Tabela I", "C.A.TabelaI")
        cat_s = cat_s.split("C.A.TabelaI...........")[-1]
        cat_s = remove_empty_spaces(cat_s.split(" "))
        n_c = len(cat_s)
        
        #C.A TABELA and DIA and PS. REAAL
        if n_c == 3:
            cat_s_tabela = cat_s[0]
            cat_s_dia = cat_s[1]
            cat_s_ps_real = cat_s[2]
            
        
        # if n_c == 2:
        #     cat_s = cat_s[0]
        #     print(cat_s)
        #     pass
    
        cat_s_tabela_arr.append(cat_s_tabela)
        cat_s_dia_arr.append(cat_s_dia)
        cat_s_ps_real_arr.append(cat_s_ps_real)
            
        # print(cat_s)

    if "Mortalidade Tabela II" in new_string:
        mrt_t_s_f = None
        mrt_t_s_d = None
        mrt_t_s_real = None
         
        mrt_t_s = new_string.replace("Mortalidade Tabela II...:", "").replace("|", "").replace(":", " ").split("Ret.")[0]
        mrt_t_s = (mrt_t_s.split(" "))
        
        # print(len(mrt_t_s))
        if len(mrt_t_s) > 5:
            # Mortalidade Tabela II
            mrt_t_s_f = (remove_empty_spaces(mrt_t_s)[0])
            
            # Mortalidade Tabela II Dia 
            mrt_t_s_d = (remove_empty_spaces(mrt_t_s)[1])
            
            # Mortalidade Tabela II ps real
            mrt_t_s_real = (remove_empty_spaces(mrt_t_s)[2])
            
        
        else:
            # Mortalidade Tabela II
            mrt_t_s_f = (mrt_t_s[1])
            
            # Mortalidade Tabela II dia
            mrt_t_s_d = "nan"
            
            # Mortalidade Tabela II ps real
            mrt_t_s_real = "nan"
            
        mortalidade_tabela_ii_dia_arr.append(mrt_t_s_d)
        mortalidade_tabela_ii_arr.append(mrt_t_s_f)
        mortalidade_tabela_ii_ps_real_arr.append(mrt_t_s_real)
        
    if "G.P.D. Tabela III" in new_string:
        gpd_t_ii_f = None
        gpd_t_ii_dia_f = None
        gpd_t_ii_ps_real_f = None
        
        gpd_t_ii = new_string.replace("G.P.D. Tabela III.......:", "").split("Cat.")[0]
        gpd_t_ii = gpd_t_ii.replace("|", "").split(" ")
        gpd_t_ii = remove_empty_spaces(gpd_t_ii)
        # gpd_t_ii = gpd_t_ii[0]

        if len(gpd_t_ii) > 1:
            
            #G.P.D. Tabela III 
            gpd_t_ii_f = (gpd_t_ii[0])
            
            #G.P.D. Tabela III Dia
            gpd_t_ii_dia_f = (gpd_t_ii[1])
            
            #G.P.D. Tabela III PS. REAL
            gpd_t_ii_ps_real_f = (gpd_t_ii[2])
            
        else:
            #G.P.D. Tabela III
            gpd_t_ii_f = gpd_t_ii[0]
            #G.P.D. Tabela III Dia
            gpd_t_ii_dia_f = "nan"
            #G.P.D. Tabela III PS. REAL
            gpd_t_ii_ps_real_f = "nan"
        
        gpd_tabela_iii_arr.append(gpd_t_ii_f)
        gpd_tabela_iii_dia_arr.append(gpd_t_ii_dia_f)
        gpd_tabela_iii_ps_real_arr.append(gpd_t_ii_ps_real_f)
        
        # Taxa Liquida - DIA - PS REAL
    if "Taxa Liquida" in new_string:
        taxa_liq_f = None
        taxa_liq_dia_f = None
        taxa_liq_ps_real_f = None
        
        taxa_liq_s = (new_string).replace("Taxa Liquida............:", "").replace("Taxa Liquida", "").replace("|", "").replace(":", " ").replace(".", "")
        taxa_liq_s = remove_empty_spaces(taxa_liq_s.split(" "))
        if len(taxa_liq_s) > 1:
            # Taxa Liquida
            taxa_liq_f = taxa_liq_s[0]
            taxa_liq_dia_f = taxa_liq_s[1]
            taxa_liq_ps_real_f = taxa_liq_s[2]
        else:
            # Taxa Liquida
            taxa_liq_f = taxa_liq_s[0]
            taxa_liq_dia_f = "nan"
            taxa_liq_ps_real_f = "nan"
        
        taxa_liquida_arr.append(taxa_liq_f)
        taxa_liquida_dia_arr.append(taxa_liq_dia_f)
        taxa_liquida_ps_real_arr.append(taxa_liq_ps_real_f)
        
    if "Valor Por cabeça..." in new_string or "Valor Por cabeca..." in new_string or "Valor Por cabe(cid:231)a..." in new_string:
        vlr_pr_cb = (new_string)
        vlr_pr_cb = vlr_pr_cb.replace("Valor Por cabeça", "Valor Por cabeça").replace("Valor Por cabeca", "Valor Por cabeça").replace("Valor Por cabe(cid:231)a", "Valor Por cabeça")
        vlr_pr_cb =  vlr_pr_cb.split("Valor Por cabeça")[-1]
        vlr_pr_cb = vlr_pr_cb.replace(".", "").replace(":", "").replace(" ", "")
        valor_por_cabeça_arr.append(vlr_pr_cb)
        
        # ret. - B.calc. - valor
    if "Ret." in new_string:
        ret_b_calc_f = None
        ret_valor_f = None
        
        ret_b_calc_s = new_string.split("Ret.")[-1]
        ret_b_calc_s = ret_b_calc_s.replace("|", "")
        ret_b_calc_s = remove_empty_spaces(ret_b_calc_s.split(" "))
        
        # B. calc 
        ret_b_calc_f = ret_b_calc_s[1]
        
        #Valor
        ret_valor_f = ret_b_calc_s[-1]
        
        ret_b_calc_arr.append(ret_b_calc_f)
        ret_valor_arr.append(ret_valor_f)
        
    if "CUSTO FOMENTO" in new_string:
        custo_fomento_s = (new_string.split("|")[0])
        custo_fomento_s = custo_fomento_s.replace("9 CUSTO FOMENTO", "").replace(".", "").replace(":", "").replace(" ", "")
        id_l = (line_item.replace(".pdf", ""))
        custo_fomento_arr.append({"Data": custo_fomento_s, "id": id_l})

    if "CUSTO CARREGAMENTO" in new_string:
        custo_carregamento_s = (new_string.split("|")[0])
        
        id_l = (line_item.replace(".pdf", ""))
        custo_carregamento_s = custo_carregamento_s.replace("10 CUSTO CARREGAMENTO", "").replace(".", "").replace(":", "").replace(" ", "")
        
        custo_carregamento_arr_tmp.append({"Data": custo_carregamento_s, "id": id_l})
        
    if "INDUSTRIALIZACAO DE RACAO" in new_string or "INDUSTRIALIZAÇÃO DE RAÇÃO" in new_string:
        industrializacao_racao_s = (new_string.split("|")[0])
        industrializacao_racao_s = industrializacao_racao_s.replace("18 INDUSTRIALIZACAO DE RACAO", "").replace(".", "").replace(":", "").replace(" ", "")
        id_l = (line_item.replace(".pdf", ""))
        industrializacao_racao_f = {"Data": industrializacao_racao_s, "id": id_l}
        industrializacao_racao_arr_tmp.append(industrializacao_racao_f)
        
    if "QUEBRA TECNICA RACAO" in new_string:
        quebra_tecnica_racao_s = (new_string.split("|")[0])
        quebra_tecnica_racao_s = quebra_tecnica_racao_s.replace("19 QUEBRA TECNICA RACAO", "").replace(".", "").replace(":", "").replace(" ", "")
        id_l = (line_item.replace(".pdf", ""))
        quebra_tecnica_racao_f = {"Data": quebra_tecnica_racao_s, "id": id_l}
        q_tec_racao_arr.append(quebra_tecnica_racao_f)
        
    if "TRANSPORTE FRANGO VIVO" in new_string:
        transporte_frango_vivo_s = new_string.split("|")[0]
        transporte_frango_vivo_s = transporte_frango_vivo_s.replace("20 TRANSPORTE FRANGO VIVO", "").replace(".", "").replace(":", "").replace(" ", "")
        id_l = line_item.replace(".pdf", "")
        transporte_frango_vivo_f = {"Data": transporte_frango_vivo_s, "id": id_l}
        transporte_frango_vivo_arr.append(transporte_frango_vivo_f)

    if "EXTORNO ICMS" in new_string:
        extorno_icms_s = new_string.split("|")[0]
        extorno_icms_s = extorno_icms_s.replace("21 EXTORNO ICMS", "").replace(".", "").replace(":", "").replace(" ", "")
        id_l = line_item.replace(".pdf", "")
        extorno_icms_f = {"Data": extorno_icms_s, "id": id_l}
        extorno_icms_arr.append(extorno_icms_f)
        
    if "Valor Bruto...." in new_string:
        id_l = line_item.replace(".pdf", "")
        vl_b = new_string.split("|")[-1]
        vl_b = vl_b.replace("Valor Bruto", "").replace(":", "").replace(".", "").replace(" ", "")
        valor_bruto_arr.append(vl_b)
        # print({'id': id_l, 'data':new_string})
        
    if "Bonificação Checklist" in new_string or "bonificação Checklist" in new_string  or "Bonifica(cid:231)ªo Checklist....." in new_string or "Bonificacao Checklist..." in new_string:
        bonif_checklist = new_string.split("|")[-1]
        bonif_checklist = bonif_checklist.replace("Bonificacao Checklist...", "").replace("Bonifica(cid:231)ªo Checklist", "").replace("Bonificação Checklist", "").replace(":", "").replace(".", "").split(" ")
        bonif_checklist = remove_empty_spaces(bonif_checklist)
        # id_l = line_item.replace(".pdf", "")
        # bnf_s = {"Data": bonif_checklist[1], "id": id_l}
        bonificacao_ch_arr.append(bonif_checklist[1])
        bonificacao_ch_p_arr.append(bonif_checklist[0])
        
    if "Bonificação....." in new_string or "bonificação....." in new_string  or "Bonifica(cid:231)ªo....." in new_string or "Bonificacao....." in new_string:
        
        bnf_s = new_string.split("|")[-1]
        bnf_s = bnf_s.replace("Bonificação.....", "").replace("bonificação.....", "").replace("Bonifica(cid:231)ªo.....", "").replace("Bonificacao.....", "").replace(":", "").replace(".", "").replace(" ", "")
        bonificacao_arr.append(bnf_s)
    
    if "Descontos......" in new_string:
        descontos_s = new_string.split("|")[-1]
        descontos_s = descontos_s.replace("Descontos", "").replace(":", "").replace(".", "").replace(" ", "")
        descontos_arr.append(descontos_s)

    if "Imposto (FunRural)....." in new_string:
        imposto_f = new_string.split("|")[-1]
        imposto_f = imposto_f.replace("Imposto (FunRural)", "").replace(":", "").replace(".", "").replace(" ", "")
        imposto_f_arr.append(imposto_f)
        
    if "Outros Descontos (Documentos).." in new_string:
        odc_s = new_string.split("|")[-1]
        odc_s = odc_s.replace("Outros Descontos (Documentos)..:", "").replace(" ", "")
        odc_arr.append(odc_s)
        
    if "Valor Líquido.." in new_string or "Valor L(cid:237)quido...." in new_string or "Valor Liquido...." in new_string:
        valor_l = new_string.split("|")[-1]
        valor_l = valor_l.replace("Valor Liquido", "").replace("Valor L(cid:237)quido", "").replace("Valor Líquido", "").replace(":", "").replace(".", "").replace(" ", "")
        id_l = line_item.replace(".pdf", "")
        valor_l = {"Data": valor_l, "id": id_l}
        valor_liquido_arr.append(valor_l)
    
    if "CONVERSÃO META DA SEMANA" in new_string or "CONVERSAO META DA SEMANA" in new_string or "CONVERSÃO MEDIA DA SEMANA" in new_string:
        # cms_f = None
        cms_s = new_string.split(", ")[1]
        if "META DA SEMANA" in str(cms_s) or ":" in str(cms_s):
            txt = re.sub(r"(SEMANA\s*)\d+", r"\1", cms_s)
            cms_f = txt.replace("CONVERSAO META DA SEMANA", "").replace("CONVERSÃO META DA SEMANA", "").replace("CONVERSÃO MEDIA DA SEMANA", "").replace(":", "").replace(" ", "").replace("MACHO", "").replace("FEMEA", "")
            if not cms_f:
                cms_f = "nan"
            # print(cms_f)
            id_l = line_item.replace(".pdf", "")

        if not "META DA SEMANA" in cms_s:
            cms_s = new_string.split(", ")[0]
            cms_s = cms_s.replace("CONVERSAO META DA", ";").replace("CONVERSÃO META DA", ";").replace(":", "").replace("MACHO", "").replace("FEMEA", "")
            cms_s = cms_s.split(";")[-1]
            txt = re.sub(r"(SEMANA\s*)\d+", r"\1", cms_s)
            cms_f = txt.replace("SEMANA", "").replace(" ", "")
            id_l = line_item.replace(".pdf", "")
            if ',' not in str(cms_f):
                cms_s = (new_string.split(", ")[1])
                cms_f = cms_s.replace("CONVERSÃO MEDIA DA SEMANA", "").replace("CONVERSÃO MEDIA DA SEMANA", "").replace(":", "").replace(" ", "").replace("MACHO", "").replace("FEMEA", "")
                
            cms_f = {"Data": cms_f, "id": id_l}
        # print(cms_f)
            cms_arr.append(cms_f)
            
        # nc_/ = len(cms_s)
        
        # if nc_ == 2:
        #     # cms_s = (cms_s)
        #     cms_ss = (cms_s[0].replace("CONVERSÃO META DA SEMANA", "|"))
            
        #     pass
        # if nc_ == 3:
        #     # print(limpar_texto(cms_s[1]))
        #     pass
        # if nc_ == 4:
        #     # print(cms_s)
        #     pass
        # if nc_ == 5:
        #     # print(cms_s)
        #     pass
    
    if "Total:" in new_string:
        total_s = (new_string.split("|")[0])
        total_s = remove_empty_spaces(total_s.replace("Total:", "").split(" "))
        len_total = len(total_s)
        id_l = line_item.replace(".pdf", "").replace(" ", "")
        # print(id_l, total_s)
        total_dic = {'data':total_s, 'id':id_l}
        test_arr.append(total_dic)
        
        # if len_total == 1:
        #     total_s, intem_id = total_s[0], id_l
            
a_arr = []

id_unic_arr = list(OrderedDict.fromkeys(id_unic_s))
# key_integrado_arr = list(OrderedDict.fromkeys(key_integrado_arr))
# nome_integrado_arr = list(OrderedDict.fromkeys(nome_integrado_arr))
# name_arr = list(set(nome_integrado_arr))



cms_arr = processar_dicionarios(id_unic_arr, cms_arr)
custo_fomento_arr = processar_dicionarios(id_unic_arr, custo_fomento_arr)
custo_carregamento_arr_f = processar_dicionarios(id_unic_arr, custo_carregamento_arr_tmp)
industrializacao_racao_arr_tmp = processar_dicionarios(id_unic_arr, industrializacao_racao_arr_tmp)
q_tec_racao_arr = processar_dicionarios(id_unic_arr, q_tec_racao_arr)
transporte_frango_vivo_arr = processar_dicionarios(id_unic_arr, transporte_frango_vivo_arr)
extorno_icms_arr = processar_dicionarios(id_unic_arr, extorno_icms_arr)
valor_liquido_arr = processar_dicionarios(id_unic_arr, valor_liquido_arr)
# bonificacao_ch_arr = processar_dicionarios(id_unic_arr, bonificacao_ch_p_arr)

x = 0
f = len(test_arr)
for i in range(len(test_arr)):
    x = i+1
    if x > f:
        x = f
        
    if test_arr[i]['id'] == test_arr[x]['id']:
        a_arr.append(test_arr[i])
    if test_arr[i]['id'] not in a_arr:
        a_arr.append(test_arr[i])

# a_arr = list(OrderedDict.fromkeys(a_arr))
print("Contagem: a_arr", len(a_arr))
# print(id_unic_arr)

        
print("Contagem: id_unic_arr", len(id_unic_arr))
# print("Total contagem:", len(cms_arr))
# Save new_dataFrame
new_dataFrame["ID_INTEGRAD"] = id_unic_arr
# new_dataFrame["CHAVE_INTEGRADO"] = key_integrado_arr
# new_dataFrame["NOME_INTEGRADO"] = nome_integrado_arr
# new_dataFrame["ALOJAMENTO"] = alojamento_arr
# new_dataFrame["ABATE"] = abate_arr
# new_dataFrame["VENCIMENTO"] = vencimento_arr
# new_dataFrame["INSTALACAO"] = instalacao_arr
# new_dataFrame["SEXO"] = sexo_arr
# new_dataFrame["LINHAGEM"] = linhagem_arr
# new_dataFrame["QUANTIDADE_ALOJADA"] = qtd_alojada_arr
# new_dataFrame["DATA_DE_EMISSAO"] = dt_emissao_arr
# new_dataFrame["PREMIX"] = premix_arr
# new_dataFrame["AVICOLA"] = avicola_arr
# new_dataFrame["IDADE_MATRIZ"] = idade_matriz_arr
# new_dataFrame["NUMERO_CAMAS"] = n_camas_arr

# new_dataFrame["TOTAL_ENVIO"] = total_e_arr
# new_dataFrame["TOTAL_DEVOLUÇÃO"] = total_d_arr
# new_dataFrame["TOTAL_LIQUIDO"] = total_l_arr
# new_dataFrame["TOTAL_CONS/FASE"] = total_c_arr

# new_dataFrame["LIQUIDO"] = liquido_arr
# new_dataFrame["PESO_MEDIO"] = peso_med_arr
# new_dataFrame["IDADE_MEDIA"] = idade_med_arr
# new_dataFrame["MORTALIDADE"] = mortalidade_arr
# new_dataFrame["CONVERSAO_ALIMENTAR"] = conversao_ali_arr
# new_dataFrame["CONVERSAO_ALIMENTAR_AJUSTADA"] = conversao_ali_ajustada_arr
# new_dataFrame["DENSIDADE"] = densidade_arr
# new_dataFrame["GANHO_DIARIO"] = ganho_diario_arr
# new_dataFrame["K/CAL"] = kcal_arr
# new_dataFrame["DIFERENCA/SOBRE_AVES"] = diferenca_sobre_aves_arr

# new_dataFrame["C_A_TABELA_I"] = cat_s_tabela_arr
# new_dataFrame["C_A_TABELA_DIA"] = cat_s_dia_arr
# new_dataFrame["C_A_TABELA_PS_REAL"] = cat_s_ps_real_arr

# new_dataFrame["MORTALIDADE_TABELA_II"] = mortalidade_tabela_ii_arr
# new_dataFrame["MORTALIDADE_TABELA_II_DIA"] = mortalidade_tabela_ii_dia_arr
# new_dataFrame["MORTALIDADE_TABELA_II_PS_REAL"] = mortalidade_tabela_ii_ps_real_arr

# new_dataFrame["GPD_TABELA_III"] = gpd_tabela_iii_arr
# new_dataFrame["GPD_TABELA_III_DIA"] = gpd_tabela_iii_dia_arr
# new_dataFrame["GPD_TABELA_III_PS_REAL"] = gpd_tabela_iii_ps_real_arr

# new_dataFrame["TAXA_LIQUIDA"] = taxa_liquida_arr
# new_dataFrame["TAXA_LIQUIDA_DIA"] = taxa_liquida_dia_arr
# new_dataFrame["TAXA_LIQUIDA_PS_REAL"] = taxa_liquida_ps_real_arr
# new_dataFrame["VALOR_POR_CABEÇA"] = valor_por_cabeça_arr
# new_dataFrame["RET_B_CALC"] = ret_b_calc_arr
# new_dataFrame["RET_VALOR"] = ret_valor_arr
# new_dataFrame["CUSTO_FOMENTO"] = custo_fomento_arr
# new_dataFrame["CUSTO_CARREGAMENTO"] = custo_carregamento_arr_f
# new_dataFrame["INDUSTRIALIZACAO_RACAO"] = industrializacao_racao_arr_tmp
# new_dataFrame["QUEBRA_TECNICA_RACAO"] = q_tec_racao_arr
# new_dataFrame["TRANSPORTE_FRANGO_VIVO"] = transporte_frango_vivo_arr
# new_dataFrame["EXTORNO_ICMS"] = extorno_icms_arr

# new_dataFrame["VALOR_BRUTO"] = valor_bruto_arr
# new_dataFrame["BINIFICACAO_CHECKLIST_%"] = bonificacao_ch_p_arr
# new_dataFrame["BINIFICACAO_CHECKLIST"] = bonificacao_ch_arr
# new_dataFrame["BONIFICAÇÃO"] = bonificacao_arr
# new_dataFrame["DESCONTOS"] = descontos_arr
# new_dataFrame["IMPOSTO_FUNRURAL"] = imposto_f_arr
# new_dataFrame["OUTROS_DESCONTOS_DOCUMENTOS"] = odc_arr
# new_dataFrame["VALOR_LIQUIDO"] = valor_liquido_arr
# new_dataFrame["CONVERSAO_META_DA_SEMANA"] = cms_arr
# new_dataFrame["TOTAL_DOIS"] = total_dois_arr
# new_dataFrame["TOTAL_DOIS"] = a_arr



# for x_ in a_arr:
#     if x_['id'] not in id_unic_arr:
#         print(x_['id'])
print("Salvando arquivo...")
new_dataFrame.to_csv(r"ArquivosCSV/avigloria_tabela.csv",  index=False)
input("Arquivo salvo com sucesso...")