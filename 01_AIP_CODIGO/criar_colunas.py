import pandas as pd
from collections import OrderedDict
from datetime import datetime
import time, re

file_csv = r"output/saida.csv"

df = pd.read_csv(file_csv, sep=";", encoding="utf-8")


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
              ["Técnico", "Tecnico"],
              "Telefone",
              "E-mail",
              "Tipo Ventilação",
              ["Kg/m2", "Ka/m?", "Ko/m?", "Ka/m2", "Ko/mº"],
              "Material",
              "Aves/m2",
              "Qtde Abatida",
              "Mort. Total",
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

def get_date_now():
    date_now = datetime.now()
    d = str(date_now.strftime("""_%d_%m_%Y"""))
    return d

def remove_empty_spaces(lst):
    return list(filter(lambda item: item.strip() != '', lst))

def remove_chars(input_str: str) -> str:
    chars_to_remove = ["[", "\"", "'", "nan", "]", ":", ".pdf", "_","-","|",'“', "*", " —", "/", "*", "-", "—", "º", "?"]
    for char in chars_to_remove:
        input_str = input_str.replace(char, "")
    return input_str

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

def find_letters(input_str):
   pattern = r'\b[A-Z\s]+\b'  # Padrão para letras
   result = re.findall(pattern, input_str)
   return result

def find_numbers(input_str):
   pattern = r'\b\d+\b'  # Padrão para números
   result = re.findall(pattern, input_str)
   return result

def find_dates(text):
   date_pattern = r"\b(0[1-9]|1[0-9]|2[0-9]|3[01])/(0[1-9]|1[0-2])/([0-9]{2})\b"
   return re.findall(date_pattern, text)
     
for index, row in df.iterrows():
   line_text = str(row['text'])
   id_l = str(row['filename']).replace("_pg_1.tif", "").replace("_pg_2.tif", "").replace("_pg_3.tif", "").replace("_pg_4.tif", "").strip()
   # get chave unica
   key_s = str(row['filename'])
   key_s = key_s.replace("_pg_1.tif", "").replace("_pg_2.tif", "").replace("_pg_3.tif", "").replace("_pg_4.tif", "").strip()
   key_arr.append(key_s)
   
   # clifor 
   if (arr_filter[0] in line_text):
      clifor_s = line_text.split(" ")
      # print(clifor_s)
      clifor_s= clifor_s[:4]
      
      
      clifor_s  = remove_chars(str(remove_empty_spaces(clifor_s)))
      clifor_s = clifor_s.split(", ")
      n_s = len(clifor_s)
      
      clifor_f = clifor_s
      
      if n_s == 1:
         # DEVIDO AO ARQUIVO PDF NAO ESTAR PADRONIZADO EM RELACAO A QUALIDADE 
         
         # print(line_text)
         # print(clifor)
         # print("clifor", row['filename'], line_text)
         clifor_f = "nan"
         
      if n_s == 3:
         clifor_f = clifor_s[1]
         if clifor_f == "Integrado":
            clifor_f = clifor_s[2]
         # print(clifor_f)
         
      if n_s == 4:
         clifor_f =  clifor_s[1].strip()
         if clifor_f == "Integrado":
            clifor_f = clifor_s[2]
      
      clifor_f_ = {"Data": clifor_f, "id": id_l}
      # p;rint(clifor_f)
      # if "Integrado" in clifor_f:
      #    print("clifor", row['filename'], clifor_f)
         
      clifor_arr.append(clifor_f_)
      
      # clifor_arr.append(clifor)
   #Pedido
   if (arr_filter[1] in line_text):
      if "Pedido:" in line_text:
         pedido_s = remove_empty_spaces(remove_chars(line_text).split(" "))[:3]
         l_n = len(pedido_s)
         if l_n == 1:
            pass
         if l_n == 2:
            pedido_s = (line_text).replace("Pedido: ", "").replace("Pedido:", "").replace(" ", "")
            pedido_f = pedido_s
         if l_n == 3:
            pedido_s = pedido_s[1]
            if re.match(r'\d+', pedido_s):
               pedido_f = pedido_s
            if not re.match(r'\d+', pedido_s):
               if re.match(r'\bS7(?:[A-Z]+|\d+)\b', pedido_s):
                  pedido_f = pedido_s
               if "Pedido" in pedido_s or "pedido" in pedido_s:
                  pedido_f = remove_empty_spaces(remove_chars(line_text).split(" "))[2]

         # if l_n == 4:
         #    print((line_text))
         #    pass
         pedido_f = remove_chars(pedido_f)
         pedido_dic = {"Data": pedido_f, "id": id_l}
         
         arr_pedido.append(pedido_dic)
   #Municipio
   if (arr_filter[2] in line_text):
      municipio_s =  line_text.replace("Técnico","Tecnico").split("Tecnico")
      municipio_s = municipio_s[0].replace("Município", "Municipio").replace("Municipio", "").replace(":", "").replace(".", "").replace(";", "").strip()
      municipio_s = remove_chars(municipio_s)
      municipio_f = {"Data": municipio_s, "id": id_l}
      arr_municipio.append(municipio_f)
   #Data Alojamento
   if (arr_filter[3] in line_text):
      dtalj_s = (line_text).replace("Qtde Abatida:", "QtdeAbatida").split("QtdeAbatida")[0]
      dtalj_s  = remove_chars(dtalj_s.replace("Data Alojamento:"," ").replace("Data Alojamento","").replace("Data Alojamento", "").replace(":", "").replace(".", "").replace(";", "").strip())
      dtalj_s = remove_empty_spaces(dtalj_s.split(" "))
      
      l_n = len(dtalj_s)
      dtalj_f = dtalj_s
      if l_n == 1:
         dtalj_f = dtalj_s[0]
      if l_n == 2:
         dtalj_f = dtalj_s[1]
      if l_n >= 3:
         if not re.search(r"\d+", dtalj_s[0]):
            dtalj_f = dtalj_s[1]
         dtalj_f =  dtalj_s[0]
      
      
      dtalj_f = {
         "Data": dtalj_f,
         "id": id_l
      }
      arr_data_aloj.append(dtalj_f)
      # print("dtalj_s", dtalj_f)
   
   #Linhagem
   if (arr_filter[4] in line_text):
      linhagem_s = (line_text).replace("Kg/m","---").replace("Ka/m?","---").replace("Ko/m?","---").replace("Ka/m2","---").replace("Ko/mº","---").split("---")
      linhagem_s = (remove_chars(linhagem_s[0])).replace("Linhagem", "").replace("Linhagem:", "").replace("Linhagem", "").replace(":", "").replace(".", "").replace(";", "").strip().split(" ")
      linhagem_s = remove_empty_spaces(linhagem_s)
      linhagem_s_f = (" ".join((linhagem_s[:3])))
      # linhagem_s_ = linhagem_s[0:3]
      linhagem_f = {
         "Data": linhagem_s_f,
         "id": id_l
         
      }
      arr_linhagem.append(linhagem_f)
   
   #Qtde Alojada
   if (arr_filter[5] in line_text):
      qtda_ = (line_text.replace("Qtde Alojada", "QtdeAlojada").replace(":", "").split("QtdeAlojada"))
      l_n = len(qtda_)
      if l_n == 2:
         qtda_s = remove_empty_spaces((qtda_[-1].split(" ")))
         qtda_s = (remove_chars(qtda_s[0]))
         
      if l_n == 1:
         # print(qtda_)
         pass
      
      qtda_f  = {
         "Data": qtda_s, 
         "id": id_l
      }
      arr_quant_alojado.append(qtda_f)
      
   # Peso Méd Pintinho: 
   if (arr_filter[6] in line_text):
      pmpe_s = (line_text.split("Aves Faltantes:"))
      ln_s = len(pmpe_s)
      if ln_s == 1:
         pmpe_s = (line_text.replace("Aves Faitantes", "Aves Faltantes").split("Aves Faltantes"))[0]
         pmpe_s = remove_chars(pmpe_s).replace("Peso Méd Pintinho", "").strip()
      if ln_s ==  2:
         pmpe_s = (pmpe_s[0])
         pmpe_s = (remove_chars(pmpe_s)).replace("Peso Méd Pintinho", "").split()
         if len(pmpe_s) == 2:
            pmpe_s = pmpe_s[-1]
         else: 
            pmpe_s = (pmpe_s[0])
      pmpe_f = {
         "Data": pmpe_s,
         "id": id_l,
      }
      
      arr_peso_medio.append(pmpe_f)
   
   # Área Alojada:
   if (arr_filter[7] in line_text):
      arl_s = remove_chars(line_text).replace("Teiefone", "Telefone").split("Telefone")
      l_n = len(arl_s)
      if l_n == 1:
         print("Área Alojada:", arl_s)
      if l_n == 2:
         arl_s = ((arl_s[0]).replace("Área Alojada", "").strip())
         arl_f = arl_s.split(" ")[-1]
      
      arr_area_aloj.append({
         
         "id": id_l,
         "Data": arl_f
      })
   
   #Data Abate
   if (arr_filter[8] in line_text):
      dta_b = (line_text)
      dta_b_s_d = (dta_b.split("Pintos Chegados Mortos")[0])
      if find_dates(dta_b.split("Pintos Chegados Mortos")[0]):
         dta_b_s_f = dta_b_s_d.split("Data Abate")[-1].replace(":", "").replace("Data Abate", "").replace(":", "")
      
         dta_b_f = {
            "id": id_l,
            "Data": dta_b_s_f
         }
         arr_data_abate.append(dta_b_f)
   
   # Categoria
   if (arr_filter[9] in line_text):
      ctg_ = (find_letters(line_text.split("Categoria")[-1]))[0]
      ctg_ =  remove_empty_spaces(ctg_.split(" "))
      ctg_s = (" ".join(ctg_[:2]))
      ctg_f = {
         "Data": ctg_s,
         "id": id_l
      }
      arr_categoria.append(ctg_f)
   
   # Técnico
   if (arr_filter[10][0] in line_text or arr_filter[10][1] in line_text):
      tecnico_s = remove_empty_spaces(find_letters(remove_chars(line_text)))
      l_n = len(tecnico_s)
      if l_n == 1:
         tecnico_s = tecnico_s[0].replace("Técnico", "Tecnico").split("Tecnico")[-1]
         tecnico_f = (tecnico_s).replace("Imposto", "").replace("SENAR", "").replace("tmposto", "").replace("FUNRURAL", "").replace("GRI", "")
      if l_n  == 2:
         if "Tecnico" in tecnico_s[0]:
            tecnico_f = (tecnico_s[0].split("Tecnico")[-1]).replace("Imposto", "").replace("SENAR", "").replace("tmposto", "").replace("FUNRURAL", "").replace("GRI", "")
         else:
            tecnico_f = (tecnico_s[-1]).replace("Imposto", "").replace("SENAR", "").replace("tmposto", "").replace("FUNRURAL", "").replace("GRI", "")
      if l_n  >= 3:
         tecnico_f = (tecnico_s[1]).replace("Credito ou", "").replace("Credito", "").replace("ou", "").replace("Imposto", "").replace("SENAR", "").replace("tmposto", "").replace("FUNRURAL", "").replace("GRI", "")
      
      tecnico_f = find_letters(tecnico_f)[0]
      
      tecnico_arr.append({
         "id": id_l,
         "Data": tecnico_f
      })
   
   # telefone
   if (arr_filter[11] in line_text):
      tel_s_ = remove_chars(line_text).split("Telefone")[-1]
      tel_s_s = find_numbers(tel_s_)   
      if tel_s_s:
         if len((tel_s_s[0])) < 5:
            tel_f = "nan"
         else:
            tel_f = tel_s_s[0]
      
      telefone_arr.append({
         "id": id_l,
         "Data": tel_f
      })
   
   # E-mail
   if (arr_filter[12] in line_text):
      e_mail = (line_text.split("E-mail")[-1]).replace(":", "").replace("Renda", ";").replace("Imposto", ";").replace("imposto", ";").replace("Repasses",  ";").split(";")
      e_mail = e_mail[0].strip()
      e_mail_f = e_mail.replace(" ", "")
      if not e_mail_f or e_mail_f == "":
         e_mail_f = "nan"
         
      email_f = {
         "id": id_l,
         "Data": e_mail_f
      }
      email_arr.append(email_f)
   
   # Tipo Ventilação 
   if (arr_filter[13] in line_text):
      tvl_s = line_text.replace("Tipo Ventilação:", "")
      tvl_s = remove_empty_spaces(remove_chars(tvl_s).split(" "))
      tvl_s = " ".join(tvl_s[:2]).replace("Repasses", "").replace("Renda", "").replace("Imposto", "")
      tvl_s_ = {
         "id": id_l,
         "Data": tvl_s
      }
      t_vent_arr.append(tvl_s_)
      
   #  Kg/m?
   if (arr_filter[14][0] in line_text  or arr_filter[14][1] in line_text  or arr_filter[14][2] in line_text or "Linhagem" in line_text):
      pattern = re.compile(r'^(?:\d{1,3}(?:,\d{3})+|\d+,\d{2})$')
      kg_m = (remove_chars(line_text))
      kg_m = kg_m.replace("Kgm", "").replace("Kom", "").replace("Kam", "")
      
      kg_m = remove_empty_spaces((kg_m).split(" "))
      
      nc = len(kg_m)
      kg_m = (kg_m[4:]) 
      nc_ = len(kg_m)
      if nc_ == 1:
         kg_m_f = kg_m[0]
         
      if nc_ >= 2:
         if pattern.findall(kg_m[0]):
            kg_m_f =  kg_m[0]
         else:
            if pattern.findall(kg_m[1]):
               kg_m_f = (kg_m[1])
            else:
               if pattern.findall(kg_m[2]):
                  kg_m_f = (kg_m[2])
               else:
                  kg_m_f = "nan"
      if pattern.findall(kg_m_f):
         kg_m_f = kg_m_f.strip()
      else:
         kg_m_f = "nan"
      #    if pattern.findall(kg_m):
      #       print(kg_m)
      kgm2_arr.append({
         "id": id_l,
         "Data": kg_m_f
      })
   
   # Material
   if (arr_filter[15] in line_text):
      patern_ = ""
      if (find_letters(line_text.split("Aves/m")[0])):
         material_s = (line_text.split("Aves/m")[0])
         material_s = remove_empty_spaces(material_s.split("Material"))
         nc_ = len(material_s)
         if nc_ == 1:
            material_f = material_s[0].replace(":",  "")
         else:
            material_f = "nan"
            
         material_arr.append({
            "id": id_l,
            "Data": material_f
         })
      #   
   # Aves/m2
   if (arr_filter[16] in line_text or "Aves/m" in line_text):
      aves_ = remove_chars(line_text.split("Aves/m")[-1])
      aves_ = remove_empty_spaces(aves_.split(" "))[0]
      if find_numbers(aves_):
        if "," in aves_:
            aves_f = aves_
      
      ave_m2_arr.append({
         "id": id_l,
         "Data": aves_f
      })
   
   # Qtde Abatida
   if (arr_filter[17] in line_text):
      qtde_a_ = (line_text.split("Qtde Abatida")[-1].replace(":", ""))
      qtde_a = remove_empty_spaces(qtde_a_.split(" "))
      nc_ = len(qtde_a)
      if nc_ == 1:
         qtde_a_f = qtde_a[0]
      if nc_ >= 2:
         if "." in (qtde_a[0]):
            qtde_a_f = qtde_a[0]
         else:
           qtde_a_f = "nan"
      
      qabate_arr.append({
         "id": id_l,
         "Data": qtde_a_f
      })
   
   # Mort. Tota
   if (arr_filter[18] in line_text):
      mort_total_s = (line_text.split("Mort. Total")[-1].replace(":", ""))
      mort_total_s = remove_empty_spaces(mort_total_s.split(" "))
      mort_total_f = mort_total_s[0]
      mort_total_arr.append({
         "id": id_l,
         "Data": mort_total_f
      })

   # Qtde Mortos
   if (arr_filter[19] in line_text):
      qtde_m_s  =  line_text.split("Qtde Mortos")
      qtde_m_s  = remove_empty_spaces(qtde_m_s[-1].replace(":", "").split(" "))
      qtde_m_s_f = qtde_m_s[0]
      quant_mortes_arr.append({
         "id": id_l,
         "Data": qtde_m_s_f
      })
   
   if (arr_filter[20] in line_text):
      print(arr_filter[20])
      pass
key_arr =  list(OrderedDict.fromkeys(key_arr))
print("len key_arr", len(key_arr))


print("dbg_arr:", len(arr_data_aloj))


clifor_arr = processar_dicionarios(key_arr, clifor_arr)
tecnico_arr = processar_dicionarios(key_arr, tecnico_arr)
arr_pedido = processar_dicionarios(key_arr, arr_pedido)
arr_municipio = processar_dicionarios(key_arr, arr_municipio)
arr_data_aloj = processar_dicionarios(key_arr, arr_data_aloj)
arr_linhagem = processar_dicionarios(key_arr, arr_linhagem)
arr_quant_alojado = processar_dicionarios(key_arr, arr_quant_alojado)
arr_peso_medio =  processar_dicionarios(key_arr, arr_peso_medio)
arr_area_aloj = processar_dicionarios(key_arr, arr_area_aloj)
telefone_arr = processar_dicionarios(key_arr, telefone_arr)
arr_data_abate = processar_dicionarios(key_arr, arr_data_abate)
arr_categoria = processar_dicionarios(key_arr, arr_categoria)
email_arr = processar_dicionarios(key_arr, email_arr)
t_vent_arr = processar_dicionarios(key_arr, t_vent_arr)
kgm2_arr = processar_dicionarios(key_arr, kgm2_arr)
material_arr = processar_dicionarios(key_arr, material_arr)
ave_m2_arr = processar_dicionarios(key_arr, ave_m2_arr)
qabate_arr = processar_dicionarios(key_arr, qabate_arr)
mort_total_arr = processar_dicionarios(key_arr, mort_total_arr)
quant_mortes_arr = processar_dicionarios(key_arr, quant_mortes_arr)



new_dataFrame = pd.DataFrame()

new_dataFrame["CHAVE"] = key_arr
new_dataFrame["TECNICO"] = tecnico_arr
new_dataFrame["CLIFOR"] = clifor_arr
new_dataFrame["TELEFONE"] = telefone_arr
new_dataFrame["PEDIDO"] = arr_pedido
new_dataFrame["MUNICIPIO"] = arr_municipio
new_dataFrame["DATA_ALOJAMENTO"] = arr_data_aloj
new_dataFrame["LINHAGEM"] = arr_linhagem
new_dataFrame["QTD_ALOJADA"] = arr_quant_alojado
new_dataFrame["PESO_MED_PINTO"] = arr_peso_medio
new_dataFrame["AREA_ALOJ"] = arr_area_aloj
new_dataFrame["DATA_ABATE"] = arr_data_abate
new_dataFrame["TIPO_PRODUTO"] = arr_categoria
new_dataFrame["EMAIL"] = email_arr
new_dataFrame["T_VENTILACAO"] = t_vent_arr
new_dataFrame["KG_M2"] = kgm2_arr
new_dataFrame["MATERIAL_GENETICO"] = material_arr
new_dataFrame["AVE_M2"] = ave_m2_arr
new_dataFrame["QUANT_ABATE"] = qabate_arr
new_dataFrame["MORTE_TOTAL"] = mort_total_arr
new_dataFrame["QUANTIDADE_MORTOS"] = quant_mortes_arr





  
   #  new_dataFrame["INTEGRADO"] = name_arr

   
   #  new_dataFrame["AVIARIO"] = aviario_arr
   #  new_dataFrame["LINHAGEM"] = arr_linhagem
   #  new_dataFrame["QUANT_ALOJADO"] = arr_quant_alojado
   #  new_dataFrame["DATA_ALOJ"] = arr_data_aloj
   #  new_dataFrame["QUANTIDADE_ELIMINADOS"] = quant_eliminados_arr
   #  new_dataFrame["IDADE_ABATE"] = idade_abate_arr
   #  new_dataFrame["PM_PINTO"] = arr_peso_medio
   #  new_dataFrame["AVES_FALTANTES"] = aves_faltantes_arr
   #  new_dataFrame["PESO_MEDIO"] = peso_medio_f_arr
   #  new_dataFrame["GPD"]=gpd_arr
   #  new_dataFrame["PESO_TOTAL"] = peso_total_arr
   #  new_dataFrame["CAAF"] = caaf_arr
   #  new_dataFrame["RACAO_CONSUMIDA"] = racao_c_arr
   #  new_dataFrame["VALOR_KG_FRANGO"] = valor_kg_f_arr
   #  new_dataFrame["VALOR_KG_RACAO"] = valor_kg_racao_arr
   #  new_dataFrame["VALOR_DO_PINTO"] = valor_pinto_real_arr
   #  new_dataFrame["PERCENTUAL_BASICO"] = percentual_basico_arr
   #  new_dataFrame["KG_CARNE_BASE"] = carne_base_arr
   #  new_dataFrame["R$_BASE"] = real_base_arr
    
    
   #  new_dataFrame['%_AJ_ESCALA_PROD'] = aj_porcent_arr
   #  new_dataFrame['KG_AJ_ESCALA_PROD'] = aj_kg_arr
   #  new_dataFrame["R$_AJ_ESCALA_PROD"] = aj_real_arr
    
   #  new_dataFrame["%_SAZONALIDADE"] = aj_sazonalidade_percent_arr
   #  new_dataFrame["KG_SAZONALIDADE"] = aj_sazonalidade_kg_arr
   #  new_dataFrame["R$_SAZONALIDADE"] = aj_sazonalidade_real_arr
    
   #  new_dataFrame["%_AJ_SEXO_PESO"] = aj_sex_pes_percent_arr
   #  new_dataFrame["KG_AJ_SEXO_PESO"] = aj_sex_pes_kg_arr
   #  new_dataFrame["R$_AJ_SEXO_PESO"] = aj_sex_pes_real_arr
    
   #  new_dataFrame["%_AJ_IDADE"] = aj_idade_percent_arr
   #  new_dataFrame["KG_AJ_IDADE"] = aj_idade_kg_arr 
   #  new_dataFrame["R$_AJ_IDADE"] =  aj_idade_real_arr
    
   #  new_dataFrame["%_AJ_MORTALIDADE"] =  aj_mortalidade_percent_arr
   #  new_dataFrame["KG_AJ_MORTALIDADE"] =  aj_mortalidade_kg_arr
   #  new_dataFrame["R$_AJ_MORTALIDADE"] =  aj_mortalidade_real_arr

   #  new_dataFrame["%_CONV_ALIMENTAR"] =  aj_conv_alimentar_percent_arr
   #  new_dataFrame["KG_CONV_ALIMENTAR"] =  aj_conv_alimentar_kg_arr
   #  new_dataFrame["R$_CONV_ALIMENTAR"] =  aj_conv_alimentar_real_arr
    
   #  # new_dataFrame["LOTE"] = arr_pedido # nao usado

   #  new_dataFrame["%_AJ_MERITOCRACIA_MT"] = aj_meritocracia_mt_percent_arr
   #  new_dataFrame["KG_AJ_MERITOCRACIA_MT"] = aj_meritocracia_mt_kg_arr
   #  new_dataFrame["R$_AJ_MERITOCRACIA_MT"] = aj_meritocracia_mt_real_arr

   #  new_dataFrame["%_AJ_CALO_PATA_A"] = aj_calo_pata_a_percent_arr
   #  new_dataFrame["KG_AJ_CALO_PATA_A"] = aj_calo_pata_a_kg_arr
   #  new_dataFrame["R$_AJ_CALO_PATA_A"] = aj_calo_pata_a_real_arr

   #  new_dataFrame["%_CONDENACOES"] = condenacoes_percent_arr
   #  new_dataFrame["KG_CONDENACOES"] = condenacoes_kg_arr
   #  new_dataFrame["R$_CONDENACOES"] = codenacoes_real_arr

   #  new_dataFrame["%_AJ_QUALIDADE_QT"] = aj_qualidade_percent_arr
   #  new_dataFrame["KG_AJ_QUALIDADE_QT"] = aj_qualidade_kg_arr
   #  new_dataFrame["R$_AJ_QUALIDADE_QT"] = aj_qualidade_real_arr

   #  new_dataFrame["%_AJ_ESTRUTURAL"] = aj_estrutural_percent_arr
   #  new_dataFrame["KG_AJ_ESTRUTURAL"] = aj_estrutural_kg_arr
   #  new_dataFrame["R$_AJ_ESTRUTURAL"] = aj_estrutural_real_arr
    
   #  new_dataFrame["%_AJ_PROCEDIMENTOS"] = aj_procedimentos_percent_arr
   #  new_dataFrame["KG_AJ_PROCEDIMENTOS"] = aj_procedimentos_kg_arr
   #  new_dataFrame["R$_AJ_PROCEDIMENTOS"] = aj_procedimentos_real_arr

   #  new_dataFrame["%_AJ_PROCESSOS_PROCEDIMENTOS_PP"] = aj_processos_procedimentos_pp_percent_arr
   #  new_dataFrame["KG_AJ_PROCESSOS_PROCEDIMENTOS_PP"] = aj_processos_procedimentos_pp_kg_arr
   #  new_dataFrame["R$_AJ_PROCESSOS_PROCEDIMENTOS_PP"] = aj_processos_procedimentos_pp_real_arr

   #  new_dataFrame["%_RESULTADO_LOTE"] = resultado_lote_percent_arr
   #  new_dataFrame["KG_RESULTADO_LOTE"] = resultado_lote_kg_arr
   #  new_dataFrame["R$_RESULTADO_LOTE"] = resultado_lote_real_arr
    
   #  new_dataFrame["R$_AVE"] = ave_real_arr
   #  new_dataFrame["R$_TON"] = ton_real_arr
   #  new_dataFrame["R$_M2"] = m2_real_arr
   #  new_dataFrame["FUNRURAL"] = funrural_arr_f
   #  new_dataFrame["SENAR"] = senar_arr
   #  new_dataFrame["CONTA_CORRENTE"] = conta_corrente_arr
   #  new_dataFrame["CONTA_VINCULADA"] = conta_vinculada;
    
   #  new_dataFrame["CONVERSAO_ALIMENTAR_REAL"] = conv_aliment_real_arr
   #  new_dataFrame["CONVERSAO_ALIMENTAR_AJ"] = conv_aliment_real_aj_arr
   #  new_dataFrame["CONVERSAO_ALIMENTAR_PREV_AJ"] = conv_aliment_prev_aj_arr
   #  new_dataFrame["CONVERSAO_ALIMENTAR_DIFERENCA"] = conv_aliment_diferenca_arr
    
   #  # Idade de Abate REAL | PREV aj | DIFERE|
   #  new_dataFrame["IDADE_DE_ABATE_REAL"] = idade_de_abate_real_arr
   #  new_dataFrame["IDADE_DE_ABATE_PREV_AJ"] = idade_de_abate_real_prev_aj_arr
   #  new_dataFrame["IDADE_DE_ABATE_DIFERENCA"] = idade_de_abate_real_dif_arr

   #  new_dataFrame["PESO_MEDIO_REAL"] = peso_medio_f_arr
   #  new_dataFrame["PESO_MEDIO_PREV_AJ"] = peso_medio_prevaj_arr 
   #  new_dataFrame["PESO_MEDIO_DIFERENCA"] =peso_medio_diferenca_arr

   #  new_dataFrame["MORTALIDADE_REAL"] = mortalidade_real_arr
   #  new_dataFrame["MORTALIDADE_REAL_AJ"] = mortalidade_real_aj_arr
   #  new_dataFrame["MORTALIDADE_PREV_AJ"]  = mortalidade_prev
   #  new_dataFrame["MORTALIDADE_DIFERENCA"] =  mortalidade_diferenca

   #  new_dataFrame["%_CALO_PATA_REAL"]  =  percent_calo_real_arr
   #  new_dataFrame["%_CALO_PATA_PREV"] = percent_calo_prev_arr
   #  new_dataFrame["%_CALO_PATA_REAL_DIFERENCA"]  = percent_calo_dife_arr

   #  new_dataFrame["%_ARRANHADURAS_REAL"]  = percent_arranhaduras_real_arr
   #  new_dataFrame["%_ARRANHADURAS_PREV_AJ"]  = percent_arranhaduras_prevaj_arr
   #  new_dataFrame["%_ARRANHADURAS_DIFERENCA"]  = percent_arranhaduras_diferenca_arr

   #  new_dataFrame["%_PAPO_CHEIO_REAL"]  = percent_papo_cheio_real_arr
   #  new_dataFrame["%_PAPO_CHEIO_PREV"]  = percent_papo_cheio_prev_arr
   #  new_dataFrame["%_PAPO_CHEIO_DIFERENCA"]  = percent_papo_cheio_diferenca_arr
    
   #  new_dataFrame["%_CODENACAO_REAL"] = percent_codenacao_real_arr
   #  new_dataFrame["%_CODENACAO_PREV"] = percent_codenacao_prev_arr
   #  new_dataFrame["%_CODENACAO_DIFERENCA"] = percent_codenacao_diferenca_arr 
   #  new_dataFrame["CENTRO"] = centro_arr



print("Salvando arquivo...")
try:
   new_dataFrame.to_csv(rf"output/aip_colunas_{get_date_now()}.csv",  index=False)
   print("Arquivo salvo com sucesso!")
except Exception as err:
   print("Erro ao salvar arquivo: ", err)
   