from hmac import new
import pandas as pd
from collections import OrderedDict
from datetime import datetime
import time 

file_csv = r"C:\Users\Miqueias\Desktop\Projetos\NotasFrangos\01_AIP_CODIGO\output\saida.csv"

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
    chars_to_remove = ["[", "\"", "'", "nan", "]", ":", ".pdf", "_","-"]
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
      clifor_s= clifor_s[:4]
   
      clifor_s  = remove_chars(str(remove_empty_spaces(clifor_s)))
      clifor_s = clifor_s.split(", ")
      n_s = len(clifor_s)
      
      clifor_f = clifor_s
      
      if n_s == 1:
         clifor_f = clifor_s[0]
         # print(clifor)
         # print("clifor", row['filename'], clifor)
         
      if n_s == 3:
         clifor_f = clifor_s[1]
         # print(clifor_f)
         
      if n_s == 4:
         clifor_f =  clifor_s[1].strip()
      
      clifor_f_ = {"Data": clifor_f, "id": id_l}
      # p;rint(clifor_f)
      # if "Integrado" in clifor_f:
      #    print("clifor", row['filename'], clifor_f)
         
      clifor_arr.append(clifor_f_)
      
      # clifor_arr.append(clifor)
   if (arr_filter[1] in line_text):
      pass
   if (arr_filter[2] in line_text):
      pass
   if (arr_filter[3] in line_text):
      pass
   if (arr_filter[4] in line_text):
      pass
   if (arr_filter[5] in line_text):
      pass
   if (arr_filter[6] in line_text):
      pass
   if (arr_filter[7] in line_text):
      pass
   if (arr_filter[8] in line_text):
      pass
   if (arr_filter[9] in line_text):
      pass
   if (arr_filter[10] in line_text):
      pass
   


key_arr =  list(OrderedDict.fromkeys(key_arr))
print("len key_arr", len(key_arr))

clifor_arr = processar_dicionarios(key_arr, clifor_arr)
new_dataFrame = pd.DataFrame()

new_dataFrame["CHAVE"] = key_arr 
new_dataFrame["CLIFOR"] = clifor_arr


print("Salvando arquivo...")
try:
   new_dataFrame.to_csv(rf"output/aip_colunas_{get_date_now()}.csv",  index=False)
   print("Arquivo salvo com sucesso!")
except Exception as err:
   print("Erro ao salvar arquivo: ", err)
   