import array
from glob import glob
from os import name, path, rename
from time import process_time_ns
import pdfplumber, re, os

condominio = r"Condominio.txt"
path_pdf =  glob(path.join('PDF_Extrair', '*.pdf'))
pdf_name_new = []
texts_arr = []

f_epr_arr = []
key_contract_arr = [] 
names_arr = []

def remove_chars(input_str: str) -> str:
   chars_to_remove = ["[", "\"", "'", "nan", "]", ":", ".pdf", ".", ",", ";", "(", ")", "{", "}", "|", "\\", "/", "?", "!", "@", "#", "$", "%", "^", "&", "*", "<", ">", "~", "`", "+", "=", "_", "-"]
   for char in chars_to_remove:
      input_str = input_str.replace(char, "")
   return input_str

def remove_empty_spaces(lst):
    return list(filter(lambda item: item.strip() != '', lst))

def remove_zeros(numero_str: str) -> str:
    """
    Recebe um número como string e remove os zeros à esquerda,
    retornando apenas a parte significativa.
    
    Exemplo:
        "0000489024" -> "489024"
        "0000600886" -> "600886"
        "0000"       -> "0"
    """
    # substitui zeros no início por nada, mas garante que não fique vazio
    return re.sub(r"^0+", "", numero_str) or "0"

def find_numbers(input_str):
   pattern = r'\b\d+\b'  # Padrão para números
   result = re.findall(pattern, input_str)
   return result

def remover_duplicatas(lista):
    lista_sem_duplicatas = []
    [lista_sem_duplicatas.append(item) for item in lista if item not in lista_sem_duplicatas]
    return lista_sem_duplicatas
    
def file_rename():
   for f_epr in f_epr_arr:
      id_path = (f_epr['id'])
      for key_contract in key_contract_arr:
         key_contract_id = key_contract['id']
         if (id_path == key_contract_id):
            # path_s = id_path.split("""\\""")[0]
            path_s = os.path.dirname(id_path)  # pega só a pasta
                
            new_name = f"{f_epr['data']}-{key_contract['data']}.pdf"
            new_path = os.path.join(path_s, new_name)

            # Verifica antes de renomear
            if os.path.exists(id_path):
               print(f"Renomeando {id_path} -> {new_path}")
               rename(id_path, new_path)
            else:
               # print(f"[ERRO] Arquivo não encontrado: {id_path}")
               pass            
      
def create_name_pdf(pdf_path: str, lines: list) -> str:
   
   for line in lines:
      # print(line.strip())
      if "Integrado" in str(line):
         integrado_s =  remove_empty_spaces(line.replace("Integrado", "").replace(":", "").strip().split(' '))[0]
         intgr_f = remove_zeros(integrado_s)         
         f_epr_arr.append({'id': pdf_path, 'data': f"{intgr_f}"})
         
      if "Pedido" in (line):
        nis_s = str(line)
        pedido_s = nis_s.replace("Pedido", "").replace(":", "").strip()
        pedido_s = find_numbers(pedido_s)
        if pedido_s:
           pedido_f = (pedido_s)[0]
           key_contract_arr.append({'id': pdf_path, 'data': f"{pedido_f}"})
        
   remover_duplicatas(key_contract_arr)
      
      # print(key_contract_arr)
   # return line

def process_pdf_(pdf_file: str):
   # print(f'Processando {pdf_file}')
   with pdfplumber.open(rf'{pdf_file}') as pdf_file_op:
      for page in pdf_file_op.pages:
         text = page.extract_text()
         if text:
            lines = text.split('\n')
            # print(lines)
            create_name_pdf(pdf_file, lines)
         else:
            pass
      # pdf_file_op.close()
         
         
def process_pdf(pdf_arr: list):
   for pdf_file in pdf_arr:
      process_pdf_(pdf_file)
      
         

process_pdf(path_pdf)
file_rename()