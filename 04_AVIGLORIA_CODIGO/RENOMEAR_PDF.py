import array
from glob import glob
from os import name, path, rename
from time import process_time_ns
import pdfplumber

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

def reader_c(name: str) -> list:
   with open(condominio, 'r', encoding='utf-8') as file:
      r_lines = [ char.strip() for char in file.readlines()]
      if name in r_lines:
         return True
      else:
         return False
   
def remove_empty_spaces(lst):
    return list(filter(lambda item: item.strip() != '', lst))

def file_rename():
   for f_epr in f_epr_arr:
      id_path = (f_epr['id'])
      for key_contract in key_contract_arr:
         key_contract_id = key_contract['id']
         for name in names_arr:
            name_id = name['id']
            if id_path == key_contract_id and id_path == name_id:
               name_f =  name['data']
               if reader_c(name_f):
                  path_s = id_path.split("""\\""")[0]
                  print(f"Renomeando {id_path} para C-{f_epr['data']}{key_contract['data']}.pdf")
                  rename(id_path, fr"{path_s}/C-{f_epr['data']}{key_contract['data']}.pdf")
               else:
                  print(f"O Integrado {name_f} não está na lista.")
                  path_s = id_path.split("""\\""")[0]
                  print(f"Renomeando {id_path} para {f_epr['data']}{key_contract['data']}.pdf")
                  rename(id_path, fr"{path_s}/{f_epr['data']}{key_contract['data']}.pdf")
      
def create_name_pdf(pdf_path: str, lines: list) -> str:
   
   for line in lines:
      # print(line.strip())
      if "Empresa" in str(line):
         epr_ = remove_empty_spaces((line).split("Empresa")[-1].split(" "))[0]
         
         f_epr_arr.append({'id': pdf_path, 'data': f"00{epr_}-"})
         
      if "Relatório de Fechamento do Contrato" in str(line) or "Relatorio de Fechamento do Contrato" in str(line) or " Relat rio de Fechamento do Contrato" in str(line) or " Fechamento do Contrato" in str(line) or "Relat(cid:243)rio de Fechamento do Contrato" in str(line):
        
        nis_s = str(line)
        
        name_integrado = remove_chars(nis_s.split("-")[-1]).strip()
        name_integrado = name_integrado.replace(" ", "")[:11]
        
        names_arr.append({'id': pdf_path, 'data': f"{name_integrado}"})
        
        
        nis_s = nis_s.replace("Relatório de Fechamento do Contrato", "Relatorio de Fechamento do Contrato").replace("Relat(cid:243)rio de Fechamento do Contrato", "Relatorio de Fechamento do Contrato").split("Relatorio de Fechamento do Contrato")
        nome_integrado_final = nis_s[1].replace(":", "").split("-")[1]
        
        chave_integrado_final = nis_s[1].replace(":", "").split("-")[0].replace(" ", "").replace(".", "")

        # CHAVE DO INTEGRADO
        chave_integrado_final = chave_integrado_final.replace(" ", "")
        key_contract_arr.append({'id': pdf_path, 'data': f"{chave_integrado_final}"})
   # return line

def process_pdf_(pdf_file: str):
   print(f'Processando {pdf_file}')
   with pdfplumber.open(rf'{pdf_file}') as pdf_file_op:
      for page in pdf_file_op.pages:
         text = page.extract_text()
         if text:
            lines = text.split('\n')
            create_name_pdf(pdf_file, lines)
         else:
            pass
      # pdf_file_op.close()
         
         
def process_pdf(pdf_arr: list):
   for pdf_file in pdf_arr:
      process_pdf_(pdf_file)
      
         

process_pdf(path_pdf)
file_rename()