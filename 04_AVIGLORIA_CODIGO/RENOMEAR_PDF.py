import array
from glob import glob
from os import path, rename
from time import process_time_ns
import pdfplumber

path_pdf =  glob(path.join('PDF_Extrair', '*.pdf'))
pdf_name_new = []
texts_arr = []

f_epr_arr = []
key_contract_arr = [] 


def remove_empty_spaces(lst):
    return list(filter(lambda item: item.strip() != '', lst))

def file_rename():
   for f_epr in f_epr_arr:
      id_path = (f_epr['id'])
      for key_contract in key_contract_arr:
         key_contract_id = key_contract['id']
         if id_path == key_contract_id:
            path_s = id_path.split("""\\""")[0]
            print(f"Renomeando {id_path} para {f_epr['data']}{key_contract['data']}.pdf")
            rename(id_path, fr"{path_s}/{f_epr['data']}{key_contract['data']}.pdf")
   
def create_name_pdf(pdf_path: str, lines: list) -> str:
   # print(lines)
   # lines = text.split('\n')
   # print(len(lines))
   
   for line in lines:
      # print(line.strip())
      if "Empresa" in str(line):
         epr_ = remove_empty_spaces((line).split("Empresa")[-1].split(" "))[0]
         
         f_epr_arr.append({'id': pdf_path, 'data': f"00{epr_}-"})
         
      if "Relatório de Fechamento do Contrato" in str(line) or "Relatorio de Fechamento do Contrato" in str(line) or " Relat rio de Fechamento do Contrato" in str(line) or " Fechamento do Contrato" in str(line) or "Relat(cid:243)rio de Fechamento do Contrato" in str(line):
        
        nis_s = str(line)
        
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