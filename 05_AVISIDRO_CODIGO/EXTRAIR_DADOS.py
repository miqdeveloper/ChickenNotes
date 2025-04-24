import datetime
import os
from re import template
import time, traceback, shutil
import pandas as pd
import pypdf
from tabula import read_pdf_with_template
from tqdm import tqdm
from threading import Semaphore, Thread
from datetime import datetime

semaforo = Semaphore(5)

def get_date_now():
    date_now = datetime.now()
    d = str(date_now.strftime("""_%d_%m_%Y"""))
    return d

cols = []

del_words = []
file_save_name = fr"dados_extraidos_avisidro.csv"

"""
ALOJAMENTO,
DESEMPENHO DO LOTE,
CÁLCULO DA PARTILHA DO INTEGRADO,
VALORES,
MOVIMENTAÇÃO (PINTOS/MATRIZES),
INFORMAÇÕES DO LOTE,
MOVIMENTAÇÃO DE RAÇÃO,
RETIRADA DO FRANGO PARA ABATE,
HISTÓRICO DOS ÚLTIMOS LOTES,

"""

def create_dirs(dirs):
    """Create directories if they don't exist."""
    for dir_ in dirs:
        if not os.path.exists(dir_):
            os.mkdir(dir_)

def get_pdf_num_pages(file_path):
    """Get the number of pages in a PDF."""
    pdf = pypdf.PdfReader(file_path)
    pdf_l = len(pdf.pages)
    return pdf_l

def remove_colons(text):
    """Remove colons from a string."""
    return text.replace(':', '')

def process_arrays(arrays, name_file, chaves):
    """Process arrays csv."""
    
    for array in arrays:        
        df = pd.DataFrame(array)
        df.insert(0, "CHAVE", name_file)
        column = df.columns[1].replace(":", "")
        # print(column)
        
        cols.append(column)
        if column in chaves:
            cols.append(column)
            
        semaforo.acquire()
        
        for _, row in df.iterrows():
            row_values = list(row.values)                
            char_l = str(row_values[1]).replace(":", "")
            
            if char_l in chaves:
                cols.append(char_l)
                
            cols.append(row_values)    
        semaforo.release()
    return name_file

def main():
    file_use = None

    start_time = time.time()

    files = ["PDF_Extrair", "Arquivos_Extraidos_CSV", "TemplateTabula", "PDF_Arquivo"]
    create_dirs(files)
    files_extrair = os.path.abspath(files[0])
    def CheckAvigloria(diretorio):
        try:
            # Lista todos os arquivos no diretório
            arquivos = os.listdir(diretorio)
            
            # Verifica se algum arquivo tem a extensão ".pdf"
            for arquivo in arquivos:
                if arquivo.lower().endswith(".pdf"):
                    return True  # Encontrou um arquivo PDF
                
            return False  # Não há arquivos PDF no diretório
        
        except FileNotFoundError:
            print(f"O diretório '{diretorio}' não foi encontrado.")
            return False

    # print("Avigloria:", CheckAvigloria(files[3]))   
    
    chaves = [
        "ALOJAMENTO",
        "DESEMPENHO DO LOTE",
        "CÁLCULO DA PARTILHA DO INTEGRADO",
        "VALORES",
        "MOVIMENTAÇÃO (PINTOS/MATRIZES)",
        "INFORMAÇÕES DO LOTE",
        "MOVIMENTAÇÃO DE RAÇÃO",
        "RETIRADA DO FRANGO PARA ABATE",
        "HISTÓRICO DOS ÚLTIMOS LOTES",
        "INFORMAÇÕES GERAIS",
        "DADOS DO LOTE",
        "RESULTADOS DO LOTE",
        "COMPLEMENTO DE RENDA",
        "INDICADORES TÉCNICOS DO LOTE",
        "CONDENAÇÕES DE CAUSAS AGROPECUÁRIA",
        "PESO SEMANAL",
        "ORGEM DO LOTE",
        "ABATES",
        "MOVIMENTAÇÃO DE RAÇÕES",
        "HISTÓRICO DOS PEDIDOS ANTERIORES",
        "OBSERVAÇÔES",
        "FINANCIAMENTO",
    ]
    
    
    file_use = files[0]

    # if CheckAvigloria(files[3]):
    #     file_use = files[3]
    #     template_json = "T1_Avigloria.tabula-template.json"

    # print(file_use)
    try:
        workers = []
        
        for file_name in tqdm(os.listdir(file_use), desc="Processando arquivos"):
            if file_name.endswith(".pdf"):
                pdf_num_pages = get_pdf_num_pages(os.path.join(file_use, file_name))
                # print("AQUI:", pdf_num_pages)
                base_name = os.path.splitext(file_name)[0]

                if file_use == files[0]:
                    if pdf_num_pages == 2:
                        template_json = "T2.tabula-template.json"
                    if pdf_num_pages == 3:
                        template_json = "T3.tabula-template.json"
                    if pdf_num_pages == 4:
                        template_json = "T4.tabula-template.json"                 
                    if pdf_num_pages == 5:
                        template_json = "T5.tabula-template.json"

                # print("tp", template_json)
                dfs = read_pdf_with_template(os.path.join(file_use, file_name), os.path.join(files[2], template_json), stream=True) 

                t = Thread(target=process_arrays, args=(dfs, base_name, chaves,), name=str(file_name))
                t.daemon = True
                workers.append(t)
                t.start()
             
        for worker_ in workers:
            # print(os.path.abspath(files[0]+'/'+worker_.name))
            shutil.move(os.path.abspath(files[0]+'/'+worker_.name), os.path.abspath(files[3]+'/'+worker_.name))
            worker_.join()
            
        # REMOVE Items da lista del_words
        if del_words:
            print("\nLimpando array AGUARDE...")
            
            for x in tqdm(range(len(cols) - 1, -1, -1), desc="Varrendo linhas..."):
                for char in del_words:
                    if char in cols[x]:
                        cols.pop(x)
                    
        print("\nAguarde enquanto o arquivo esta sendo gravado...")
        
        # print(cols)
        df = pd.DataFrame(cols)
        # print(cols)
        # df.to_excel("file_execel.xlsx", index=False)        
        df.to_csv(f"{files[1]}/{file_save_name}", mode='w', header=True, index=False, encoding='utf-8', errors='ignore')      
        print("Completo!")
    except Exception as err:
        print(err)

    end_time = (time.time() - start_time)/60
    print(f"Tempo de exec: {end_time:.2f}")
    # input("\nAperte qualquer tecla para sair...")

main()