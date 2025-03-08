import os
import time, traceback
import pandas as pd
import pypdf
from tabula import read_pdf_with_template
from tqdm import tqdm
from threading import Semaphore, Thread

semaforo = Semaphore(5)

cols = []

del_words = ["ALOJAMENTO","DESEMPENHO DO LOTE","CÁLCULO DA PARTILHA DO INTEGRADO"]
file_save_name = r"file_csv.csv"
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
    return len(pdf.pages)

def remove_colons(text):
    """Remove colons from a string."""
    return text.replace(':', '')

def process_arrays(arrays, name_file, chaves):
    """Process arrays csv."""
    
    for array in arrays:
        df = pd.DataFrame(array)
        df.insert(0, "CHAVE", name_file)
        column = df.columns[1].replace(":", "")
        
        semaforo.acquire()
        
        if column in chaves:
            cols.append(column)
        
        for _, row in df.iterrows():
            row_values = list(row.values)                
            char_l = str(row_values[1]).replace(":", "")
            
            if char_l in chaves:
                cols.append(char_l)
                
            cols.append(row_values)    
        semaforo.release()

def main():

    start_time = time.time()
    files = ["Notas", "ArquivosCSV", "TemplateTabula"]
    
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
    
    create_dirs(files)
    
    try:
        workers = []
        
        for file_name in tqdm(os.listdir(files[0]), desc="Processando arquivos"):
            if file_name.endswith(".pdf"):
                pdf_num_pages = get_pdf_num_pages(os.path.join(files[0], file_name))
                base_name = os.path.splitext(file_name)[0]
                if pdf_num_pages == 2:
                    template_json = "T2.tabula-template.json"
                if pdf_num_pages == 3:
                    template_json = "T3.tabula-template.json"
                if pdf_num_pages == 4:
                    template_json = "T4.tabula-template.json"                 
                if pdf_num_pages == 5:
                    template_json = "T5.tabula-template.json"
                
                dfs = read_pdf_with_template(os.path.join(files[0], file_name), os.path.join(files[2], template_json), stream=True)
                # process_arrays(dfs, base_name, chaves)
                
                t = Thread(target=process_arrays, args=(dfs, base_name, chaves,))
                t.daemon = True
                workers.append(t)
                t.start()
        
        for worker_ in workers:
            # print(worker_)
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
        df.to_csv(f"{files[1]}/{file_save_name}", mode='a', header=False, index=False, encoding='utf-8', errors='ignore')      
        print("Completo!")
    except Exception as err:
        traceback.print_exc

    end_time = (time.time() - start_time)/60
    print(f"Tempo de exec: {end_time:.2f}")
    input("\nAperte qualquer tecla para sair...")

main()