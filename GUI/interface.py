import customtkinter, os
import time, ctypes, traceback
from multiprocessing import Pipe
import pandas as pd
import pypdf
from tabula import read_pdf_with_template
from tqdm import tqdm
from threading import Semaphore, Thread
from threading import  *
from customtkinter import CTkComboBox, CTk, CTkButton, CTkFrame, CTkLabel, CTkTextbox, StringVar, CTkProgressBar
from customtkinter import CTk
# from init import main
semaforo = Semaphore(5)

cols = []

del_words = ["ALOJAMENTO","DESEMPENHO DO LOTE"]
file_save_name = r"file_csv.csv"

test_l = []
global ref_
global f


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

def main(label, bar_p):
    start_time = time.time()
    global f
    f = False 
    files = ["Notas", "ArquivosCSV", "TemplateTabula"]
    label.insert("0.0","Iniciando...\n")
    
    # label.configure(wraplength=1)
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
        bar_p.configure(mode="determinate")
        
        b = bar_p.set(1)
        for file_name in tqdm(os.listdir(files[0]), desc="Processando arquivos"):
            bar_p.start()
            # print(tqdm)
            label.insert("0.0", f"Prcessando arquivos {file_name}\n")
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
                b=+1
                
        
        for worker_ in workers:
            # print(worker_)
            worker_.join()
        
        # REMOVE Items da lista del_words
        if del_words:
            # print("\nLimpando array AGUARDE...")
            label.insert("0.0", f"Limpando array AGUARDE...\n")
            
            for x in tqdm(range(len(cols) - 1, -1, -1), desc="Varrendo linhas..."):
                for char in del_words:
                    if char in cols[x]:
                        cols.pop(x)
        # print("\nAguarde enquanto o arquivo esta sendo gravado...")
        label.insert("0.0", "Aguarde enquanto o arquivo esta sendo gravado...\n")
        # print(cols)
        df = pd.DataFrame(cols)
        # print(cols)
        # df.to_excel("file_execel.xlsx", index=False)        
        df.to_csv(f"{files[1]}/{file_save_name}", mode='a', header=False, index=False, encoding='utf-8', errors='ignore')      
        # print("Completo!")
        label.insert("0.0", f"Completo...\n")
    except Exception as err:
        label.insert("0.0", f"{traceback.print_exc}")
    
    end_time = (time.time() - start_time)/60
    # print(f"Tempo de exec: {end_time:.2f}")
    label.insert("0.0", f"Tempo de exec: {end_time:.2f} \n")
    label.configure(state="disabled")
    bar_p.stop()
    # input("\nAperte qualquer tecla para sair...")
    f=True


def update(label, btn):
    btn.configure(state="disabled")
    time.sleep(30)
    btn.configure(state="normal")
    

def start_(label_t, btn, bar_p):
    
    t = Thread(target=main, args=(label_t, bar_p))
    t1 = Thread(target=update, args=(label_t, btn))
  
    t1.daemon = True
    t1.start()
    t.daemon = True
    t.start()
    
class MyMainFrame2(customtkinter.CTkScrollableFrame):            
    def __init__(self, master, **kwargs):
        super().__init__(master, **kwargs)
        global lab_t
        global bar_p
        
        self.label_t = CTkTextbox(self, font=("hack", 28), width=1000, height=300)        
        self.label_t.grid(row=3, column=0, sticky="WN",   padx=2, pady=1)
        self.label_t.insert("0.0","Aguardando...\n")
        self.label_t.configure(state="disabled")
        
        # self.label_t.delete("0.0", "end")
        # self.label_t.configure(state="disabled")
        # self.label_t.configure(state="normal")
        lab_t  = self.label_t
        
        self.bar_p = CTkProgressBar(self, width=1000, height=20, progress_color="green")
        self.bar_p.grid(row=6, column=0, sticky="Nw",   padx=5, pady=20)
        self.bar_p.set(0)
        bar_p = self.bar_p

             
class App(customtkinter.CTk):
    
    class MyMainFrame(customtkinter.CTkFrame):
        def __init__(self, master, **kwargs):
            
            super().__init__(master, **kwargs)
            
            def combobox_callback(self, option):
                print("combobox dropdown clicked:", option)
        
            # self.master.grid_configure(0, weight=1)
            combobox_var = customtkinter.StringVar(value="option 2")
            self.combobox = CTkComboBox(master=master, values=["option 1", "option 2"], justify="left", command=lambda: combobox_callback(self.combobox.cget("values")), variable=combobox_var)
            self.combobox.set("option 1")
            print("GET COMBOBOX:", self.combobox.get())
            self.combobox.grid(row=0, column=0, padx=30, pady=30, sticky="Nw")
                      
            # self.combobox.pack(padx=0, pady=10)
            
    class MyMainFrame3(customtkinter.CTkFrame):    
        def __init__(self, master, **kwargs):
            super().__init__(master, **kwargs)
            
            self.btnStart = CTkButton(master=self, height=50, width=160, text="Start", font=("hack", 26), command=lambda: start_(lab_t, self.btnStart, bar_p))
            self.btnStart.grid(row=6, column=0, sticky="SN",   padx=20, pady=50)

   
    def get_screen_resolution(self):
        
        
        user32 = ctypes.windll.user32
        width = user32.GetSystemMetrics(0)
        height = user32.GetSystemMetrics(1)       

        # Obtém a resolução da tela


        # Divide a resolução por 1.5
        new_width = int(width / 1.3)
        new_height = int(height / 1.3)
      
        return new_width, new_height
    def __init__(self):
        super().__init__()
        
        self.wid, self.heig = self.get_screen_resolution()       
        self._set_appearance_mode("dark")
        self.geometry(f"{self.wid}x{self.heig}")        
        self.grid_columnconfigure(0, weight=1)
        # self.grid_rowconfigure(0)
        
        
        self.frame1 = self.MyMainFrame(master=self, border_width=1, border_color="green", width=6000, height=230, fg_color="transparent")
        self.frame1.grid(row=0, column=0, pady=5, sticky="nw")
    
        self.frame2 = MyMainFrame2(self, border_width=1, border_color="black", width=2500, height=350)        
        self.frame2.grid(row=2, column=0)

        self.frame3 = self.MyMainFrame3(self, border_width=0, width=6000, height=300, fg_color="transparent")        
        self.frame3.grid(row=3, column=0, pady=30, sticky="Nw")
 
app = App()
app.mainloop()