from warnings import simplefilter

import os
import pdfplumber
import pandas as pd
from threading import Thread, Lock
from queue import Queue
from tqdm import tqdm


def create_dirs(dirs):
    """Create directories if they don't exist."""
    for dir_ in dirs:
        if not os.path.exists(dir_):
            os.mkdir(dir_)



files = ["ArquivosPDF", "Arquivos_Extraidos_CSV", "TemplateTabula", "NotasAvigloria"]
create_dirs(files)


simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

class PDFExtractor:
    def __init__(self, input_dir, output_file, num_threads=4):
        self.input_dir = input_dir
        self.output_file = output_file
        self.num_threads = num_threads
        self.pdf_queue = Queue()
        self.results = []
        self.lock = Lock()
        
    def extract_text_from_pdf(self, pdf_path, filename, pbar_pages):
        try:
            with pdfplumber.open(pdf_path) as pdf:
                total_pages = len(pdf.pages)
                pbar_pages.total = total_pages
                
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        lines = text.split('\n')
                        with self.lock:
                            self.results.extend([[filename, line.strip()] for line in lines if line.strip()])
                    pbar_pages.update(1)
        except Exception as e:
            print(f"Error processing {filename}: {e}")

    def worker(self, pbar_files, pbar_pages):
        while True:
            try:
                pdf_info = self.pdf_queue.get_nowait()
                self.extract_text_from_pdf(*pdf_info, pbar_pages)
                pbar_files.update(1)
                self.pdf_queue.task_done()
            except Queue.Empty:
                break

    def process_pdfs(self):
        pdf_files = [(os.path.join(self.input_dir, f), f) 
                    for f in os.listdir(self.input_dir) 
                    if f.endswith('.pdf')]
        
        total_files = len(pdf_files)
        
        for pdf_info in pdf_files:
            self.pdf_queue.put(pdf_info)

        with tqdm(total=total_files, desc="Files processed", position=0) as pbar_files, \
             tqdm(total=0, desc="Pages processed", position=1) as pbar_pages:
            
            threads = []
            for _ in range(min(self.num_threads, total_files)):
                t = Thread(target=self.worker, args=(pbar_files, pbar_pages))
                t.start()
                threads.append(t)

            for t in threads:
                t.join()

        print("\nSaving results to CSV...")
        df = pd.DataFrame(self.results, columns=['filename', 'content'])
        df.to_csv(self.output_file, index=False, encoding='utf-8')
        print(f"Completed! Extracted {len(self.results)} lines from {total_files} files")

def main():
    extractor = PDFExtractor('NotasAvigloria', 'ArquivosCSV/file_csv.csv')
    extractor.process_pdfs()

if __name__ == "__main__":
    main()