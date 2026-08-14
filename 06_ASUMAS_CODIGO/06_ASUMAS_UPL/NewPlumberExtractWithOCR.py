import shutil
from warnings import simplefilter
# import ocr_extract_dates
import os
from pathlib import Path
import pdfplumber
import pandas as pd
from threading import Thread, Lock
from queue import Queue
from tqdm import tqdm

simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

class PDFExtractor:
    def __init__(self, input_dir, output_file, num_threads=4):
        
        
        self.input_dir = input_dir
        self.output_file = output_file
        self.num_threads = num_threads
        self.pdf_queue = Queue()
        self.results = []
        self.lock = Lock()
        self.not_data = []
        
        os.makedirs('arquivosPDF/arquivosPDF_SemTexto', exist_ok=True)
    def call_ocr(self):
        for file in dict.fromkeys(self.not_data):
            source = Path(self.input_dir) / file
            destination = Path('arquivosPDF/arquivosPDF_SemTexto') / file
            if not source.is_file():
                continue
            if destination.exists():
                print(f"OCR pendente ja existe no destino: {destination}")
                continue
            shutil.move(str(source), str(destination))
        
     #   ocr_extract_dates.init_()
    
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
                    if not text:
                        with self.lock:
                            self.not_data.append(filename)
                            # shutil.move(pdf_path, os.path.join('arquivosPDF/arquivosPDF_SemTexto', filename))
                            # os.remove(pdf_path)
                        
                    pbar_pages.update(1)
        except Exception as e:
            print(f"Error processing {filename}: {e}")
            with self.lock:
                self.not_data.append(filename)

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
                    if Path(f).suffix.lower() == '.pdf']
        
            
        total_files = len(pdf_files)
        

        for pdf_info in pdf_files:
            self.pdf_queue.put(pdf_info)
            
        with tqdm(total=total_files, desc="Files processed", position=0) as pbar_files, \
             tqdm(total=0, desc="Pages processed", position=1) as pbar_pages:
                 
            while not self.pdf_queue.empty():
                pdf_info = self.pdf_queue.get()
                self.extract_text_from_pdf(*pdf_info, pbar_pages)
                pbar_files.update(1)
                self.pdf_queue.task_done()
            # threads = []
            # for _ in range(min(self.num_threads, total_files)):
            #     t = Thread(target=self.worker, args=(pbar_files, pbar_pages))
            #     t.start()
            #     threads.append(t)

            # for t in threads:
            #     t.join()

        print("\nSaving results to CSV...")
        df = pd.DataFrame(self.results, columns=['filename', 'content'])
        df.to_csv(self.output_file, index=False, encoding='utf-8')
        print(f"Completed! Extracted {len(self.results)} lines from {total_files} files")

def main():
    extractor = PDFExtractor('pdfs', 'output/output.csv')
    extractor.process_pdfs()
    # aqui deve ser chamado o llm ocr 
    extractor.call_ocr()

if __name__ == "__main__":
    main()
