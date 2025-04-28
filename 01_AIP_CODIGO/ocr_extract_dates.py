#  tesseract -l por --psm 1 --oem 2 -c preserve_interword_spaces=1 .\624556-5700550547_page-0001.jpg tsv
#  tesseract -l por --psm 1 --oem 2 --dpi 1000 -c preserve_interword_spaces=1 .\624556-5700550547_page-0003.jpg tsv
# tesseract -l por+eng+osd --psm 3 --oem 1 --dpi 1000 -c preserve_interword_spaces=0 .\624556-5700550547_page_1.tif  saida txt
#  tesseract  .\937850-5700543975_pg_2.tif -  --psm 0
# magick mogrify -path processed -format tif -colorspace Gray -resize 200% -contrast-stretch 0% -despeckle -unsharp 1.5x1+0.7+0.02 -threshold 50% -deskew 40% *.tif
# magick mogrify -format tif *.jpg

# converte de PDF para TIF 
# preprocessar a imagem de TIF para TIF melhorando a qualidade das letras 
# passar tessetract para gerar o arquivo de texto com dados tessdata  do vite-ocr

# install winget install procgov


import glob,time
from importlib.metadata import files
import os
import re
from wand.image import Image
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor  # gerenciamento de pool de threads :contentReference[oaicite:1]{index=1}
import subprocess, time
import pandas as pd

t_i = time.time()

def clean_files(folder_path):
    """
    Deletes all *.tf files in the specified folder without comments.
    
    Args:
        folder_path (str): The path to the folder containing the *.tf files.
    """
    for filename in os.listdir(folder_path):
        if filename.endswith(".tif"):
            file_path = os.path.join(folder_path, filename)
            try:
                os.remove(file_path)
                print(f"Deleted file: {file_path}")
            except Exception as e:
                print(f"Error deleting file {file_path}: {e}")

def normalizar_texto(texto: str) -> str:
    """
    Corrige indicadores ordinais (ª→a, º→o) e remove acentos
    de todas as letras do alfabeto, mantendo maiúsculas.
    """
    # Mapa de substituição: ordinais + letras acentuadas
    mapa = {
        # Ordinais
        'ª': 'a', 'º': 'o',
        # A
        'À':'A','Á':'A','Â':'A','Ã':'A','Ä':'A','Å':'A',
        'à':'a','á':'a','â':'a','ã':'a','ä':'a','å':'a',
        # E
        'È':'E','É':'E','Ê':'E','Ë':'E',
        'è':'e','é':'e','ê':'e','ë':'e',
        # I
        'Ì':'I','Í':'I','Î':'I','Ï':'I',
        'ì':'i','í':'i','î':'i','ï':'i',
        # O
        'Ò':'O','Ó':'O','Ô':'O','Õ':'O','Ö':'O',
        'ò':'o','ó':'o','ô':'o','õ':'o','ö':'o',
        # U
        'Ù':'U','Ú':'U','Û':'U','Ü':'U',
        'ù':'u','ú':'u','û':'u','ü':'u',
        # C e N
        'Ç':'C','ç':'c','Ñ':'N','ñ':'n'
    }
    # Compila um único padrão que casa qualquer caractere do mapa
    regex = re.compile("|".join(re.escape(c) for c in mapa.keys()))
    # Substitui usando a função de callback que retorna o valor do mapa
    return regex.sub(lambda m: mapa[m.group()], texto)

if not os.path.exists('arquivosPDF') or not os.path.isdir('images') or not os.path.isdir('process_output') or not os.path.isdir('output'):
    try:
        os.makedirs('output')
        os.makedirs('arquivosPDF')
    except FileExistsError:
        pass

pdf_path = os.path.abspath('arquivosPDF')
images_path = os.path.abspath('images')
process_images = os.path.abspath('process_output')
csv_f = os.path.abspath('output')

def run_remove_blank(tif_path: str) -> None:
    

    
    proc = subprocess.run(
            [
                "tesseract",
                tif_path,
                "stdout",
                "-l", "por",
                "--psm", "3",
                "--oem", "3",
                "--dpi", "300",
                "-c", "preserve_interword_spaces=0"
            ],
            capture_output=True,
            text=True,
            check=True,
            encoding="utf-8"
        )
        # Texto completo extraído, sem espaços nas pontas
    full_text = proc.stdout.strip()
    if not full_text:
        print(f"Arquivo {tif_path} não contém texto legível.")
        os.remove(tif_path)  # Remove arquivo se não houver texto
    
    orientation = subprocess.run([
        "tesseract",
        tif_path,
        "-",
        "--psm", "0"
    ], stdout=subprocess.PIPE)
    
    match = re.search(r'Rotate:\s*(\d+)', orientation.stdout.decode('utf-8'))
    
    if match:
        angle = int(match.group(1))
    else:
        raise ValueError("Não foi possível encontrar o valor de Rotate no OSD")
    # print("Orientação do arquivo: ", orientation.stdout)
    with Image(filename=tif_path) as img:
        img.rotate(angle)
        img.save(filename=tif_path)  # Salva a imagem rotacionada
        
def convert_pdfs_to_tifs(input_dir: str, output_dir: str, pdf_path: str, base_name:str, resolution: int = 350):
    """
    Converte todos os arquivos .pdf em input_dir para arquivos .tif em output_dir.
    Cada página do PDF é salva como um TIFF separado.
    
    :param input_dir: Pasta contendo arquivos PDF de entrada.
    :param output_dir: Pasta onde os TIFFs serão salvos.
    :param resolution: DPI usado para rasterização (padrão: 300).
    """
    # Garante que o diretório de saída exista
    os.makedirs(output_dir, exist_ok=True)

        # Abre o PDF com a resolução especificada
    with Image(filename=pdf_path, resolution=resolution) as pdf:
        # Itera por cada página do PDF
            for i, page in enumerate(pdf.sequence):
                with Image(page) as img:
                    # Remove canal alfa e define fundo branco
                    img.background_color = 'white'
                    img.alpha_channel = 'remove'

                    # Caminho do TIFF de saída para a página atual
                    output_filename = f"{base_name}_pg_{i+1}.tif"
                    output_path = os.path.join(output_dir, output_filename)

                    # Salva a página como TIFF
                    img.save(filename=output_path)
                    print("Pdf convertido com sucesso -->", output_filename)
                    run_remove_blank(output_path)
        #  run_remove_blank(output_path)  # Chama a função para remover o arquivo se não houver texto
         # Chama a função para remover o arquivo se não houver texto


 # Processa os PDFs em paralelo usando threads

def convert_thread(input_dir: str, output_dir: str):
    futures = []
    
    with ThreadPoolExecutor(max_workers=25) as executor:
        
        for filename in os.listdir(input_dir):
            if not filename.lower().endswith('.pdf'):
                continue
            
            pdf_path = os.path.join(input_dir, filename)
            base_name = os.path.splitext(filename)[0]
        
            futures.append(executor.submit(convert_pdfs_to_tifs, input_dir, output_dir, pdf_path, base_name))
        
            # Aguarda a conclusão de todas as threads
        for future in as_completed(futures):
            try:
                future.result()  # Propaga exceções, se houver
            except Exception as e:
                pass

def _process_image(src_path: str, dst_path: str,
                   resize_factor: float,
                   black_point: float,
                   threshold: float,
                   deskew_threshold: float) -> None:
    """ 
    Processa uma única imagem TIFF:
    - remove canal alfa e define fundo branco
    - aplica tons de cinza, redimensiona, estica contraste
    - despeckle, máscara de nitidez, threshold e deskew
    - salva em dst_path como TIFF
    """
    with Image(filename=src_path) as img:
        img.background_color = 'white'
        img.alpha_channel = 'remove'                             # -background white -alpha remove :contentReference[oaicite:3]{index=3}
        img.transform_colorspace('gray')                         # -colorspace Gray :contentReference[oaicite:4]{index=4}
        img.resize(int(img.width * resize_factor), int(img.height * resize_factor))# -resize 200% :contentReference[oaicite:5]{index=5}
        img.contrast_stretch(black_point)
        img.morphology(method='erode', kernel='Octagon:1', iterations=1)
        img.despeckle()                                          # -despeckle :contentReference[oaicite:7]{index=7}
        img.unsharp_mask(1.5, 1.0, 0.7, 0.02)                    # -unsharp-mask 1.5x1+0.7+0.02 :contentReference[oaicite:8]{index=8}
        img.threshold(threshold)                                 # -threshold 50% :contentReference[oaicite:9]{index=9}
        img.deskew(deskew_threshold)                             # -deskew 40% :contentReference[oaicite:10]{index=10}
        img.save(filename=dst_path)                         # grava TIFF processado :contentReference[oaicite:11]{index=11}
    return dst_path

def batch_process_tifs_threaded(input_dir: str,
                                output_dir: str,
                                resize_factor: float = 2.5,
                                black_point: float = 0.0,
                                threshold: float = 0.5,
                                deskew_threshold: float = 40.0) -> None:
    """
    Converte e aprimora todas as imagens .tif de input_dir em paralelo,
    salvando os resultados em output_dir.
    """
    # 1. Cria diretório de saída, se necessário
    os.makedirs(output_dir, exist_ok=True)

    # 2. Lista todos os arquivos .tif
    files = [f for f in os.listdir(input_dir) if f.lower().endswith('.tif')]

    # 3. Processamento em pool de threads
    max_workers = 20  # Número de threads a serem usadas
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for fname in files:
            src = os.path.join(input_dir, fname)
            dst = os.path.join(output_dir, fname)
            futures.append(executor.submit(
                _process_image, src, dst,
                resize_factor, black_point,
                threshold, deskew_threshold
            ))  # agenda cada tarefa em uma thread :contentReference[oaicite:12]{index=12}

        # 4. Aguarda e coleta resultados assim que cada tarefa termina
        for future in as_completed(futures):  # itera no instante de conclusão :contentReference[oaicite:13]{index=13}
            # Propaga exceções, se houver
            print("FUTURE: ", future.result())

def ocr_tifs_to_csv(input_dir: str, output_csv: str) -> None:
    """
    Processa todos os arquivos .tif em 'input_dir' usando Tesseract OCR e
    salva os resultados em 'output_csv' (CSV com colunas 'filename' e 'text').
    
    :param input_dir: pasta contendo arquivos .tif de entrada
    :param output_csv: caminho do CSV de saída (será sobrescrito, se existir)
    """
    records = []
    
    def ocr_call(tif_path: str, fname: str) -> str:
        
        print("Processando Arquivo --> ", fname)
        proc = subprocess.run(
            [
                "tesseract",
                tif_path,
                "stdout",
                "-l", "por_lt",
                "--psm", "3",
                "--oem", "2",
                "--dpi", "1150",
                "-c", "preserve_interword_spaces=0"
            ],
            capture_output=True,
            text=True,
            check=True,
            encoding="utf-8"
        )
        # Texto completo extraído, sem espaços nas pontas
        full_text = proc.stdout.strip()
        # print("Texto extraído: ", full_text)
        if not full_text:
            print(f"Arquivo {fname} não contém texto legível.")
            os.remove(tif_path)  # Remove arquivo se não houver texto
        
        # Divide em linhas e registra cada uma como uma linha no DataFrame
        for line in full_text.splitlines():
            # Debug: imprime cada linha processada
            records.append({
                "filename": fname,
                "text": line
            })
        return fname
    
    # Itera sobre todos os arquivos .tif
    with ThreadPoolExecutor(max_workers=25) as executor:
        futures = []
        for fname in os.listdir(input_dir):                               # :contentReference[oaicite:1]{index=1}
            if not fname.lower().endswith('.tif'):
                continue
            
            tif_path = os.path.join(input_dir, fname)
            
            # Chama o Tesseract via subprocess com captura de stdout
            futures.append(executor.submit(ocr_call, tif_path, fname))          # :contentReference[oaicite:2]{index=2}
        
        # Aguarda a conclusão de todas as threads
    for future in as_completed(futures):
        print("Future OCR:", future.result())  # Propaga exceções, se houver
        # tif_path = os.path.join(input_dir, fname)

    df = pd.DataFrame(records)
    df.to_csv(output_csv, index=False, sep=';', mode='w', encoding='utf-8')
    print(f"\n\nArquivo CSV gerado: {output_csv}")
    
    
def init():
    files_ = glob.glob(pdf_path+'\\'+'*.pdf')
    if not files_:
      print("Nenhum arquivo PDF encontrado na pasta especificada.")
      return
    try:
      print("Convertendo arquivos PDF para TIF...\n")
    #   convert_thread(pdf_path, images_path)
      
      print("melhorando a qualidade das imagens TIF...\n")
    #   batch_process_tifs_threaded(images_path, process_images)
      
      print("Usando OCR...\n")
      ocr_tifs_to_csv(process_images, os.path.join(csv_f, 'saida.csv'))
    #   clean_files(process_images)  # Limpa arquivos TIF processados
    #   clean_files(images_path)  # Limpa arquivos TIF processados
    except Exception as err:
        print(f"Erro ao processar os arquivos: {err}")

init()
t_e = (t_i - time.time())/60

print("Tempo de execucao -> {t_e:.2f}")