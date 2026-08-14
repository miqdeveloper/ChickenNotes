import base64
import mimetypes
import os
import shutil
import subprocess
import sys
import glob
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import cv2
from PIL import Image as PILImage, ImageOps, ImageFilter
from wand.image import Image as WandImage
from wand.color import Color

from langchain_openai import ChatOpenAI
from langchain.agents import create_agent
from validate import *

# =========================
# CAMINHOS
# =========================

PATH_MODEL = r"E:\AGROFOCO\PROGRAMA_PYTHON\LLM_OCR\Model\gemma-4-31B-it-GGUF\gemma-4-31B-it-Q6_K.gguf"
MMO_PATH = r"E:\AGROFOCO\PROGRAMA_PYTHON\LLM_OCR\Model\gemma-4-31B-it-GGUF\mmproj-gemma-4-31B-it-BF16.gguf"
IMG_COMP = r"img_completa"
IMAGE_PATH = r"E:\AGROFOCO\PROGRAMA_PYTHON\LLM_OCR\imgs"
PDF_PATH = r"E:\AGROFOCO\PROGRAMA_PYTHON\LLM_OCR\arquivosPDF\arquivosPDF_SemTexto"

OUT_TEXT_FILE = r"E:\AGROFOCO\PROGRAMA_PYTHON\LLM_OCR\saida_text"

# =========================
# CONFIGURAÇÕES
# =========================

CONVERTER_PDFS = True
COMPRIMIR_IMAGENS = False
# RODAR_OCR = True
RODAR_OCR = True

MAX_WIDTH_LLM = 1900
JPG_QUALITY_LLM = 99


# =========================
# UTILITÁRIOS DE IMAGEM
# =========================


def resize_keep(img, max_width=1900):
    largura, altura = img.size

    if largura <= max_width:
        return img

    proporcao = max_width / largura
    nova_altura = int(altura * proporcao)

    return img.resize((max_width, nova_altura), PILImage.Resampling.LANCZOS)


def comprimir_para_llm_sobrescrever(img_path: str, max_width=1900, quality=95) -> str:
    src_path = Path(img_path)

    if src_path.suffix.lower() not in [".jpg", ".jpeg"]:
        return f"Ignorado: {src_path.name}"

    tmp_path = src_path.with_name(src_path.stem + ".__tmp__.jpg")

    try:
        tamanho_antes = src_path.stat().st_size / 1024

        with PILImage.open(src_path) as img:
            img = ImageOps.exif_transpose(img)
            img = resize_keep(img, max_width=max_width)

            # Para documento/OCR, tons de cinza reduz bastante e mantém leitura
            img = img.convert("L")
            img = ImageOps.autocontrast(img)
            img = img.filter(ImageFilter.SHARPEN)

            img.save(
                tmp_path,
                format="JPEG",
                quality=quality,
                optimize=True,
                progressive=True,
            )

        # Sobrescreve o arquivo original na mesma pasta imgs
        os.replace(tmp_path, src_path)

        tamanho_depois = src_path.stat().st_size / 1024

        return (
            f"{src_path.name} | " f"{tamanho_antes:.1f} KB -> {tamanho_depois:.1f} KB"
        )

    except Exception as e:
        if tmp_path.exists():
            tmp_path.unlink()

        return f"Erro em {src_path.name}: {e}"


def comprimir_pasta_imgs_sobrescrever(pasta_imgs: str, max_workers=5):
    pasta = Path(pasta_imgs)

    if not pasta.exists():
        raise FileNotFoundError(f"Pasta não encontrada: {pasta}")

    jpg_files = list(pasta.glob("*.jpg")) + list(pasta.glob("*.jpeg"))

    print(f"Total de imagens encontradas: {len(jpg_files)}")

    if not jpg_files:
        return

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                comprimir_para_llm_sobrescrever,
                str(img),
                MAX_WIDTH_LLM,
                JPG_QUALITY_LLM,
            )
            for img in jpg_files
        ]

        for future in as_completed(futures):
            print(future.result())


# =========================
# MELHORIA COM OPENCV
# OPCIONAL
# =========================


def melhorar_documento_ripi(input_path, output_path=None, modo="cor"):
    """Melhora apenas imagens que realmente precisam, sem perder detalhes do OCR."""
    import numpy as np

    input_path = Path(input_path)
    output_path = input_path if output_path is None else Path(output_path)

    if output_path.is_dir():
        output_path = output_path / input_path.name

    modo = str(modo).strip().lower()
    if modo not in {"cor", "cinza", "pb"}:
        raise ValueError("modo deve ser 'cor', 'cinza' ou 'pb'")

    img = cv2.imread(str(input_path), cv2.IMREAD_COLOR)
    if img is None:
        raise FileNotFoundError(f"Não consegui abrir: {input_path}")

    # Mede a qualidade numa cópia menor para manter o custo baixo.
    height, width = img.shape[:2]
    scale = min(1.0, 1600.0 / max(height, width))
    sample = img
    if scale < 1.0:
        sample = cv2.resize(
            img,
            (max(1, int(width * scale)), max(1, int(height * scale))),
            interpolation=cv2.INTER_AREA,
        )

    sample_gray = cv2.cvtColor(sample, cv2.COLOR_BGR2GRAY)
    background = float(np.percentile(sample_gray, 95))
    foreground = sample_gray[sample_gray < background - 12]
    contrast = (
        background - float(np.median(foreground)) if foreground.size >= 100 else 255.0
    )
    focus = float(cv2.Laplacian(sample_gray, cv2.CV_64F).var())

    # Limites deliberadamente conservadores: filtros leves também podem apagar
    # traços que o OCR já reconhece em páginas apenas um pouco degradadas.
    low_contrast = contrast < 30.0
    blurred = focus < 60.0
    processed = low_contrast or blurred

    if processed:
        lab = cv2.cvtColor(img, cv2.COLOR_BGR2LAB)
        lightness, channel_a, channel_b = cv2.split(lab)

        if low_contrast:
            lightness = cv2.createCLAHE(
                clipLimit=1.5,
                tileGridSize=(8, 8),
            ).apply(lightness)

        if blurred:
            smooth = cv2.GaussianBlur(lightness, (0, 0), sigmaX=0.9)
            lightness = cv2.addWeighted(lightness, 1.20, smooth, -0.20, 0)

        final = cv2.cvtColor(
            cv2.merge((lightness, channel_a, channel_b)),
            cv2.COLOR_LAB2BGR,
        )
    else:
        final = img

    if modo == "cinza":
        final = cv2.cvtColor(final, cv2.COLOR_BGR2GRAY)
    elif modo == "pb":
        gray = cv2.cvtColor(final, cv2.COLOR_BGR2GRAY)
        _, final = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

    same_path = input_path.resolve() == output_path.resolve()
    same_format = input_path.suffix.lower() == output_path.suffix.lower()
    if same_path and not processed and modo == "cor":
        return str(output_path)

    tmp_path = output_path.with_name(output_path.stem + ".__tmp__" + output_path.suffix)
    try:
        if not processed and modo == "cor" and same_format:
            shutil.copy2(input_path, tmp_path)
        else:
            params = []
            if output_path.suffix.lower() in {".jpg", ".jpeg"}:
                params = [cv2.IMWRITE_JPEG_QUALITY, 99]
            if not cv2.imwrite(str(tmp_path), final, params):
                raise OSError(f"Não consegui salvar: {tmp_path}")

        os.replace(tmp_path, output_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()

    return str(output_path)


# =========================
# CONVERTER PDF PARA JPG
# =========================


def convert_pdfs_to_jpegs_threaded(
    input_dir: str,
    output_dir: str,
    resolution: int = 300,
    jpeg_quality: int = 90,
    max_workers: int = 5,
) -> None:
    os.makedirs(output_dir, exist_ok=True)

    pdf_files = [f for f in os.listdir(input_dir) if f.lower().endswith(".pdf")]

    if not pdf_files:
        print("Nenhum arquivo PDF encontrado.")
        return

    def convert_single_pdf(pdf_filename: str) -> list[str]:
        pdf_path = os.path.join(input_dir, pdf_filename)
        base_name = os.path.splitext(pdf_filename)[0]

        saved_files = []
        staged_files = []

        try:
            with WandImage(filename=pdf_path, resolution=resolution) as pdf:
                for i, page in enumerate(pdf.sequence, start=1):
                    with WandImage(image=page) as img:
                        img.background_color = Color("white")
                        img.alpha_channel = "remove"

                        img.format = "jpeg"
                        img.compression_quality = jpeg_quality

                        output_filename = f"{base_name}_page-{i:04d}.jpg"
                        output_path = os.path.join(output_dir, output_filename)
                        temporary_path = output_path + ".__tmp__.jpg"
                        staged_files.append((temporary_path, output_path))

                        img.save(filename=temporary_path)
                        if os.path.getsize(temporary_path) == 0:
                            raise OSError(f"Imagem temporaria vazia: {temporary_path}")

            if not staged_files:
                raise OSError(f"PDF sem paginas renderizaveis: {pdf_filename}")

            for temporary_path, output_path in staged_files:
                os.replace(temporary_path, output_path)
                saved_files.append(output_path)
                print("PDF convertido com sucesso -->", os.path.basename(output_path))
        finally:
            for temporary_path, _ in staged_files:
                if os.path.exists(temporary_path):
                    os.remove(temporary_path)

        return saved_files

    failed_pdfs = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(convert_single_pdf, pdf_filename): pdf_filename
            for pdf_filename in pdf_files
        }

        for future in as_completed(futures):
            pdf_filename = futures[future]
            try:
                result = future.result()
                print(f"Arquivo PDF finalizado. Páginas geradas: {len(result)}")
            except Exception as e:
                failed_pdfs.append(pdf_filename)
                print(f"Erro ao converter {pdf_filename}: {e}")

    failures_after_retry = []
    for pdf_filename in failed_pdfs:
        print(f"Repetindo conversao sequencial: {pdf_filename}")
        try:
            result = convert_single_pdf(pdf_filename)
            print(f"Arquivo PDF recuperado. Páginas geradas: {len(result)}")
        except Exception as e:
            failures_after_retry.append((pdf_filename, str(e)))
            print(f"Falha definitiva em {pdf_filename}: {e}")

    if failures_after_retry:
        details = "; ".join(
            f"{pdf_filename}: {error}"
            for pdf_filename, error in failures_after_retry
        )
        raise RuntimeError(f"PDFs nao convertidos apos nova tentativa: {details}")


# =========================
# MELHORIA COM WAND
# OPCIONAL
# =========================


def batch_improve_jpegs_overwrite(
    input_dir: str,
    resize_factor: float = 2,
    black_point: float = 3,
    threshold: float = 0.3,
    deskew_threshold: float = 0.6,
    jpeg_quality: int = 99,
    max_workers: int = 10,
) -> None:
    if not os.path.exists(input_dir):
        raise FileNotFoundError(f"Pasta não encontrada: {input_dir}")

    jpg_files = [
        f for f in os.listdir(input_dir) if f.lower().endswith((".jpg", ".jpeg"))
    ]

    if not jpg_files:
        print("Nenhuma imagem JPG/JPEG encontrada.")
        return

    def improve_single_jpeg(filename: str) -> str:
        src_path = os.path.join(input_dir, filename)
        tmp_path = src_path + ".tmp.jpg"

        with WandImage(filename=src_path) as img:
            img.background_color = Color("white")
            img.alpha_channel = "remove"

            img.transform_colorspace("gray")

            new_width = int(img.width * resize_factor)
            new_height = int(img.height * resize_factor)
            img.resize(new_width, new_height)

            img.contrast_stretch(black_point)

            img.morphology(method="erode", kernel="Octagon:1", iterations=1)

            img.despeckle()

            img.unsharp_mask(radius=1.5, sigma=1.0, amount=0.7, threshold=0.02)

            img.threshold(threshold)
            img.deskew(deskew_threshold)

            img.format = "jpeg"
            img.compression_quality = jpeg_quality

            img.save(filename=tmp_path)

        os.replace(tmp_path, src_path)

        return src_path

    futures = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for filename in jpg_files:
            futures.append(executor.submit(improve_single_jpeg, filename))

        for future in as_completed(futures):
            try:
                result = future.result()
                print("Imagem melhorada e sobrescrita -->", result)
            except Exception as e:
                print(f"Erro ao melhorar imagem: {e}")


# =========================
# CONVERTER IMAGEM PARA BASE64
# =========================


def convert_img(img_path: str) -> tuple[str, str]:
    path = Path(img_path)

    if not path.exists():
        raise FileNotFoundError(f"Imagem não encontrada: {path}")

    mime_type, _ = mimetypes.guess_type(path)

    if mime_type is None:
        mime_type = "image/jpeg"

    img_base64 = base64.b64encode(path.read_bytes()).decode("utf-8")

    return img_base64, mime_type


def build_image_message(img_path: str, pergunta: str) -> dict:
    img_base64, mime_type = convert_img(img_path)

    return {
        "role": "user",
        "content": [
            {
                "type": "text",
                "text": pergunta,
            },
            {
                "type": "image_url",
                "image_url": {"url": f"data:{mime_type};base64,{img_base64}"},
            },
        ],
    }


# =========================
# PIPELINE JPEG
# =========================


def init_jpeg():
    print("Iniciando pipeline de imagens...\n")

    if CONVERTER_PDFS:
        print("Convertendo PDFs para JPEG...\n")

        convert_pdfs_to_jpegs_threaded(
            PDF_PATH, IMAGE_PATH, resolution=300, jpeg_quality=99, max_workers=10
        )

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for file in glob.iglob(f"{IMAGE_PATH}/*.jpg"):
            file_path = Path(file)
            name_lower = file_path.name.lower()
            try:
                invalid_file = (
                    ".tmp." in name_lower
                    or ".__tmp__" in name_lower
                    or file_path.stat().st_size == 0
                )
            except OSError as error:
                print(f"Imagem ignorada: {file_path} ({error})")
                continue

            if invalid_file:
                print(f"Imagem temporária ou vazia ignorada: {file_path.name}")
                continue

            futures.append(executor.submit(melhorar_documento_ripi, file, IMAGE_PATH))

            if len(futures) == 10:
                for future in as_completed(futures):
                    try:
                        future.result()
                    except (FileNotFoundError, OSError, cv2.error) as error:
                        print(f"Imagem inválida ignorada: {error}")
                futures.clear()

        for future in as_completed(futures):
            try:
                future.result()
            except (FileNotFoundError, OSError, cv2.error) as error:
                print(f"Imagem inválida ignorada: {error}")

    # batch_improve_jpegs_overwrite(IMAGE_PATH, threshold=0.3, max_workers=10)

    # if COMPRIMIR_IMAGENS:
    #     print("\nComprimindo imagens da pasta imgs e sobrescrevendo...\n")

    #     comprimir_pasta_imgs_sobrescrever(
    #         IMAGE_PATH,
    #         max_workers=10
    #     )

    print("\nPipeline de imagens finalizado.\n")


# =========================
# CHECA SE TEM E OQUE JA FOI FEITO - TEXT
# =========================
def check_text():
    completed_stems = set()
    for file_txt in glob.glob(f"{OUT_TEXT_FILE}/*.txt"):
        text_path = Path(file_txt)
        try:
            if text_path.stat().st_size > 0:
                completed_stems.add(text_path.stem)
        except OSError as error:
            print(f"Texto ignorado: {text_path} ({error})")
    destination_dir = Path(IMG_COMP)
    destination_dir.mkdir(parents=True, exist_ok=True)

    for image_path in Path(IMAGE_PATH).glob("*.jpg"):
        if image_path.stem not in completed_stems:
            continue

        destination = destination_dir / image_path.name
        if destination.exists():
            print(f"Destino ja existe, imagem mantida na origem: {destination}")
            continue

        shutil.move(str(image_path), str(destination))


def run_paddle_ocr():
    script_dir = Path(__file__).resolve().parent
    command = [
        sys.executable,
        str(script_dir / "paddle_ocr_mod.py"),
        "--output-dir",
        OUT_TEXT_FILE,
        *sys.argv[1:],
    ]
    print(f"Iniciando PaddleOCR em processo isolado: {sys.executable}", flush=True)
    subprocess.run(command, check=True, cwd=script_dir)


# =========================
# EXECUÇÃO
# =========================

a = 0
if __name__ == "__main__":
    file_f = None

    files_empty = read_text()
    if files_empty:
        file_f = [
            str(f).replace("saida_text\\", "imgs\\").replace(".txt", ".jpg")
            for f in files_empty
        ]

    if not files_empty:
        files_empty = None

    # check_text()

    init_jpeg()

    if RODAR_OCR:
        run_paddle_ocr()

    converter_txt_para_csv(glob.glob(f"saida_text/*.txt"))
