from random import randint

from google.cloud import vision
from pathlib import Path
import glob
from pdf2image import convert_from_path

client = vision.ImageAnnotatorClient()


def convert_to(image_path):
# Convert PDF to a list of PIL Image objects
  images = convert_from_path(image_path, dpi=600)

  for i, image in enumerate(images):
      # Save each page as a PNG
    image.save(f'page_{i}_{str(randint(10, 100))}_.png', 'PNG')


PASTA = Path("arquivosPDF")

for file in PASTA.glob("*.pdf"):
  convert_to(file)

for file in glob.glob("*.png"):
  # print("processando:", file)
  # convert_to(file) 
    # print("processando:", file.name)
  # ou *.jpg
    with open(file, "rb") as f:
        content = f.read()

    image = vision.Image(content=content)

    response = client.document_text_detection(image=image)

    if response.error.message:
        print("erro:", response.error.message)
        continue

    texto = response.full_text_annotation.text

    print("arquivo:", file)
    print(texto)
    print("-"*50)