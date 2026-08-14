import glob

from pathlib import Path

TEXT_PATH = "saida_text"
import csv
import tempfile
import os
import re




def converter_txt_para_csv(caminhos_txt: list[str]) -> bool:


    """
    converte para o formato csv
    """

    try:
        for caminho in caminhos_txt:
            arquivo_txt = Path(caminho)

            if not arquivo_txt.exists() or not arquivo_txt.is_file():
                return False

            # Exemplo:
            # 30228017637-01-2412_page-0001.txt
            # vira id_file:
            # 30228017637-01-2412
            nome_sem_extensao = arquivo_txt.stem

            id_file = re.sub(
                r"_page[-_]\d+$",
                "",
                nome_sem_extensao,
                flags=re.IGNORECASE
            )

            linhas = arquivo_txt.read_text(
                encoding="utf-8",
                errors="ignore"
            ).splitlines()

            # Mantém o nome original, mas troca a extensão para .csv
            arquivo_csv = arquivo_txt.with_suffix(".csv")

            with arquivo_csv.open(
                mode="w",
                encoding="utf-8",
                newline=""
            ) as f:
                writer = csv.writer(f)

                writer.writerow(["filename", "content"])

                for linha in linhas:
                    if linha.strip():
                        writer.writerow([id_file, linha])

        return True

    except Exception:
        return False

def read_text():
  """
    Retorna a lista com arquivos de texto vazios
    ou lista vazia 

  """
  files_ = glob.glob(f"{TEXT_PATH}/*.txt")
  
  tmp_list = []
  name_id = [name.removeprefix('saida_text\\') for name in files_]
  
  if not files_:
    return False
  else:
    for file in files_:
        file = Path(file)
        if not (file.read_text(encoding="utf-8")):
            tmp_list.append(file)
    return tmp_list


converter_txt_para_csv(glob.glob(f"saida_text/*.txt"))
