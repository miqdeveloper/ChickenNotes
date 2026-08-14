r"""OCR das imagens de ``imgs`` com PP-OCRv6 Medium e CUDA.

Execucao normal (processa apenas imagens ainda sem saida):
    python test2_paddle.py

Teste rapido com uma imagem, sem limitar a carga media:
    python test2_paddle.py --limit 1 --gpu-usage-limit 100

A varredura e incremental: os caminhos nao sao acumulados em uma lista. Assim,
o consumo de memoria permanece praticamente constante mesmo com muitos arquivos.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
import types
from collections.abc import Iterator
from pathlib import Path
from statistics import median
from typing import Any


DETECTION_MODEL = "PP-OCRv6_medium_det"
RECOGNITION_MODEL = "PP-OCRv6_medium_rec"
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".tif", ".tiff"}
GPU_TEMPERATURE_POLL_SECONDS = 5.0


class GpuMonitoringError(RuntimeError):
    """Indica que o controle de temperatura da GPU deixou de funcionar."""


def gpu_temperature_c(gpu_index: int = 0) -> int:
    """Consulta a temperatura atual da GPU NVIDIA em graus Celsius."""

    try:
        result = subprocess.run(
            [
                "nvidia-smi",
                f"--id={gpu_index}",
                "--query-gpu=temperature.gpu",
                "--format=csv,noheader,nounits",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        return int(result.stdout.strip().splitlines()[0])
    except (OSError, subprocess.SubprocessError, ValueError, IndexError) as error:
        raise GpuMonitoringError(
            "nao foi possivel consultar a temperatura com nvidia-smi"
        ) from error


def wait_until_gpu_cools(
    maximum_temperature: int,
    resume_temperature: int,
    gpu_index: int = 0,
) -> float:
    """Pausa ao atingir o limite e retorna o total de segundos aguardados."""

    temperature = gpu_temperature_c(gpu_index)
    if temperature < maximum_temperature:
        return 0.0

    started = time.perf_counter()
    print(
        f"GPU a {temperature} C; pausando ate chegar a "
        f"{resume_temperature} C...",
        flush=True,
    )
    last_report = started
    while temperature > resume_temperature:
        time.sleep(GPU_TEMPERATURE_POLL_SECONDS)
        temperature = gpu_temperature_c(gpu_index)
        now = time.perf_counter()
        if now - last_report >= 30:
            print(f"GPU ainda resfriando: {temperature} C", flush=True)
            last_report = now

    print(f"GPU resfriada para {temperature} C; retomando.", flush=True)
    return time.perf_counter() - started


def duty_cycle_pause(active_seconds: float, usage_limit: int) -> float:
    """Limita a carga media pausando entre imagens completas."""

    if usage_limit >= 100:
        return 0.0
    pause_seconds = active_seconds * (100 - usage_limit) / usage_limit
    time.sleep(pause_seconds)
    return pause_seconds


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OCR PP-OCRv6 em CUDA")
    parser.add_argument("--input-dir", type=Path, default=Path("imgs"))
    parser.add_argument(
        "--output-dir", type=Path, default=Path("saida_text_paddle")
    )
    parser.add_argument(
        "--cache-dir", type=Path, default=Path("Model") / "PaddleOCR"
    )
    parser.add_argument("--det-model", default=DETECTION_MODEL)
    parser.add_argument("--rec-model", default=RECOGNITION_MODEL)
    parser.add_argument("--device", default="gpu:0")
    parser.add_argument(
        "--max-image-dimension",
        type=int,
        default=1920,
        help="limite interno do maior lado usado pelo detector",
    )
    parser.add_argument("--recognition-batch-size", type=int, default=6)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--progress-every", type=int, default=100)
    parser.add_argument(
        "--gpu-usage-limit",
        type=int,
        default=40,
        help="carga media aproximada entre imagens, de 10 a 100 por cento",
    )
    parser.add_argument("--max-gpu-temperature", type=int, default=80)
    parser.add_argument("--resume-gpu-temperature", type=int, default=50)
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if not args.input_dir.is_dir():
        raise FileNotFoundError(f"Pasta de imagens nao encontrada: {args.input_dir}")
    if args.limit is not None and args.limit < 1:
        raise ValueError("--limit deve ser maior que zero")
    if args.max_image_dimension < 64:
        raise ValueError("--max-image-dimension deve ser pelo menos 64")
    if args.recognition_batch_size < 1:
        raise ValueError("--recognition-batch-size deve ser maior que zero")
    if args.progress_every < 1:
        raise ValueError("--progress-every deve ser maior que zero")
    if not 10 <= args.gpu_usage_limit <= 100:
        raise ValueError("--gpu-usage-limit deve ficar entre 10 e 100")
    if not 40 <= args.resume_gpu_temperature < args.max_gpu_temperature <= 85:
        raise ValueError(
            "as temperaturas devem obedecer: 40 <= retomada < maxima <= 85"
        )
    if args.device != "gpu:0":
        raise ValueError("este script foi validado para --device gpu:0")


def iter_images(input_dir: Path) -> Iterator[Path]:
    """Percorre a pasta sem manter todos os caminhos na memoria."""

    with os.scandir(input_dir) as entries:
        for entry in entries:
            try:
                if entry.is_file() and Path(entry.name).suffix.lower() in IMAGE_EXTENSIONS:
                    yield Path(entry.path)
            except OSError as error:
                print(f"ERRO ao consultar {entry.name}: {error}", flush=True)


def output_already_exists(output_path: Path, overwrite: bool) -> bool:
    """Evita reprocessar somente quando o TXT existe e nao esta vazio."""

    if overwrite:
        return False
    try:
        return output_path.is_file() and output_path.stat().st_size > 0
    except OSError:
        return False


def load_ocr(args: argparse.Namespace) -> Any:
    """Carrega o PP-OCRv6 com o runtime Paddle CUDA."""

    args.cache_dir.mkdir(parents=True, exist_ok=True)
    os.environ["PADDLE_PDX_CACHE_HOME"] = str(args.cache_dir)
    os.environ["PADDLE_PDX_MODEL_SOURCE"] = "bos"
    os.environ["PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK"] = "True"

    # PaddleX importa ModelScope mesmo quando o download usa o servidor BOS.
    # ModelScope, por sua vez, importa PyTorch e causa colisao entre as DLLs
    # cuDNN do PyTorch 12.8 e do Paddle 12.6 no mesmo processo Windows.
    # O modulo vazio evita apenas essa importacao desnecessaria neste script.
    sys.modules.setdefault("modelscope", types.ModuleType("modelscope"))

    import paddle
    from paddleocr import PaddleOCR

    if not paddle.device.is_compiled_with_cuda():
        raise RuntimeError("PaddlePaddle foi instalado sem suporte a CUDA")
    paddle.set_device(args.device)

    print(
        f"Carregando {args.det_model} + {args.rec_model} em {args.device}...",
        flush=True,
    )
    return PaddleOCR(
        device=args.device,
        text_detection_model_name=args.det_model,
        text_recognition_model_name=args.rec_model,
        text_recognition_batch_size=args.recognition_batch_size,
        use_doc_orientation_classify=False,
        use_doc_unwarping=False,
        use_textline_orientation=False,
        text_det_limit_side_len=args.max_image_dimension,
        text_det_limit_type="max",
        text_rec_score_thresh=0.0,
    )


def lines_from_result(result: Any) -> list[str]:
    """Agrupa caixas da mesma linha visual e preserva as colunas com tabulacao."""

    texts = list(result.get("rec_texts", []))
    raw_boxes = result.get("rec_boxes", [])
    boxes = raw_boxes.tolist() if hasattr(raw_boxes, "tolist") else list(raw_boxes)
    if len(boxes) != len(texts):
        return [str(value).strip() for value in texts if str(value).strip()]

    items: list[dict[str, float | str]] = []
    for value, box in zip(texts, boxes):
        text = str(value).strip()
        if not text or len(box) < 4:
            continue
        left, top, right, bottom = (float(number) for number in box[:4])
        height = max(1.0, bottom - top)
        items.append(
            {
                "text": text,
                "left": left,
                "center": (top + bottom) / 2.0,
                "height": height,
            }
        )
    if not items:
        return []

    typical_height = median(float(item["height"]) for item in items)
    minimum_tolerance = max(3.0, typical_height * 0.35)
    items.sort(key=lambda item: (float(item["center"]), float(item["left"])))

    rows: list[dict[str, Any]] = []
    for item in items:
        if rows:
            row = rows[-1]
            tolerance = max(
                minimum_tolerance,
                min(float(item["height"]), float(row["height"])) * 0.45,
            )
            if abs(float(item["center"]) - float(row["center"])) <= tolerance:
                row_items = row["items"]
                row_items.append(item)
                count = len(row_items)
                row["center"] = (
                    float(row["center"]) * (count - 1) + float(item["center"])
                ) / count
                row["height"] = median(
                    float(row_item["height"]) for row_item in row_items
                )
                continue
        rows.append(
            {
                "center": float(item["center"]),
                "height": float(item["height"]),
                "items": [item],
            }
        )

    lines: list[str] = []
    for row in rows:
        row["items"].sort(key=lambda item: float(item["left"]))
        lines.append("\t".join(str(item["text"]) for item in row["items"]))
    return lines


def extract_text(image_path: Path, ocr: Any) -> str:
    """Retorna somente o texto reconhecido, na ordem visual do documento."""

    lines: list[str] = []
    for result in ocr.predict(str(image_path), text_rec_score_thresh=0.0):
        lines.extend(lines_from_result(result))
    return "\n".join(lines)


def save_text(output_path: Path, text: str) -> None:
    """Grava atomicamente apenas o texto; pagina vazia gera arquivo vazio."""

    temporary = output_path.with_suffix(output_path.suffix + ".tmp")
    temporary.write_text(text + ("\n" if text else ""), encoding="utf-8")
    temporary.replace(output_path)


def main() -> None:
    args = parse_args()
    validate_args(args)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print(
        "Varredura incremental: os nomes das imagens nao serao acumulados na memoria.",
        flush=True,
    )
    print(
        f"Limite medio da GPU: {args.gpu_usage_limit}% | "
        f"pausa a {args.max_gpu_temperature} C | "
        f"retoma a {args.resume_gpu_temperature} C",
        flush=True,
    )

    ocr: Any | None = None
    scanned = 0
    selected = 0
    completed = 0
    skipped = 0
    failures = 0
    total_inference = 0.0
    total_started = time.perf_counter()

    for image_path in iter_images(args.input_dir):
        if args.limit is not None and selected >= args.limit:
            break
        scanned += 1
        output_path = args.output_dir / f"{image_path.stem}.txt"
        if output_already_exists(output_path, args.overwrite):
            skipped += 1
            continue
        selected += 1

        if ocr is None:
            # wait_until_gpu_cools(
            #     args.max_gpu_temperature, args.resume_gpu_temperature
            # )
            ocr = load_ocr(args)

        if output_already_exists(output_path, args.overwrite):
            skipped += 1
            continue
        

        # wait_until_gpu_cools(args.max_gpu_temperature, args.resume_gpu_temperature)
        started = time.perf_counter()
        try:
            text = extract_text(image_path, ocr)
            inference_seconds = time.perf_counter() - started
            total_inference += inference_seconds
            save_text(output_path, text)
            completed += 1
            if completed == 1 or selected % args.progress_every == 0:
                print(
                    f"[{selected}] {image_path.name} -> {output_path.name} "
                    f"({inference_seconds:.2f}s, {len(text)} caracteres)",
                    flush=True,
                )
        except GpuMonitoringError:
            raise
        except Exception as error:
            failures += 1
            inference_seconds = time.perf_counter() - started
            print(f"[{selected}] ERRO em {image_path.name}: {error}", flush=True)

        duty_cycle_pause(inference_seconds, args.gpu_usage_limit)

    total_seconds = time.perf_counter() - total_started
    print(
        f"Finalizado: {completed} sucesso(s), {failures} erro(s), "
        f"{skipped} pulada(s), {scanned} entrada(s) examinada(s).",
        flush=True,
    )
    print(
        f"Inferencia: {total_inference:.2f}s | tempo total: {total_seconds:.2f}s | "
        f"saida: {args.output_dir}",
        flush=True,
    )
    if ocr is None:
        print("Nada para processar.", flush=True)
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
