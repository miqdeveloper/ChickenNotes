# Copilot Instructions for NotasFrangos Project

## Project Overview
This is a Python-based data extraction pipeline for processing poultry company financial/operational reports (PDFs) into structured CSV tables. Each company (AIP, AVICAR, AVIGRAND, etc.) has a dedicated module with custom extraction logic.

## Architecture
- **Modular Structure**: Numbered folders (01_AIP_CODIGO/) contain company-specific scripts.
- **Data Flow**: PDFs → Tabula extraction with JSON templates → Raw CSV → Pandas processing → Structured CSVs.
- **Key Components**:
  - `EXTRAIR_DADOS.py`: Uses tabula-py to extract tables from PDFs using page-specific templates (e.g., T2.tabula-template.json for 2-page PDFs).
  - `CRIAR_TABELA_*.py`: Cleans and structures extracted data, adding columns like "CHAVE" (filename key).
  - `TemplateTabula/`: Stores JSON templates for tabula extraction.
  - Output folders: `Arquivos_Extraidos_CSV/` (raw extracts), `Colunas_Criadas_CSV/` (processed tables).

## Dependencies & Environment
- **Virtual Env**: Activate with `env\Scripts\activate` (Windows).
- **Requirements**: Install from `requeriments.txt` (note: typo in filename). Key libs: pandas, tabula-py (requires Java/JPype1), pypdf, tqdm.
- **Java Requirement**: tabula-py needs Java runtime; ensure JPype1 is installed.

## Workflows
- **Extraction**: Run `EXTRAIR_DADOS.py` first to generate raw CSV from PDFs in `PDF_Extrair/` or `PDF_Arquivo/`.
- **Processing**: Run `CRIAR_TABELA_*.py` to transform raw data into final tables.
- **Development**: Use VS Code task "Run Python com Watchdog (auto-reload)" for auto-execution on .py changes (watches and runs specified script).
- **GUI**: `GUI/interface.py` provides a CustomTkinter interface for PDF processing.
- **Web Filtering**: `index/index.html` offers a simple web UI for post-processing CSV filtering.

## Patterns & Conventions
- **Naming**: Scripts in Portuguese (e.g., CRIAR_TABELA = create table). Files use date suffixes (e.g., `aip_colunas__02_07_2025.csv`).
- **Data Handling**: Use pandas DataFrames; remove trailing spaces/colons with `remove_last_space()` and `remove_colons()`. Add "CHAVE" column as primary key.
- **Threading**: Employ semaphores (e.g., `Semaphore(5)`) for parallel PDF processing to avoid overload.
- **Error Handling**: Wrap in try-except; use tqdm for progress bars.
- **Folder Structure**: Consistent per module: `PDF_Arquivo/`, `Arquivos_Extraidos_CSV/`, `Colunas_Criadas_CSV/`, `TemplateTabula/`.
- **OCR Integration**: For date extraction, use `ocr_extract_dates.py` in `ExtractDataFromImage/` or `01_AIP_CODIGO/`.

## Examples
- Extract AVICAR data: Run `02_AVICAR_CODIGO/EXTRAIR_DADOS.py` → outputs `dados_extraidos_avicar.csv`.
- Process into table: Run `02_AVICAR_CODIGO/CRIAR_TABELA_AVICAR.py` → structured CSVs in `Colunas_Criadas_CSV/`.
- Custom logic: Each `CRIAR_TABELA_*.py` has company-specific cleaning (e.g., regex for dates, numbers, letters).

## Debugging Tips
- Check intermediate CSVs in `Arquivos_Extraidos_CSV/` for extraction issues.
- Verify templates in `TemplateTabula/` match PDF layouts.
- Run scripts individually; use `print(df.head())` for DataFrame inspection.
- For GUI issues, check `GUI/interface.py` threading and semaphore usage.