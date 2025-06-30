# Wikimedia Dump Downloader & Content Extractor

This module provides tools to download and extract rich, structured content from Wikimedia project dumps (Wikipedia, Wikibooks, Wiktionary, etc.) with comprehensive metadata.

## Directory Structure

- [`wikimedia_downloader.py`](wikimedia/wikimedia_downloader.py): Main script for downloading Wikimedia dumps and extracting structured content.
- [`scrape_dumplist.py`](wikimedia/scrape_dumplist.py): Scrapes the Wikimedia dumps index to list available projects.
- [`indic_dump.py`](wikimedia/indic_dump.py): Identifies Indic language Wikimedia projects for targeted extraction.
- [`dumps.txt`](wikimedia/dumps.txt): List of Wikimedia project codes (one per line).

## Features

- **Download** the latest XML dumps for any Wikimedia project.
- **Extract** articles, categories, infoboxes, links, references, images, and more.
- **Save** clean text, structured JSON, analytics, and per-page metadata.
- **Parallel processing** for efficient extraction.
- **Supports** all Wikimedia projects (Wikipedia, Wikibooks, Wiktionary, etc.).

## Usage

### 1. Download and Extract Content

Download and extract all pages for one or more projects:

```sh
python wikimedia_downloader.py hiwikibooks enwiki
```

### 2. Use an Input File

You can provide a file with a list of project codes (one per line), for example [`dumps.txt`](wikimedia/dumps.txt):

```
hiwikibooks
enwiki
bnwikibooks
```

Run:

```sh
python wikimedia_downloader.py --input-file dumps.txt
```

### 3. Extract Only (Skip Download)

If you already have a dump file and want to extract from it:

```sh
python wikimedia_downloader.py --extract-only path/to/dump.xml.bz2
```

### 4. Limit Pages

To limit the number of pages extracted per project, use `--max-pages`.  
Set `--max-pages None` (the default) to extract **all** pages:

```sh
python wikimedia_downloader.py hiwikibooks --max-pages 1000
python wikimedia_downloader.py enwiki --max-pages None
```

### 5. Output Directory

By default, output is stored in the `dumps` directory.  
You can change this with `--output-dir`:

```sh
python wikimedia_downloader.py hiwikibooks --output-dir my_output
```

## Output

For each project, the extractor creates:

- `wiki/<lang>/<project>/complete_content.txt` — All clean text content
- `wiki/<lang>/<project>/structured_data.json` — Structured metadata for each page
- `wiki/<lang>/<project>/individual_pages/` — One text file per page
- `wiki/<lang>/<project>/page_metadata/` — One JSON metadata file per page
- `wiki/<lang>/<project>/content_analytics.json` — Analytics and statistics
- `wiki/<lang>/<project>/enhanced_summary.txt` — Human-readable summary

## Requirements

- Python 3.7+
- `requests`, `lxml`, `beautifulsoup4`, `tqdm`

Install dependencies:

```sh
pip install requests lxml beautifulsoup4 tqdm
```

## Notes

- Project codes (e.g., `hiwikibooks`, `enwiki`) are listed in [`dumps.txt`](wikimedia/dumps.txt).
- For Indic languages, see [`indic_dump.py`](wikimedia/indic_dump.py).
- The extractor skips non-content/system pages and very short stubs.
- Use `--max-pages None` to extract all pages (default behavior).

## License

MIT License

---

For questions or improvements, please see [`wikimedia_downloader.py`](wikimedia/wikimedia_downloader.py).