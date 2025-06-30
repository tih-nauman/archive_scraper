#!/usr/bin/env python3
"""
Enhanced Wikimedia Content Extractor - Extract rich, structured content with comprehensive metadata
Downloads Wikimedia dumps and extracts detailed content with categories, links, references, and more.
"""

import requests
import os
import sys
import bz2
import gzip
import xml.etree.ElementTree as ET
from urllib.parse import urljoin
from datetime import datetime
import re
import json
from pathlib import Path
import html
from collections import defaultdict, Counter
import hashlib
from concurrent.futures import ProcessPoolExecutor, as_completed

class WikimediaDumpDownloader:
    def __init__(self, base_url="https://dumps.wikimedia.org/"):
        self.base_url = base_url
        self.session = requests.Session()
    
    def get_latest_dump_date(self, project):
        """Get the latest dump date for a given project."""
        try:
            project_url = urljoin(self.base_url, f"{project}/")
            response = self.session.get(project_url)
            response.raise_for_status()
            
            date_pattern = r'(\d{8})/'
            dates = re.findall(date_pattern, response.text)
            
            if not dates:
                print(f"No dump dates found for {project}")
                return None
            
            return max(dates)
            
        except requests.RequestException as e:
            print(f"Error fetching dump dates for {project}: {e}")
            return None
    
    def get_content_dump_files(self, project, date):
        """Get content dump files (articles and pages)."""
        try:
            dump_url = urljoin(self.base_url, f"{project}/{date}/")
            response = self.session.get(dump_url)
            response.raise_for_status()
            
            # Look for content files
            content_patterns = [
                rf'{project}-{date}-pages-articles\.xml\.bz2',      # Main articles
                rf'{project}-{date}-pages-meta-current\.xml\.bz2',  # All current pages
            ]
            
            files = []
            for pattern in content_patterns:
                matches = re.findall(pattern, response.text)
                files.extend(matches)
            
            return files
            
        except requests.RequestException as e:
            print(f"Error fetching dump files for {project}: {e}")
            return []
    
    def download_file(self, project, date, filename, download_dir="dumps"):
        """Download a specific dump file."""
        try:
            project_dir = os.path.join(download_dir, date, project)
            os.makedirs(project_dir, exist_ok=True)
            
            file_url = urljoin(self.base_url, f"{project}/{date}/{filename}")
            local_path = os.path.join(project_dir, filename)
            
            if os.path.exists(local_path):
                print(f"✓ {filename} already exists")
                return local_path
            
            print(f"Downloading {filename}...")
            
            response = self.session.get(file_url, stream=True)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        if total_size > 0:
                            progress = (downloaded / total_size) * 100
                            print(f"\rProgress: {progress:.1f}%", end='', flush=True)
            
            print(f"\n✓ Downloaded {filename}")
            return local_path
            
        except Exception as e:
            print(f"Error downloading {filename}: {e}")
            return None
    
    def download_project_content(self, project, download_dir="dumps"):
        """Download content files for a project."""
        print(f"\n--- Downloading content for {project} ---")
        
        latest_date = self.get_latest_dump_date(project)
        if not latest_date:
            return None
        
        print(f"Latest dump date: {latest_date}")
        
        files = self.get_content_dump_files(project, latest_date)
        if not files:
            print(f"No content files found for {project}")
            return None
        
        downloaded_files = []
        for filename in files:
            file_path = self.download_file(project, latest_date, filename, download_dir)
            if file_path:
                downloaded_files.append(file_path)
        
        return downloaded_files

class EnhancedWikiContentExtractor:
    """Extract rich, structured content from Wikipedia XML dumps with comprehensive metadata."""
    
    def __init__(self):
        self.ns = {'mw': 'http://www.mediawiki.org/xml/export-0.10/'}
        
        # Enhanced patterns for better extraction
        self.infobox_pattern = re.compile(r'\{\{[Ii]nfobox[^}]*\}\}', re.DOTALL)
        self.template_pattern = re.compile(r'\{\{([^}|]+)(?:\|[^}]*)?\}\}', re.DOTALL)
        self.category_pattern = re.compile(r'\[\[Category:([^\]]+)\]\]', re.IGNORECASE)
        self.internal_link_pattern = re.compile(r'\[\[([^|\]]+)(?:\|([^\]]+))?\]\]')
        self.external_link_pattern = re.compile(r'\[([^\s\]]+)\s+([^\]]*)\]')
        self.reference_pattern = re.compile(r'<ref[^>]*>(.*?)</ref>', re.DOTALL | re.IGNORECASE)
        self.header_pattern = re.compile(r'^(=+)\s*([^=]+?)\s*\1\s*$', re.MULTILINE)
        self.image_pattern = re.compile(r'\[\[(?:File|Image):([^\]|]+)(?:\|[^\]]*)?\]\]', re.IGNORECASE)
        self.coord_pattern = re.compile(r'\{\{coord\|([^}]+)\}\}', re.IGNORECASE)
        
    def open_dump_file(self, filepath):
        """Open dump file (handles compression)."""
        if filepath.endswith('.bz2'):
            return bz2.open(filepath, 'rt', encoding='utf-8')
        elif filepath.endswith('.gz'):
            return gzip.open(filepath, 'rt', encoding='utf-8')
        else:
            return open(filepath, 'r', encoding='utf-8')
    
    def extract_structured_data(self, title, raw_content, extra_metadata=None):
        """Extract rich structured data from wiki content, with all possible metadata."""
        data = {
            'title': title,
            'categories': [],
            'infobox': {},
            'templates': [],
            'internal_links': [],
            'external_links': [],
            'references': [],
            'images': [],
            'coordinates': None,
            'sections': [],
            'disambiguation_links': [],
            'language_stats': {},
            'content_hash': '',
            'metadata': {},
            'raw_content': raw_content or ''
        }
        
        if not raw_content:
            if extra_metadata:
                data['metadata'].update(extra_metadata)
            return data
        
        # Extract categories
        categories = self.category_pattern.findall(raw_content)
        data['categories'] = [cat.strip() for cat in categories]
        
        # Extract infobox
        infobox_match = self.infobox_pattern.search(raw_content)
        if infobox_match:
            data['infobox'] = self.parse_infobox(infobox_match.group(0))
        
        # Extract templates
        templates = self.template_pattern.findall(raw_content)
        data['templates'] = list(set([t.strip() for t in templates if t.strip()]))
        
        # Extract internal links
        internal_links = self.internal_link_pattern.findall(raw_content)
        for link in internal_links:
            link_data = {
                'target': link[0].strip(),
                'display_text': link[1].strip() if link[1] else link[0].strip()
            }
            data['internal_links'].append(link_data)
        
        # Extract external links
        external_links = self.external_link_pattern.findall(raw_content)
        for link in external_links:
            link_data = {
                'url': link[0].strip(),
                'text': link[1].strip() if link[1] else link[0].strip()
            }
            data['external_links'].append(link_data)
        
        # Extract references
        references = self.reference_pattern.findall(raw_content)
        data['references'] = [self.clean_reference(ref) for ref in references]
        
        # Extract images
        images = self.image_pattern.findall(raw_content)
        data['images'] = [img.strip() for img in images]
        
        # Extract coordinates
        coord_match = self.coord_pattern.search(raw_content)
        if coord_match:
            data['coordinates'] = coord_match.group(1).strip()
        
        # Extract sections and structure
        data['sections'] = self.extract_sections(raw_content)
        
        # Check for disambiguation
        if 'disambiguation' in raw_content.lower()[:1000]:
            data['disambiguation_links'] = self.extract_disambiguation_links(raw_content)
        
        # Language and content statistics
        clean_text = self.clean_wikitext(raw_content)
        data['language_stats'] = self.analyze_text_stats(clean_text)
        
        # Content hash for deduplication
        data['content_hash'] = hashlib.md5(clean_text.encode('utf-8')).hexdigest()
        
        # Add all possible metadata
        if extra_metadata:
            data['metadata'].update(extra_metadata)
        # Add detailed metadata
        # Add a link to the page if possible (for Wikipedia projects)
        page_url = None
        if title and extra_metadata and 'namespace' in extra_metadata:
            # Only for main/article namespace (0)
            if str(extra_metadata['namespace']) == '0':
                # Try to infer project domain from project_code
                # e.g., hiwikibooks -> hi.wikibooks.org
                project_code = extra_metadata.get('project_code')
                if not project_code and 'page_id' in extra_metadata:
                    project_code = None  # fallback
                if project_code:
                    lang = project_code[:2]
                    if project_code.endswith('wiki'):
                        domain = f"{lang}.wikipedia.org"
                    elif project_code.endswith('wikibooks'):
                        domain = f"{lang}.wikibooks.org"
                    elif project_code.endswith('wiktionary'):
                        domain = f"{lang}.wiktionary.org"
                    elif project_code.endswith('wikinews'):
                        domain = f"{lang}.wikinews.org"
                    elif project_code.endswith('wikiquote'):
                        domain = f"{lang}.wikiquote.org"
                    elif project_code.endswith('wikivoyage'):
                        domain = f"{lang}.wikivoyage.org"
                    elif project_code.endswith('wikiversity'):
                        domain = f"{lang}.wikiversity.org"
                    else:
                        domain = f"{lang}.wikipedia.org"
                    # MediaWiki titles use underscores for spaces
                    title_url = title.replace(' ', '_')
                    page_url = f"https://{domain}/wiki/{title_url}"
        data['metadata']['page_url'] = page_url
        # Add detailed metadata
        data['metadata'].update({
            'is_disambiguation': bool(data['disambiguation_links']),
            'has_infobox': bool(data['infobox']),
            'category_count': len(data['categories']),
            'internal_link_count': len(data['internal_links']),
            'external_link_count': len(data['external_links']),
            'reference_count': len(data['references']),
            'image_count': len(data['images']),
            'section_count': len(data['sections']),
            'template_count': len(data['templates']),
            'raw_content_length': len(raw_content),
            'clean_content_length': len(clean_text),
            'namespace': extra_metadata.get('namespace', None) if extra_metadata else None,
            'page_id': extra_metadata.get('page_id', None) if extra_metadata else None,
            'revision_id': extra_metadata.get('revision_id', None) if extra_metadata else None,
            'parent_id': extra_metadata.get('parent_id', None) if extra_metadata else None,
            'contributor': extra_metadata.get('contributor', None) if extra_metadata else None,
            'timestamp': extra_metadata.get('timestamp', None) if extra_metadata else None,
            'redirect': extra_metadata.get('redirect', None) if extra_metadata else None,
            'first_edit': extra_metadata.get('first_edit', None) if extra_metadata else None,
            'last_edit': extra_metadata.get('last_edit', None) if extra_metadata else None,
            'protection': extra_metadata.get('protection', None) if extra_metadata else None,
            'encoding': extra_metadata.get('encoding', None) if extra_metadata else None,
            'language': extra_metadata.get('language', None) if extra_metadata else None
        })
        
        return data
    
    def parse_infobox(self, infobox_text):
        """Parse infobox into structured data."""
        infobox_data = {}
        try:
            # Remove the outer template brackets
            content = infobox_text[2:-2]  # Remove {{ and }}
            
            # Split by | but handle nested templates
            parts = self.split_template_params(content)
            
            for part in parts[1:]:  # Skip the template name
                if '=' in part:
                    key, value = part.split('=', 1)
                    key = key.strip()
                    value = self.clean_wikitext(value.strip())
                    if key and value:
                        infobox_data[key] = value
        except:
            pass
        
        return infobox_data
    
    def split_template_params(self, text):
        """Split template parameters handling nested brackets."""
        parts = []
        current = ""
        bracket_count = 0
        
        for char in text:
            if char == '{':
                bracket_count += 1
            elif char == '}':
                bracket_count -= 1
            elif char == '|' and bracket_count == 0:
                parts.append(current)
                current = ""
                continue
            
            current += char
        
        if current:
            parts.append(current)
        
        return parts
    
    def extract_sections(self, content):
        """Extract sections with their content."""
        sections = []
        matches = list(self.header_pattern.finditer(content))
        
        for i, match in enumerate(matches):
            level = len(match.group(1))
            title = match.group(2).strip()
            start_pos = match.end()
            
            # Find content until next header of same or higher level
            end_pos = len(content)
            for j in range(i + 1, len(matches)):
                next_match = matches[j]
                next_level = len(next_match.group(1))
                if next_level <= level:
                    end_pos = next_match.start()
                    break
            
            section_content = content[start_pos:end_pos].strip()
            clean_content = self.clean_wikitext(section_content)
            
            if clean_content:
                sections.append({
                    'title': title,
                    'level': level,
                    'content': clean_content,
                    'word_count': len(clean_content.split()),
                    'char_count': len(clean_content)
                })
        
        return sections
    
    def extract_disambiguation_links(self, content):
        """Extract disambiguation page links."""
        links = []
        lines = content.split('\n')
        
        for line in lines:
            if line.strip().startswith('*') and '[[' in line:
                # Extract the link and description
                link_matches = self.internal_link_pattern.findall(line)
                if link_matches:
                    for link in link_matches:
                        description = line.split(']]', 1)[-1].strip()
                        if description.startswith(','):
                            description = description[1:].strip()
                        
                        links.append({
                            'target': link[0].strip(),
                            'display_text': link[1].strip() if link[1] else link[0].strip(),
                            'description': description[:200]  # Limit description
                        })
        
        return links
    
    def analyze_text_stats(self, text):
        """Analyze text for language statistics."""
        if not text:
            return {}
        
        words = text.split()
        sentences = re.split(r'[.!?]+', text)
        paragraphs = [p.strip() for p in text.split('\n\n') if p.strip()]
        
        # Character frequency for language detection hints
        char_freq = Counter(text.lower())
        
        return {
            'word_count': len(words),
            'sentence_count': len([s for s in sentences if s.strip()]),
            'paragraph_count': len(paragraphs),
            'character_count': len(text),
            'unique_words': len(set(word.lower() for word in words if word.isalpha())),
            'avg_words_per_sentence': len(words) / max(len(sentences), 1),
            'avg_sentence_length': len(text) / max(len(sentences), 1),
            'most_common_chars': dict(char_freq.most_common(10))
        }
    
    def clean_reference(self, ref_text):
        """Clean and extract useful information from references."""
        # Remove HTML tags
        clean_ref = re.sub(r'<[^>]+>', '', ref_text)
        
        # Extract URLs
        urls = re.findall(r'https?://[^\s<>"]+', clean_ref)
        
        # Extract titles (often in quotes)
        titles = re.findall(r'"([^"]+)"', clean_ref)
        
        return {
            'raw_text': clean_ref.strip(),
            'urls': urls,
            'titles': titles
        }
    
    def clean_wikitext(self, text):
        """Enhanced cleaning of Wikipedia markup to get readable text."""
        if not text:
            return ""
        
        # Store original for comparison
        original_length = len(text)
        
        # Remove HTML comments
        text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)
        
        # Remove HTML tags but preserve content
        text = re.sub(r'<(?!ref)[^>]+>', '', text)
        
        # Remove references but keep the content structure
        text = re.sub(r'<ref[^>]*>.*?</ref>', '', text, flags=re.DOTALL)
        text = re.sub(r'<ref[^>]*/?>', '', text)
        
        # Clean templates but keep some useful ones
        # Remove most templates but preserve coordinates, dates, etc.
        text = re.sub(r'\{\{(?!coord|birth date|death date)[^}]*\}\}', '', text, flags=re.DOTALL)
        
        # Remove categories and file links
        text = re.sub(r'\[\[Category:[^\]]*\]\]', '', text)
        text = re.sub(r'\[\[File:[^\]]*\]\]', '', text)
        text = re.sub(r'\[\[Image:[^\]]*\]\]', '', text)
        
        # Clean links - keep text, remove markup
        text = re.sub(r'\[\[([^|\]]*)\|([^\]]*)\]\]', r'\2', text)  # [[link|text]] -> text
        text = re.sub(r'\[\[([^\]]*)\]\]', r'\1', text)  # [[link]] -> link
        text = re.sub(r'\[([^\s\]]+)\s+([^\]]*)\]', r'\2', text)  # [url text] -> text
        
        # Clean formatting but preserve structure
        text = re.sub(r"'''([^']*?)'''", r'\1', text)  # Bold
        text = re.sub(r"''([^']*?)''", r'\1', text)   # Italic
        
        # Convert headers to readable format
        text = re.sub(r'==+\s*([^=]+?)\s*==+', r'\n\n\1\n' + '-'*20 + '\n', text)
        
        # Clean up whitespace while preserving paragraph structure
        text = re.sub(r'\n{4,}', '\n\n\n', text)  # Max 3 newlines
        text = re.sub(r'[ \t]+', ' ', text)     # Multiple spaces to single
        text = re.sub(r' *\n *', '\n', text)    # Clean line breaks
        
        # Decode HTML entities
        text = html.unescape(text)
        
        # Remove remaining template artifacts
        text = re.sub(r'\{\{[^}]*\}\}', '', text)
        text = re.sub(r'\[\[[^|\]]*\|', '', text)
        
        return text.strip()
    
    def is_content_page(self, title, text, structured_data=None):
        """Enhanced check if this is a content page we want to extract."""
        if not title or not text:
            return False
        
        # Skip system pages
        skip_prefixes = [
            'Template:', 'Category:', 'File:', 'Image:', 'MediaWiki:', 
            'User:', 'User talk:', 'Talk:', 'Wikipedia:', 'Help:', 'Portal:',
            'Module:', 'TimedText:', 'Special:'
        ]
        
        for prefix in skip_prefixes:
            if title.startswith(prefix):
                return False
        
        # Skip redirects
        if text.strip().upper().startswith('#REDIRECT'):
            return False
        
        # Get content statistics
        if structured_data:
            stats = structured_data.get('language_stats', {})
            word_count = stats.get('word_count', 0)
            
            # More sophisticated content filtering
            if word_count < 50:  # Very short articles
                return False
            
            # Skip if it's mostly a list of links (disambiguation-like)
            if (structured_data.get('metadata', {}).get('internal_link_count', 0) > word_count / 10 
                and word_count < 500):
                return False
        else:
            # Fallback to simple check
            clean_text = self.clean_wikitext(text)
            if len(clean_text) < 200:
                return False
        
        return True
    
    def extract_all_content(self, xml_file_path, output_dir=None, max_pages=None):
        """Extract all readable content with rich metadata from XML dump."""
        # Determine project and language for folder structure
        filename = os.path.basename(xml_file_path)
        # Example: hiwikibooks-20250620-pages-articles.xml.bz2
        parts = filename.split('-')
        if len(parts) > 0:
            project_code = parts[0]  # e.g., hiwikibooks
            language = project_code[:2]
        else:
            project_code = 'unknownwiki'
            language = 'xx'
        # If output_dir is None, use the directory of the xml_file_path
        if output_dir is None:
            output_dir = os.path.dirname(xml_file_path)
        content_dir = os.path.join(output_dir, 'wiki', language, f"{project_code}")
        os.makedirs(content_dir, exist_ok=True)
        # Set project_name for use in output files
        project_name = project_code
        # Output files
        all_content_file = os.path.join(content_dir, "complete_content.txt")
        structured_data_file = os.path.join(content_dir, "structured_data.json")
        rich_index_file = os.path.join(content_dir, "rich_content_index.json")
        analytics_file = os.path.join(content_dir, "content_analytics.json")
        stories_dir = os.path.join(content_dir, "individual_pages")
        metadata_dir = os.path.join(content_dir, "page_metadata")
        
        os.makedirs(stories_dir, exist_ok=True)
        os.makedirs(metadata_dir, exist_ok=True)
        
        print(f"Processing XML file: {xml_file_path}")
        print(f"Enhanced output directory: {content_dir}")
        
        pages_processed = 0
        pages_saved = 0
        all_structured_data = []
        content_analytics = {
            'categories': Counter(),
            'templates': Counter(),
            'common_links': Counter(),
            'word_frequency': Counter(),
            'page_types': Counter()
        }
        
        try:
            with self.open_dump_file(xml_file_path) as f:
                # Parse XML iteratively
                context = ET.iterparse(f, events=('start', 'end'))
                context = iter(context)
                event, root = next(context)
                
                current_page = {}
                in_revision = False
                extra_metadata = {}
                
                with open(all_content_file, 'w', encoding='utf-8') as all_content:
                    all_content.write(f"Enhanced Content Collection from {project_name}\n")
                    all_content.write("=" * 100 + "\n")
                    all_content.write(f"Extraction Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    all_content.write("=" * 100 + "\n\n")
                    
                    for event, elem in context:
                        if event == 'start':
                            if elem.tag.endswith('page'):
                                current_page = {}
                                extra_metadata = {}
                            elif elem.tag.endswith('revision'):
                                in_revision = True
                        
                        elif event == 'end':
                            tag = elem.tag.split('}')[-1]
                            if tag == 'title':
                                current_page['title'] = elem.text or ''
                            elif tag == 'id' and not in_revision:
                                current_page['page_id'] = elem.text or ''
                                extra_metadata['page_id'] = elem.text or ''
                            elif tag == 'ns':
                                extra_metadata['namespace'] = elem.text or ''
                            elif tag == 'redirect':
                                extra_metadata['redirect'] = elem.attrib.get('title', '')
                            elif tag == 'text' and in_revision:
                                current_page['content'] = elem.text or ''
                            elif tag == 'timestamp' and in_revision:
                                current_page['timestamp'] = elem.text or ''
                                extra_metadata['timestamp'] = elem.text or ''
                            elif tag == 'parentid' and in_revision:
                                extra_metadata['parent_id'] = elem.text or ''
                            elif tag == 'id' and in_revision:
                                extra_metadata['revision_id'] = elem.text or ''
                            elif tag == 'contributor' and in_revision:
                                contributor = {}
                                for child in elem:
                                    child_tag = child.tag.split('}')[-1]
                                    contributor[child_tag] = child.text
                                extra_metadata['contributor'] = contributor
                            elif tag == 'revision':
                                in_revision = False
                            elif tag == 'page':
                                pages_processed += 1
                                
                                if pages_processed % 50 == 0:
                                    print(f"Processed {pages_processed} pages, saved {pages_saved} rich content pages...")
                                
                                # Extract rich structured data
                                title = current_page.get('title', '')
                                raw_content = current_page.get('content', '')
                                
                                structured_data = self.extract_structured_data(title, raw_content, extra_metadata)
                                
                                # Check if this is content we want
                                if self.is_content_page(title, raw_content, structured_data):
                                    # Clean the content
                                    clean_content = self.clean_wikitext(raw_content)
                                    
                                    if clean_content:
                                        pages_saved += 1
                                        
                                        # Add additional metadata
                                        structured_data.update({
                                            'page_id': current_page.get('page_id', ''),
                                            'timestamp': current_page.get('timestamp', ''),
                                            'extraction_date': datetime.now().isoformat(),
                                            'clean_content': clean_content,
                                            'raw_content_length': len(raw_content),
                                            'clean_content_length': len(clean_content)
                                        })
                                        
                                        # Save to master content file
                                        all_content.write(f"\n\n{'='*100}\n")
                                        all_content.write(f"TITLE: {title}\n")
                                        all_content.write(f"PAGE ID: {current_page.get('page_id', 'N/A')}\n")
                                        all_content.write(f"PAGE TYPE: {structured_data.get('page_type', 'N/A')}\n")
                                        all_content.write(f"CATEGORIES: {', '.join(structured_data['categories'][:5])}\n")
                                        all_content.write(f"WORD COUNT: {structured_data['language_stats'].get('word_count', 0):,}\n")
                                        all_content.write(f"LAST MODIFIED: {current_page.get('timestamp', 'N/A')}\n")
                                        all_content.write(f"{'='*100}\n\n")
                                        all_content.write(clean_content)
                                        all_content.write("\n\n")
                                        
                                        # Save individual content file
                                        safe_title = re.sub(r'[^\w\s-]', '', title).strip()[:100]
                                        safe_title = re.sub(r'[-\s]+', '_', safe_title)
                                        individual_file = os.path.join(stories_dir, f"{safe_title}.txt")
                                        
                                        with open(individual_file, 'w', encoding='utf-8') as f:
                                            f.write(f"Title: {title}\n")
                                            f.write(f"Type: {structured_data.get('page_type', 'N/A')}\n")
                                            f.write(f"Categories: {', '.join(structured_data['categories'])}\n")
                                            f.write(f"{'='*len(title)}\n\n")
                                            f.write(clean_content)
                                        
                                        # Save individual metadata file
                                        metadata_file = os.path.join(metadata_dir, f"{safe_title}_metadata.json")
                                        metadata_copy = structured_data.copy()
                                        metadata_copy.pop('clean_content', None)  # Don't duplicate content
                                        
                                        with open(metadata_file, 'w', encoding='utf-8') as f:
                                            json.dump(metadata_copy, f, indent=2, ensure_ascii=False)
                                        
                                        # Add to structured data collection
                                        all_structured_data.append(structured_data)
                                        
                                        if max_pages and pages_saved >= max_pages:
                                            break
                            
                            # Clear memory
                            elem.clear()
                            root.clear()
        
        except Exception as e:
            print(f"Error processing XML: {e}")
            return None
        
        # Save complete structured data
        print("Saving structured data and analytics...")
        
        with open(structured_data_file, 'w', encoding='utf-8') as f:
            # Don't include clean_content in structured data file to save space
            export_data = []
            for page_data in all_structured_data:
                export_page = page_data.copy()
                export_page.pop('clean_content', None)
                export_data.append(export_page)
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        
        # Create rich index
        rich_index = {
            'project': project_name,
            'total_pages_processed': pages_processed,
            'content_pages_saved': pages_saved,
            'extraction_date': datetime.now().isoformat(),
            'files': {
                'complete_content': 'complete_content.txt',
                'structured_data': 'structured_data.json',
                'individual_pages_dir': 'individual_pages/',
                'metadata_dir': 'page_metadata/',
                'analytics': 'content_analytics.json'
            },
            'content_summary': {
                'total_categories': len(content_analytics['categories']),
                'total_templates': len(content_analytics['templates']),
                'page_types': dict(content_analytics['page_types']),
                'avg_words_per_page': sum(page['language_stats'].get('word_count', 0) for page in all_structured_data) / max(len(all_structured_data), 1)
            },
            'top_categories': dict(content_analytics['categories'].most_common(20)),
            'top_templates': dict(content_analytics['templates'].most_common(20)),
            'most_linked_pages': dict(content_analytics['common_links'].most_common(20))
        }
        
        with open(rich_index_file, 'w', encoding='utf-8') as f:
            json.dump(rich_index, f, indent=2, ensure_ascii=False)
        
        # Save detailed analytics
        analytics_export = {
            'categories': dict(content_analytics['categories'].most_common(100)),
            'templates': dict(content_analytics['templates'].most_common(100)),
            'common_links': dict(content_analytics['common_links'].most_common(100)),
            'page_types': dict(content_analytics['page_types']),
            'extraction_stats': {
                'total_processed': pages_processed,
                'total_saved': pages_saved,
                'success_rate': (pages_saved / max(pages_processed, 1)) * 100,
                'extraction_date': datetime.now().isoformat()
            }
        }
        
        with open(analytics_file, 'w', encoding='utf-8') as f:
            json.dump(analytics_export, f, indent=2, ensure_ascii=False)
        
        # Create enhanced summary
        summary_file = os.path.join(content_dir, "enhanced_summary.txt")
        with open(summary_file, 'w', encoding='utf-8') as f:
            # Write a human-readable summary of the extraction
            f.write(f"Enhanced Wikimedia Content Extraction Summary\n")
            f.write(f"Project: {project_name}\n")
            f.write(f"Extraction Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Pages Processed: {pages_processed}\n")
            f.write(f"Content Pages Saved: {pages_saved}\n")
            f.write(f"Top Categories: {', '.join(list(content_analytics['categories'].keys())[:10])}\n")
            f.write(f"Top Templates: {', '.join(list(content_analytics['templates'].keys())[:10])}\n")
            f.write(f"Most Linked Pages: {', '.join(list(content_analytics['common_links'].keys())[:10])}\n")
            f.write(f"Page Types: {json.dumps(dict(content_analytics['page_types']))}\n")
            f.write(f"Average Words per Page: {rich_index['content_summary']['avg_words_per_page']:.2f}\n")
            f.write(f"\nSee 'structured_data.json' and 'content_analytics.json' for more details.\n")

    def classify_page_type(self, structured_data):
        """Classify the type of page based on structured data."""
        # Disambiguation
        if structured_data.get('metadata', {}).get('is_disambiguation') or structured_data.get('disambiguation_links'):
            return 'disambiguation'
        # List page
        if any(cat.lower().startswith('lists of') for cat in structured_data.get('categories', [])) or \
           (structured_data.get('sections') and any('list' in s['title'].lower() for s in structured_data['sections'])):
            return 'list'
        # Stub
        if structured_data.get('language_stats', {}).get('word_count', 0) < 200:
            return 'stub'
        # Article (default)
        return 'article'

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Enhanced Wikimedia Content Extractor - Extract rich, structured content with comprehensive metadata')
    parser.add_argument('projects', nargs='*', help='Wikimedia projects (e.g., hiwikibooks enwiki)')
    parser.add_argument('--input-file', default=None, help='Input file with list of projects or dumps to download')
    parser.add_argument('--max-pages', type=int, help='Maximum pages to extract per project None for all extraction')
    parser.add_argument('--download-dir', default='dumps', help='Download directory')
    parser.add_argument('--extract-only', help='Skip download, extract from existing file')
    parser.add_argument('--output-dir', default='dumps', help='Directory to store extracted content (default: alongside dump)')
    args = parser.parse_args()


    if args.input_file:
        # Read projects from input file
        with open(args.input_file, 'r') as f:
            project = [line.strip() for line in f if line.strip()]
        if project:
           args.projects = project
            
    downloader = WikimediaDumpDownloader()
    extractor = EnhancedWikiContentExtractor()

    if args.extract_only:
        # Extract from existing file
        print(f"Extracting content from: {args.extract_only}")
        extractor.extract_all_content(args.extract_only, output_dir=args.output_dir, max_pages=args.max_pages)
        return

    print("=== Enhanced Wikimedia Content Extractor ===")
    print(f"Projects: {', '.join(args.projects)}")
    print("This will download dumps and extract all rich, structured content")

    with ProcessPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(process_project, project, args) for project in args.projects]
        for future in as_completed(futures):
            pass  # All output is printed in the worker processes

def process_project(project, args):
    try:
        print(f"\n🚀 Processing {project}")
        downloader = WikimediaDumpDownloader()
        extractor = EnhancedWikiContentExtractor()
        files = downloader.download_project_content(project, download_dir=args.download_dir)
         
        if files:
            # return
            for xml_file in files:
                print(f"\n📖 Extracting content from {os.path.basename(xml_file)}")
                content_dir = extractor.extract_all_content(xml_file, output_dir=args.output_dir, max_pages=args.max_pages)
                if content_dir:
                    print(f"✅ Content ready in: {content_dir}")
        else:
            print(f"❌ No files downloaded for {project}")
            with open(os.path.join(f"error_in_downloads.txt"), 'a+') as f:
                f.write(f"{project}: Check the project name or network connection.\n")
    except Exception as e:
        print(f"❌ Error processing {project}: {e}")

if __name__ == "__main__":
    if len(sys.argv) == 1:
        # Simple usage
        projects = ["hiwikibooks"]
        downloader = WikimediaDumpDownloader()
        extractor = EnhancedWikiContentExtractor()
        for project in projects:
            files = downloader.download_project_content(project)
            if files:
                for xml_file in files:
                    extractor.extract_all_content(xml_file, max_pages=None)
    else:
        main()
