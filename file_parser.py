import os
import json
import csv
import openpyxl
import PyPDF2
from typing import Dict, List, Any, Union, BinaryIO
from pathlib import Path
from serializers import serialize_data

# Supported file formats
SUPPORTED_EXTENSIONS = {
    '.xlsx': 'Excel Spreadsheet',
    '.xls': 'Excel Spreadsheet (Legacy)',
    '.csv': 'Comma-Separated Values',
    '.json': 'JavaScript Object Notation',
    '.pdf': 'Portable Document Format',
    '.txt': 'Plain Text'
}

def calculate_file_hash(file_content: bytes) -> str:
    """Calculate SHA-256 hash of file content."""
    import hashlib
    return hashlib.sha256(file_content).hexdigest()

def parse_excel(file_path: str) -> Dict[str, Dict[str, List[Dict]]]:
    """
    Parse Excel file and return tables from all sheets.
    
    Args:
        file_path: Path to the Excel file
        
    Returns:
        Dictionary containing sheet names as keys and their data as values
    """
    import pandas as pd
    
    # Read the Excel file
    xls = pd.ExcelFile(file_path)
    result = {}
    
    # Process each sheet
    for sheet_name in xls.sheet_names:
        try:
            # Read the sheet into a DataFrame
            df = pd.read_excel(xls, sheet_name=sheet_name)
            
            # Convert DataFrame to list of dictionaries
            if not df.empty:
                # Replace NaN with None for JSON serialization
                df = df.where(pd.notnull(df), None)
                # Convert to list of dictionaries
                data = df.to_dict('records')
                result[sheet_name] = {'Table': data}
        except Exception as e:
            print(f"Error processing sheet '{sheet_name}': {str(e)}")
            continue
    
    return result

def parse_csv(file_path: str) -> Dict[str, List[Dict]]:
    """Parse CSV file and return a single table."""
    tables = {}
    table_data = []
    
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert all values to strings and clean them
            cleaned_row = {k: str(v).strip() if v is not None else '' for k, v in row.items()}
            table_data.append(cleaned_row)
    
    if table_data:
        tables['CSV_Data'] = table_data
    
    return {'CSV_Sheet': tables}

def parse_json(file_path: str) -> Dict[str, Dict[str, List[Dict]]]:
    """Parse JSON file and return its content as tables."""
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # If it's a list of objects, treat as a single table
    if isinstance(data, list):
        return {'JSON_Sheet': {'JSON_Data': data}}
    # If it's an object, treat each key as a separate table
    elif isinstance(data, dict):
        return {'JSON_Sheet': {'JSON_Data': [data]}}
    else:
        return {}

def get_file_extension(file_path: str) -> str:
    """Get file extension in lowercase."""
    return Path(file_path).suffix.lower()

def parse_pdf(file_path: str) -> Dict[str, Dict[str, List[Dict]]]:
    """
    Parse PDF file and extract text content.
    
    Args:
        file_path: Path to the PDF file
        
    Returns:
        Dictionary with PDF text content organized by pages
    """
    result = {}
    text_content = []
    
    try:
        with open(file_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            num_pages = len(reader.pages)
            
            for i in range(num_pages):
                page = reader.pages[i]
                page_text = page.extract_text()
                if page_text.strip():
                    text_content.append({
                        'page_number': i + 1,
                        'content': page_text,
                        'char_count': len(page_text),
                        'word_count': len(page_text.split())
                    })
    except Exception as e:
        raise ValueError(f"Error reading PDF file: {str(e)}")
    
    if text_content:
        result['PDF_Content'] = {
            'metadata': {
                'page_count': len(text_content),
                'total_chars': sum(p['char_count'] for p in text_content),
                'total_words': sum(p['word_count'] for p in text_content)
            },
            'pages': text_content
        }
    
    return {'PDF_Document': result}

def parse_text(file_path: str) -> Dict[str, Dict[str, List[Dict]]]:
    """
    Parse plain text file and return its content.
    
    Args:
        file_path: Path to the text file
        
    Returns:
        Dictionary with text content
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
            
        lines = content.splitlines()
        non_empty_lines = [line for line in lines if line.strip()]
        
        return {
            'Text_Content': {
                'metadata': {
                    'line_count': len(lines),
                    'non_empty_line_count': len(non_empty_lines),
                    'char_count': len(content),
                    'word_count': len(content.split())
                },
                'content': content
            }
        }
    except Exception as e:
        raise ValueError(f"Error reading text file: {str(e)}")

def parse_file(file_path: str) -> Dict[str, Dict[str, List[Dict]]]:
    """
    Parse a file based on its extension.
    
    Args:
        file_path: Path to the file to parse
        
    Returns:
        Dictionary containing parsed data in the format:
        {
            'sheet_name': {
                'table_name': [
                    {'column1': 'value1', 'column2': 'value2'},
                    ...
                ]
            }
        }
        
    Raises:
        ValueError: If the file format is not supported
    """
    ext = get_file_extension(file_path)
    
    if ext in ['.xlsx', '.xls']:
        return parse_excel(file_path)
    elif ext == '.csv':
        return parse_csv(file_path)
    elif ext == '.json':
        return parse_json(file_path)
    elif ext == '.pdf':
        return parse_pdf(file_path)
    elif ext == '.txt':
        return parse_text(file_path)
    else:
        supported = ', '.join(SUPPORTED_EXTENSIONS.keys())
        raise ValueError(f"Unsupported file format: {ext}. Supported formats: {supported}")

def is_valid_file_extension(filename: str) -> bool:
    """Check if the file has a supported extension."""
    ext = get_file_extension(filename)
    return ext.lower() in SUPPORTED_EXTENSIONS

def save_uploaded_file(uploaded_file, upload_folder: str) -> tuple:
    """
    Save uploaded file to disk and return its path and hash.
    Preserves the original filename, adds a timestamp, and prevents duplicates.
    """
    import re
    from datetime import datetime
    
    os.makedirs(upload_folder, exist_ok=True)
    
    # Get the original filename and create a safe version
    original_name = uploaded_file.name
    
    # Validate file extension
    if not is_valid_file_extension(original_name):
        supported = ', '.join(SUPPORTED_EXTENSIONS.keys())
        raise ValueError(f"Unsupported file type. Supported formats: {supported}")
    
    safe_name = re.sub(r'[^\w\-. ]', '_', original_name)
    
    # Add timestamp to the filename (before the extension)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name, ext = os.path.splitext(safe_name)
    safe_name = f"{base_name}_{timestamp}{ext}"
    
    # Check if file already exists and create a unique name if needed
    file_path = os.path.join(upload_folder, safe_name)
    counter = 1
    
    while os.path.exists(file_path):
        # If file exists (unlikely with timestamp), append a counter before the extension
        file_path = os.path.join(upload_folder, f"{base_name}_{timestamp}_{counter}{ext}")
        counter += 1
    
    # Save the file
    file_content = uploaded_file.getvalue()
    with open(file_path, "wb") as f:
        f.write(file_content)
    
    # Calculate file hash from the content
    file_hash = calculate_file_hash(file_content)
    
    return file_path, file_hash
