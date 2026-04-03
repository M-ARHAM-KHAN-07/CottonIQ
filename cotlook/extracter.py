import re
import pandas as pd
import pdfplumber
import logging
import numpy as np
from datetime import datetime, date
import os

logger = logging.getLogger(__name__)

def extract_data_from_pdf(pdf_path):
    """Extract data from the downloaded PDF using advanced extraction logic"""
    try:
        if not pdf_path or not os.path.exists(pdf_path):
            raise Exception("No PDF file to process")
        
        logger.info(f"Extracting data from: {os.path.basename(pdf_path)}")
        
        # Use the advanced extraction method
        compositions_df, other_df = extract_cotton_data(pdf_path)
        
        logger.info(f"Extracted {len(compositions_df)} composition records and {len(other_df)} other records")
        
        return compositions_df, other_df
        
    except Exception as e:
        logger.error(f"Error extracting data from PDF: {e}")
        raise

def clean_numeric_value(value):
    """
    Clean numeric values by removing space separators and converting to proper numeric type
    Examples: '1 432' -> 1432.0, '2 560.5' -> 2560.5
    """
    if pd.isna(value) or value in ['NQ', 'NO', 'Unch', '']:
        return np.nan
    
    # Convert to string for processing
    str_val = str(value).strip()
    
    # Remove space separators in numbers
    cleaned = re.sub(r'(\d)\s+(\d)', r'\1\2', str_val)
    
    try:
        # Try to convert to numeric - always return float for consistency
        return float(cleaned)
    except (ValueError, TypeError):
        return np.nan

def should_exclude_a_index_row(row):
    """
    Check if a row should be excluded based on A Index criteria
    Returns True if any value in the row starts with "A Index"
    """
    for value in row.values:
        if isinstance(value, str) and value.startswith('A Index'):
            return True
    return False

def finalize_compositions_dataframe(df):
    """
    Apply final cleaning and formatting to the COMPOSITIONS dataframe ONLY
    This function excludes A Index rows from compositions data
    """
    if df.empty:
        return df
    
    # Remove rows where any value starts with "A Index" - ONLY for compositions
    df = df[~df.apply(should_exclude_a_index_row, axis=1)]
    
    # Clean and convert numeric columns
    numeric_columns = ['Spot_price', 'Spot_change', 'forward_price', 'forward_change']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].apply(clean_numeric_value)
    
    # Ensure shipment columns match [0-9]{1,2}/[0-9]{1,2} format only
    shipment_columns = ['Spot_shpt', 'forward_shpt']
    for col in shipment_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)
            # Replace any value not matching \d{1,2}/\d{1,2} with empty string
            df[col] = df[col].apply(lambda x: x if re.match(r'^\d{1,2}/\d{1,2}$', x) else '')
            df.loc[df[col] == 'nan', col] = ''
    
    return df.reset_index(drop=True)

def finalize_other_dataframe(df):
    """
    Apply final cleaning and formatting to the OTHER dataframe ONLY
    This function does NOT exclude A Index rows - they are valid for other data
    """
    if df.empty:
        return df
    
    # Clean and convert numeric columns for other data
    if 'Value' in df.columns:
        df['Value'] = df['Value'].apply(clean_numeric_value)
    if 'Change' in df.columns:
        df['Change'] = df['Change'].apply(clean_numeric_value)
    
    return df.reset_index(drop=True)

def ensure_compositions_data_types(df):
    """Ensure proper data types for compositions DataFrame"""
    if df.empty:
        return df
    
    # Convert Date column to datetime
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Convert string columns
    df['Marketing_Year'] = df['Marketing_Year'].astype('string')
    df['Growth'] = df['Growth'].astype('string')
    df['Spot_shpt'] = df['Spot_shpt'].astype('string')
    df['forward_shpt'] = df['forward_shpt'].astype('string')
    
    # Convert numeric columns, handling special values
    df['Spot_price'] = pd.to_numeric(df['Spot_price'], errors='coerce')
    df['Spot_change'] = pd.to_numeric(df['Spot_change'], errors='coerce')
    df['forward_price'] = pd.to_numeric(df['forward_price'], errors='coerce')
    df['forward_change'] = pd.to_numeric(df['forward_change'], errors='coerce')
    
    return df

def ensure_other_data_types(df):
    """Ensure proper data types for other DataFrame"""
    if df.empty:
        return df
    
    # Convert Date column to datetime
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Convert string columns
    df['Marketing_Year'] = df['Marketing_Year'].astype('string')
    df['Index_Name'] = df['Index_Name'].astype('string')
    df['Unit'] = df['Unit'].astype('string')
    
    # Convert numeric columns, handling special values
    df['Value'] = pd.to_numeric(df['Value'], errors='coerce')
    df['Change'] = pd.to_numeric(df['Change'], errors='coerce')
    
    return df

def extract_cotton_data(pdf_path):
    """Extract cotton data from a single PDF file using advanced logic"""
    with pdfplumber.open(pdf_path) as pdf:
        full_text = ""
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                full_text += page_text + "\n"
    
    # Check if PDF has cotton data before processing
    if not has_cotton_data(full_text):
        return pd.DataFrame(), pd.DataFrame()
    
    # Extract document date
    doc_date = parse_document_date_to_datetime(full_text)
    
    # Determine PDF structure type
    pdf_type = determine_pdf_type(full_text)
    logger.info(f"PDF type: {pdf_type}")
    
    if pdf_type == "cif_europe":
        compositions_df = create_compositions_csv_cif_europe(full_text, doc_date)
    elif pdf_type == "dual_index_system":
        compositions_df = create_compositions_csv_dual_index_system(full_text, doc_date)
    elif pdf_type == "dual_year":
        compositions_df = create_compositions_csv_dual_year(full_text, doc_date)
    elif pdf_type == "multi_index_format":
        compositions_df = create_compositions_csv_multi_index_format(full_text, doc_date)
    else:
        compositions_df = create_compositions_csv_single_year(full_text, doc_date)
    
    other_df = create_other_csv(full_text)
    
    compositions_df = finalize_compositions_dataframe(compositions_df)
    other_df = finalize_other_dataframe(other_df)
    
    compositions_df = ensure_compositions_data_types(compositions_df)
    other_df = ensure_other_data_types(other_df)
    
    return compositions_df, other_df

def standardize_year(year):
    """Convert short year format (e.g., 2006/07) to full year format (e.g., 2006/2007)"""
    if not year or not isinstance(year, str):
        return year
    
    # Remove asterisk for processing
    asterisk = '*' if year.endswith('*') else ''
    year_clean = year.rstrip('*')
    
    # Match year patterns like 2006/07 or 2006/2007
    match = re.match(r'(\d{4})/(\d{2,4})', year_clean)
    if match:
        start_year = match.group(1)
        end_year = match.group(2)
        # If end year is two digits, prepend the first two digits of start year
        if len(end_year) == 2:
            end_year = start_year[:2] + end_year
        return f"{start_year}/{end_year}"
    return year

def has_cotton_data(text):
    """Enhanced check if the PDF contains relevant cotton data"""
    cotton_indicators = [
        'A Index', 'B Index', 'Composition', 'Price', 'Change', 'Shpt',
        'CIF CAD N. European ports', 'N. European values', 'Far Eastern values',
        'Egyptian', 'Giza', 'American Pima', 'Sudan Barakat', 'Cen Asian',
        'DUAL INDEX SYSTEM', 'NON-INDEX PRICES',
        'Greek Middling', 'Uzbekistan', 'Syrian', 'African', 'Brazilian',
        'Memphis', 'Orleans', 'Turkish', 'Indian', 'Chinese', 'Pakistan',
        'Australian', 'Paraguayan', 'Tanzanian', 'Mexican', 'California',
        'Burkina Faso', 'Cameroon', 'Chad', 'Israeli', 'Mali', 'Spanish',
        'Zambian', 'Zimbabwe', 'Ivory Coast', 'Benin', 'Argentine',
        'Ugandan'
    ]
    return any(indicator in text for indicator in cotton_indicators)

def parse_document_date_to_datetime(text):
    """Extract and parse date from document to proper datetime format"""
    date_patterns = [
        r'Liverpool,\s+([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})',  # Liverpool, Month DD, YYYY
        r'Liverpool,\s+(\d{1,2})\s+([A-Za-z]+),?\s+(\d{4})',  # Liverpool, DD Month, YYYY
        r'Liverpool,\s+(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})',   # Liverpool, DD Month YYYY
        r'Liverpool,\s+([A-Za-z]+)\s+(\d{4})',                 # Liverpool, Month YYYY
        r'([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})',               # Month DD, YYYY
        r'(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})',                # DD Month YYYY
    ]
    
    month_map = {
        'january': 1, 'february': 2, 'march': 3, 'april': 4,
        'may': 5, 'june': 6, 'july': 7, 'august': 8,
        'september': 9, 'october': 10, 'november': 11, 'december': 12,
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
        'jun': 6, 'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
    }
    
    for pattern in date_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            groups = match.groups()
            if len(groups) == 3:
                if groups[0].isalpha():
                    month_name = groups[0].lower()
                    day = int(groups[1])
                    year = int(groups[2])
                else:
                    day = int(groups[0])
                    month_name = groups[1].lower()
                    year = int(groups[2])
                
                month_num = month_map.get(month_name)
                if month_num:
                    try:
                        parsed_date = date(year, month_num, day)
                        return parsed_date
                    except ValueError:
                        continue
                        
            elif len(groups) == 2:
                month_name = groups[0].lower()
                year = int(groups[1])
                month_num = month_map.get(month_name)
                if month_num:
                    try:
                        parsed_date = date(year, month_num, 1)
                        return parsed_date
                    except ValueError:
                        continue
    
    return date.today()

def determine_pdf_type(text):
    """Determine the PDF structure type"""
    if 'CIF CAD N. European ports' in text or 'N. European values' in text or 'A (NE) Index' in text:
        return "cif_europe"
    
    if ('DUAL INDEX SYSTEM' in text) or \
       (re.search(r'20\d{2}/\d{2,4}\*?\s+A\s+\([^)]*\)\s+[\d.]+\s+[-+]?[\d.]+', text) and 
        re.search(r'20\d{2}/\d{2,4}\*?\s+B\s+[\d.]+\s+[-+]?[\d.]+', text)):
        return "dual_index_system"
    
    if ('A Index' in text and 'A (NE) Index' in text) or \
       ('Far Eastern values' in text and 'N. European values' in text):
        return "multi_index_format"
    
    if re.search(r'20\d{2}/\d{2,4}\*?', text) and len(extract_years_from_text(text)) >= 2:
        return "dual_year"
    
    return "single_year"

def is_valid_composition(composition):
    """Enhanced check if a composition name is valid"""
    if not composition or len(composition.strip()) < 2:
        return False
    
    composition_clean = composition.strip()
    
    invalid_keywords = [
        'Liverpool', 'Subscription', 'enquiries', 'fax', 'tel', 'email', 'www',
        'DISCLAIMER', 'Disclaimer', 'Cotlook Limited', 'transmission', 'NOTES:',
        'Price', 'Change', 'Shpt', 'Composition', 'DUAL INDEX SYSTEM',
        'NON-INDEX PRICES', 'LONG STAPLE VARIETIES', 'subscription and transmission',
        'World Wide Web', 'http://', 'Quotations in US cents'
    ]
    
    if any(keyword.lower() in composition_clean.lower() for keyword in invalid_keywords):
        return False
    
    valid_prefixes = [
        'African', 'American', 'Argentine', 'Australian', 'Benin', 'Brazilian',
        'Burkina Faso', 'California', 'Calif', 'Cameroon', 'Cen Asian', 'Central Asian',
        'Chad', 'Chinese', 'Egyptian', 'Giza', 'Greek', 'Indian', 'Israeli',
        'Ivory Coast', 'Mali', 'Memphis', 'Mexican', 'Pakistan', 'Paraguayan',
        'Spanish', 'Sudan', 'Syrian', 'Tanzanian', 'Texas', 'Turkish', 'US Pima',
        'Ugandan', 'Uzbekistan', 'Zambian', 'Zimbabwe', 'A Index', 'B Index',
        'Orleans', 'Peruvian', 'Iranian', 'Xinjiang'
    ]
    
    return any(composition_clean.startswith(prefix) for prefix in valid_prefixes)

def is_long_staple_variety(composition):
    """Check if composition is a long staple variety that should be excluded"""
    long_staple_indicators = [
        'Giza', 'American Pima', 'Peruvian Pima', 'Sudan Barakat', 'Cen Asian'
    ]
    return any(indicator in composition for indicator in long_staple_indicators)

def extract_years_from_text(text):
    """Extract year patterns from text, including asterisk"""
    year_patterns = [
        r'(20\d{2}/\d{2,4}\*?)',
    ]
    
    years = []
    for pattern in year_patterns:
        matches = re.findall(pattern, text)
        years.extend([standardize_year(year) for year in matches])
    
    return list(set(years))

def extract_single_year_from_text(text):
    """Extract single year from text, including asterisk"""
    year_match = re.search(r'(20\d{2}/\d{2,4}\*?)', text)
    if year_match:
        return year_match.group(1)
    return "Unknown"

def extract_first_year_from_text(text):
    """Extract the first year (with or without asterisk) from the document"""
    year_patterns = [
        r'(20\d{2}/\d{2,4}\*?)',
    ]
    
    years = []
    for pattern in year_patterns:
        matches = re.findall(pattern, text)
        years.extend([standardize_year(year) for year in matches])
    
    if years:
        return years[0]
    
    year_match = re.search(r'(20\d{2}/\d{2,4}\*?)', text)
    if year_match:
        return standardize_year(year_match.group(1))
    
    return "Unknown"

def create_compositions_csv_cif_europe(text, doc_date):
    """Enhanced CIF Europe CSV creation with better data extraction"""
    data = []
    lines = text.split('\n')
    
    current_year = None
    in_composition_section = False
    in_long_staple_section = False
    current_section = None
    
    year_candidates = extract_years_from_text(text)
    if year_candidates:
        current_year = year_candidates[0]
    
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue
            
        if any(skip_word in line for skip_word in ['DISCLAIMER', 'Disclaimer', 'Subscription', 
                                                   'Quotations in US cents', 'subscription and transmission',
                                                   'World Wide Web', 'http://']):
            continue
        
        year_match = re.search(r'(\d{4}/\d{2,4}\*?)', line)
        if year_match:
            current_year = standardize_year(year_match.group(1))
            logger.info(f"Found year: {current_year}")
        
        if 'LONG STAPLE VARIETIES' in line.upper():
            in_long_staple_section = True
            in_composition_section = False
            logger.info(f"Entering LONG STAPLE section - will skip")
            continue
        elif 'DUAL INDEX SYSTEM' in line:
            in_composition_section = True
            in_long_staple_section = False
            current_section = 'DUAL_INDEX'
            logger.info(f"Found DUAL INDEX SYSTEM section")
            continue
        elif 'NON-INDEX PRICES' in line:
            in_composition_section = True
            in_long_staple_section = False
            current_section = 'NON_INDEX'
            logger.info(f"Found NON-INDEX PRICES section")
            continue
        
        if in_long_staple_section:
            continue
        
        a_index_patterns = [
            r'(20\d{2}/\d{2,4}\*?)\s+A\s*(?:\([^)]*\))?\s*(Index)?\s+([\d.]+)\s+([-+]?[\d.]+|Unch)',
            r'A\s*(?:\([^)]*\))?\s*Index.*?([\d.]+)\s+([-+]?[\d.]+|Unch)'
        ]
        
        for pattern in a_index_patterns:
            match = re.search(pattern, line)
            if match:
                if len(match.groups()) >= 4 and match.group(1):
                    year_found = standardize_year(match.group(1))
                    price = match.group(3)
                    change_str = match.group(4)
                else:
                    year_found = current_year
                    price = match.group(1)
                    change_str = match.group(2)
                
                change = '0.0' if change_str == 'Unch' else change_str
                
                data.append({
                    'Date': doc_date,
                    'Marketing_Year': year_found or current_year,
                    'Growth': 'A Index Main',
                    'Spot_price': price,
                    'Spot_change': change,
                    'Spot_shpt': '',
                    'forward_price': np.nan,
                    'forward_change': np.nan,
                    'forward_shpt': ''
                })
                in_composition_section = True
                logger.info(f"Found A Index: {price} ({change_str})")
                break
        
        b_index_patterns = [
            r'(20\d{2}/\d{2,4}\*?)\s+B\s+([\d.]+|NQ)\s*([-+]?[\d.]+|Unch|NQ)?',
            r'B\s*Index.*?([\d.]+|NQ)\s*([-+]?[\d.]+|Unch|NQ)?'
        ]
        
        for pattern in b_index_patterns:
            match = re.search(pattern, line)
            if match:
                if len(match.groups()) >= 3 and re.match(r'20\d{2}', match.group(1) or ''):
                    year_found = standardize_year(match.group(1))
                    price_str = match.group(2)
                    change_str = match.group(3) if match.group(3) else '0'
                else:
                    year_found = current_year
                    price_str = match.group(1)
                    change_str = match.group(2) if match.group(2) else '0'
                
                if price_str != 'NQ':
                    price = price_str
                    change = '0.0' if change_str in ['Unch', 'NQ'] else (change_str if change_str else '0.0')
                    
                    data.append({
                        'Date': doc_date,
                        'Marketing_Year': year_found or current_year,
                        'Growth': 'B Index Main',
                        'Spot_price': price,
                        'Spot_change': change,
                        'Spot_shpt': '',
                        'forward_price': np.nan,
                        'forward_change': np.nan,
                        'forward_shpt': ''
                    })
                    logger.info(f"Found B Index: {price} ({change_str})")
                in_composition_section = True
                break
        
        if ('Composition' in line and 'Price' in line) or \
           ('Price' in line and 'Change' in line and 'Shpt' in line):
            continue
        
        if in_composition_section and current_year and not in_long_staple_section:
            if (re.search(r'[A-Za-z]', line) and 
                (re.search(r'[\d.]+', line) or 'NQ' in line or 'NO' in line) and
                not line.startswith('**') and not line.startswith('*')):
                
                parsed_data = parse_cif_europe_data_line(line, doc_date, current_year)
                if parsed_data:
                    logger.info(f"Parsed composition: {[d['Growth'] for d in parsed_data]}")
                    data.extend(parsed_data)
    
    df = pd.DataFrame(data)
    if not df.empty:
        df = df[~df['Growth'].apply(is_long_staple_variety)]
        
        required_columns = ['Date', 'Marketing_Year', 'Growth', 'Spot_price', 'Spot_change', 'Spot_shpt', 'forward_price', 'forward_change', 'forward_shpt']
        for col in required_columns:
            if col not in df.columns:
                df[col] = np.nan if 'price' in col or 'change' in col else ''
        df = df[required_columns]
        df = df.sort_values(['Growth', 'Marketing_Year']).reset_index(drop=True)
    
    logger.info(f"Total compositions extracted: {len(df)}")
    return df

def parse_cif_europe_data_line(line, doc_date, year):
    """Enhanced parsing for CIF Europe format data lines"""
    data = []
    if not line or len(line.strip()) < 5:
        return data
        
    if line.startswith('**') or line.startswith('*') or 'based' in line.lower():
        return data
        
    tokens = line.split()
    if len(tokens) < 2:
        return data

    first_number_idx = find_first_data_index(tokens)
    if first_number_idx == -1 or first_number_idx == 0:
        return data

    composition = ' '.join(tokens[:first_number_idx]).strip()
    if not is_valid_composition(composition):
        return data

    data_tokens = tokens[first_number_idx:]
    
    year_data = extract_single_year_data(data_tokens, 0)
    if year_data:
        data.append({
            'Date': doc_date,
            'Marketing_Year': year,
            'Growth': composition,
            'Spot_price': year_data['price'],
            'Spot_change': year_data['change'],
            'Spot_shpt': year_data['shpt'],
            'forward_price': np.nan,
            'forward_change': np.nan,
            'forward_shpt': ''
        })

    return data

def find_first_data_index(tokens):
    """Find the index of the first data field, skipping numbers that are part of composition names"""
    indicator_words = {'grade', 'giza', 'type', 'no', 'no.'}
    for i, token in enumerate(tokens):
        if is_numeric_value(token):
            cleaned_token = token.rstrip('.').lower()
            if i > 0 and re.match(r'^\d{1,3}$', cleaned_token) and tokens[i-1].rstrip('.').lower() in indicator_words:
                continue
            return i
    return -1

def extract_single_year_data(tokens, start_idx):
    """Extract price, change, and shpt for one year starting from start_idx"""
    if start_idx >= len(tokens):
        return None
        
    result = {
        'price': None,
        'change': None,
        'shpt': '',
        'next_index': start_idx + 1
    }
    
    if start_idx < len(tokens):
        result['price'] = parse_value(tokens[start_idx])
        current_idx = start_idx + 1
    else:
        return None
    
    while current_idx < len(tokens) and current_idx < start_idx + 4:
        token = tokens[current_idx]
        
        if result['change'] is None and is_change_value(token):
            result['change'] = parse_change(token)
        elif result['shpt'] == '' and is_shpt_value(token):
            result['shpt'] = token
        elif is_numeric_value(token) and current_idx > start_idx:
            break
            
        current_idx += 1
    
    result['next_index'] = current_idx
    return result

def create_compositions_csv_dual_year(text, doc_date):
    """Create compositions CSV for PDFs with dual year columns"""
    data = []
    lines = text.split('\n')
    
    table_started = False
    years = extract_years_from_text(text)
    current_section = None
    in_long_staple_section = False
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if 'DISCLAIMER' in line or 'Disclaimer' in line or 'Subscription' in line or 'Quotations in US cents' in line:
            continue
        
        if 'LONG STAPLE VARIETIES' in line.upper():
            in_long_staple_section = True
            continue
        elif any(header in line.upper() for header in ['HIGHER GRADES', 'LOWER GRADES', 'NOTES:']):
            in_long_staple_section = False
            current_section = line.upper()
            continue
        
        if in_long_staple_section:
            continue
        
        year_match = re.search(r'(\d{4}/\d{2,4}\*?)', line)
        if year_match:
            years.append(standardize_year(year_match.group(1)))
            years = list(set(years))
        
        if any(year in line for year in years) and not table_started:
            table_started = True
            continue
        if not table_started:
            continue
        if 'Composition' in line and 'Price' in line and 'Change' in line:
            continue
        
        if current_section != 'LONG STAPLE VARIETIES':
            parsed_data = parse_data_line_dual_year(line, doc_date, years)
            data.extend(parsed_data)
    
    df = pd.DataFrame(data)
    if not df.empty:
        required_columns = ['Date', 'Marketing_Year', 'Growth', 'Spot_price', 'Spot_change', 'Spot_shpt', 'forward_price', 'forward_change', 'forward_shpt']
        for col in required_columns:
            if col not in df.columns:
                df[col] = np.nan if 'price' in col or 'change' in col else ''
        df = df[required_columns]
        df = df.sort_values(['Marketing_Year', 'Growth']).reset_index(drop=True)
    
    return df

def parse_data_line_dual_year(line, doc_date, years):
    """Parse data line for dual year format"""
    data = []
    if not line or len(line.strip()) < 10:
        return data
    
    tokens = line.split()
    if len(tokens) < 4:
        return data

    first_number_idx = find_first_data_index(tokens)
    if first_number_idx == -1 or first_number_idx == 0:
        return data

    composition = ' '.join(tokens[:first_number_idx]).strip()
    if not is_valid_composition(composition) or is_long_staple_variety(composition):
        return data

    data_tokens = tokens[first_number_idx:]
    
    if len(years) >= 1:
        year1_data = extract_single_year_data(data_tokens, 0)
        next_idx = year1_data['next_index'] if year1_data else 0
        
        year2_data = None
        if len(years) >= 2 and next_idx < len(data_tokens):
            year2_data = extract_single_year_data(data_tokens, next_idx)
        
        if year1_data:
            record = {
                'Date': doc_date,
                'Marketing_Year': years[0],
                'Growth': composition,
                'Spot_price': year1_data['price'],
                'Spot_change': year1_data['change'],
                'Spot_shpt': year1_data['shpt'],
                'forward_price': year2_data['price'] if year2_data else np.nan,
                'forward_change': year2_data['change'] if year2_data else np.nan,
                'forward_shpt': year2_data['shpt'] if year2_data else ''
            }
            data.append(record)

    return data

def create_compositions_csv_multi_index_format(text, doc_date):
    """Create compositions CSV for PDFs with multiple indices (A Index and A (NE) Index)"""
    data = []
    lines = text.split('\n')
    
    current_index_type = None
    current_year = None
    in_composition_section = False
    section_type = None
    in_long_staple_section = False
    
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue
            
        if 'DISCLAIMER' in line or 'Disclaimer' in line or 'Subscription' in line:
            continue
        
        if 'LONG STAPLE VARIETIES' in line.upper() or 'Long Staples' in line:
            in_long_staple_section = True
            in_composition_section = False
            continue
        elif any(section in line for section in ['A Index (Far Eastern values)', 'A (NE) Index (N. European values)', 'B (NE) Index']):
            in_long_staple_section = False
            continue
        
        if in_long_staple_section:
            continue
            
        year_match = re.search(r'(\d{4}/\d{2,4}\*?)', line)
        if year_match:
            current_year = standardize_year(year_match.group(1))
        
        if 'A Index (Far Eastern values)' in line or 'The Cotlook A Index (Far Eastern values)' in line:
            current_index_type = "A Index (Far East)"
            continue
        elif 'A (NE) Index (N. European values)' in line or 'Cotlook A (NE) Index (N. European values)' in line:
            current_index_type = "A (NE) Index (N. Europe)"
            continue
        elif 'B (NE) Index' in line:
            current_index_type = "B (NE) Index (N. Europe)"
            continue
            
        index_patterns = [
            r'(20\d{2}/\d{2,4}\*?)\s+A\s+Index\s+([\d.]+)\s+([-+]?[\d.]+|Unch)',
            r'(20\d{2}/\d{2,4}\*?)\s+A\s+\(NE\)\s+Index\s+([\d.]+)\s+([-+]?[\d.]+|Unch)',
            r'(20\d{2}/\d{2,4}\*?)\s+B\s+\(NE\)\s+Index\s+(NQ|[\d.]+)',
        ]
        
        for pattern in index_patterns:
            match = re.search(pattern, line)
            if match:
                current_year = standardize_year(match.group(1))
                if current_index_type and len(match.groups()) >= 3:
                    if match.group(2) != 'NQ':
                        try:
                            price = match.group(2)
                            change_str = match.group(3) if len(match.groups()) > 2 else '0'
                            change = '0.0' if change_str == 'Unch' else change_str
                            
                            data.append({
                                'Date': doc_date,
                                'Marketing_Year': current_year,
                                'Growth': f'{current_index_type} Main',
                                'Spot_price': price,
                                'Spot_change': change,
                                'Spot_shpt': '',
                                'forward_price': np.nan,
                                'forward_change': np.nan,
                                'forward_shpt': ''
                            })
                        except (ValueError, IndexError):
                            pass
                break
        
        if 'Composition' in line and ('Price' in line or 'Change' in line):
            in_composition_section = True
            section_type = 'main'
            continue
        elif 'Higher grades' in line:
            section_type = 'higher_grades'
            continue
        elif 'Lower grades' in line:
            section_type = 'lower_grades'
            continue
        elif 'NOTES:' in line or 'FORWARD QUOTATIONS' in line:
            in_composition_section = False
            continue
            
        if current_index_type and current_year and in_composition_section and not in_long_staple_section:
            if not re.search(r'[A-Za-z]', line) or not re.search(r'[\d.]', line):
                continue
                
            if any(header in line.upper() for header in ['PRICE', 'CHANGE', 'SHPT', 'COMPOSITION']):
                continue
                
            parsed_data = parse_multi_index_data_line(line, doc_date, current_year, current_index_type, section_type)
            if parsed_data:
                filtered_data = [d for d in parsed_data if not is_long_staple_variety(d['Growth'])]
                data.extend(filtered_data)
    
    df = pd.DataFrame(data)
    if not df.empty:
        required_columns = ['Date', 'Marketing_Year', 'Growth', 'Spot_price', 'Spot_change', 'Spot_shpt', 'forward_price', 'forward_change', 'forward_shpt']
        for col in required_columns:
            if col not in df.columns:
                df[col] = np.nan if 'price' in col or 'change' in col else ''
        df = df[required_columns]
        df = df.sort_values(['Marketing_Year', 'Growth']).reset_index(drop=True)
    
    return df

def parse_multi_index_data_line(line, doc_date, year, index_type, section_type):
    """Parse data line for multi-index format"""
    data = []
    if not line or len(line.strip()) < 5:
        return data
    
    tokens = line.split()
    if len(tokens) < 2:
        return data

    first_number_idx = find_first_data_index(tokens)
    if first_number_idx == -1 or first_number_idx == 0:
        return data

    composition = ' '.join(tokens[:first_number_idx]).strip()
    if not is_valid_composition(composition):
        return data

    if section_type == 'higher_grades':
        composition = f"Higher Grade - {composition}"
    elif section_type == 'lower_grades':
        composition = f"Lower Grade - {composition}"

    data_tokens = tokens[first_number_idx:]
    
    year_data = extract_single_year_data(data_tokens, 0)
    if year_data and year_data['price'] is not None:
        record = {
            'Date': doc_date,
            'Marketing_Year': year,
            'Growth': composition,
            'Spot_price': year_data['price'],
            'Spot_change': year_data['change'] if year_data['change'] is not None else 0.0,
            'Spot_shpt': year_data['shpt'],
            'forward_price': np.nan,
            'forward_change': np.nan,
            'forward_shpt': ''
        }
        data.append(record)

    return data

def create_other_csv(text):
    """Create other data CSV from the text - COMPREHENSIVE VERSION WITH UPDATED COLUMN NAMES"""
    data = []
    full_text_joined = ' '.join(text.split('\n'))
    
    doc_date = parse_document_date_to_datetime(text)
    first_year = extract_first_year_from_text(text)
    
    other_section_text = None
    other_patterns = [
        r'Other\s+(.*?)\s+COMMODITY INDICES',
        r'Other\s+(.*?)\s+OTHER QUOTATIONS',
        r'Other\s+(.*?)\s+Cotlook Yarn Index',
        r'Other\s+(.*?)(?:\n\n|\r\n\r\n)'
    ]
    
    for pattern in other_patterns:
        match = re.search(pattern, full_text_joined, re.DOTALL | re.IGNORECASE)
        if match:
            other_section_text = match.group(1)
            break
    
    if not other_section_text:
        return pd.DataFrame()

    # China Cotton Index patterns
    china_patterns = [
        r'China Cotton Index\s+(\d{1,2})-([A-Za-z]+)\s+([\d,]+)\s+([-+]?\d+)\s+yuan/tonne',
        r'China Cotton Index\s+(\d{1,2})-([A-Za-z]+)\s+([\d,]+)\s+yuan/tonne'
    ]
    
    for pattern in china_patterns:
        match = re.search(pattern, other_section_text)
        if match:
            day = match.group(1)
            month = match.group(2)
            value = match.group(3).replace(',', '')
            change = match.group(4) if len(match.groups()) > 3 else ''
            
            # Parse date properly
            try:
                month_map = {
                    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
                }
                month_num = month_map.get(month.lower()[:3])
                if month_num:
                    parsed_date = date(doc_date.year, month_num, int(day))
                else:
                    parsed_date = doc_date
            except:
                parsed_date = doc_date
                
            data.append({
                'Date': parsed_date,
                'Marketing_Year': first_year,
                'Index_Name': 'China Cotton Index',
                'Value': value,
                'Change': change,
                'Unit': 'yuan/tonne'
            })
            break

    # CC Index patterns
    cc_patterns = [
        r'CC Index minus A Index.*?inc one percent duty.*?(\d{1,2})-([A-Za-z]+)\s+([-\d,.\s]+)',
        r'CC Index minus A Index.*?(\d{1,2})-([A-Za-z]+)\s+([-\d,.\s]+)'
    ]
    
    for pattern in cc_patterns:
        match = re.search(pattern, other_section_text)
        if match:
            day = match.group(1)
            month = match.group(2)
            value = match.group(3).replace(',', '').strip()
            
            # Parse date properly
            try:
                month_map = {
                    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
                }
                month_num = month_map.get(month.lower()[:3])
                if month_num:
                    parsed_date = date(doc_date.year, month_num, int(day))
                else:
                    parsed_date = doc_date
            except:
                parsed_date = doc_date
                
            data.append({
                'Date': parsed_date,
                'Marketing_Year': first_year,
                'Index_Name': 'CC Index minus A Index (adjusted to Chinese delivered mill terms, inc one percent duty)',
                'Value': value,
                'Change': '',
                'Unit': 'yuan/tonne'
            })
            break

    # Cotlook patterns
    cotlook_patterns = [
        r'Cotlook A Index.*?including.*?duty.*?(\d{1,2})-([A-Za-z]+)\s+([\d,]+)',
        r'Cotlook A Index.*?(\d{1,2})-([A-Za-z]+)\s+([\d,]+)'
    ]
    
    for pattern in cotlook_patterns:
        match = re.search(pattern, other_section_text)
        if match:
            day = match.group(1)
            month = match.group(2)
            value = match.group(3).replace(',', '')
            
            # Parse date properly
            try:
                month_map = {
                    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
                }
                month_num = month_map.get(month.lower()[:3])
                if month_num:
                    parsed_date = date(doc_date.year, month_num, int(day))
                else:
                    parsed_date = doc_date
            except:
                parsed_date = doc_date
                
            data.append({
                'Date': parsed_date,
                'Marketing_Year': first_year,
                'Index_Name': 'Cotlook A Index',
                'Value': value,
                'Change': '',
                'Unit': ''
            })
            break

    # CEPEA pattern
    cepea_pattern = r'CEPEA/ESALQ.*?(\d{1,2})-([A-Za-z]+)\s+([\d.]+)\s+([-+]?[\d.]+)'
    match = re.search(cepea_pattern, other_section_text)
    if match:
        day = match.group(1)
        month = match.group(2)
        value = match.group(3)
        change = match.group(4)
        
        # Parse date properly
        try:
            month_map = {
                'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
            }
            month_num = month_map.get(month.lower()[:3])
            if month_num:
                parsed_date = date(doc_date.year, month_num, int(day))
            else:
                parsed_date = doc_date
        except:
            parsed_date = doc_date
            
        data.append({
            'Date': parsed_date,
            'Marketing_Year': first_year,
            'Index_Name': 'CEPEA/ESALQ Cotton Price Index',
            'Value': value,
            'Change': change,
            'Unit': 'cents of reais per pound weight'
        })

    # KCA patterns
    kca_patterns = [
        r'KCA Spot.*?per maund of ([\d.]+) kgs.*?(\d{1,2})-([A-Za-z]+).*?([\d,]+)\s+([-+]?\d+|Unch)',
        r'KCA Spot.*?(\d{1,2})-([A-Za-z]+).*?([\d,]+)\s+([-+]?\d+|Unch)'
    ]
    
    for pattern in kca_patterns:
        match = re.search(pattern, other_section_text)
        if match:
            groups = match.groups()
            if len(groups) == 5:
                maund_weight = groups[0]
                day = groups[1]
                month = groups[2]
                value = groups[3].replace(',', '')
                change = groups[4]
                index_name = f'KCA Spot (per maund of {maund_weight} kgs)'
            else:
                day = groups[0]
                month = groups[1]
                value = groups[2].replace(',', '')
                change = groups[3]
                index_name = 'KCA Spot'
            
            # Parse date properly
            try:
                month_map = {
                    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
                }
                month_num = month_map.get(month.lower()[:3])
                if month_num:
                    parsed_date = date(doc_date.year, month_num, int(day))
                else:
                    parsed_date = doc_date
            except:
                parsed_date = doc_date
                
            data.append({
                'Date': parsed_date,
                'Marketing_Year': first_year,
                'Index_Name': index_name,
                'Value': value,
                'Change': change,
                'Unit': ''
            })
            break

    # Average of A Index & Uzbek pattern
    avg_pattern = r'Average of A Index & Uzbek\s+(\d{1,2})-([A-Za-z]+)\s+([\d.]+)\s+([-+]?[\d.]+|Unch)'
    match = re.search(avg_pattern, other_section_text)
    if match:
        day = match.group(1)
        month = match.group(2)
        value = match.group(3)
        change = match.group(4)
        
        # Parse date properly
        try:
            month_map = {
                'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
            }
            month_num = month_map.get(month.lower()[:3])
            if month_num:
                parsed_date = date(doc_date.year, month_num, int(day))
            else:
                parsed_date = doc_date
        except:
            parsed_date = doc_date
            
        data.append({
            'Date': parsed_date,
            'Marketing_Year': first_year,
            'Index_Name': 'Average of A Index & Uzbek',
            'Value': value,
            'Change': change,
            'Unit': 'cents/kilo'
        })

    # Keqiao-China Textile Index pattern
    keqiao_pattern = r'Keqiao-China Textile Index\s+(\d{1,2})-([A-Za-z]+)\s+([\d.]+)\s+([-+]?[\d.]+)'
    match = re.search(keqiao_pattern, other_section_text)
    if match:
        day = match.group(1)
        month = match.group(2)
        value = match.group(3)
        change = match.group(4)
        
        # Parse date properly
        try:
            month_map = {
                'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
            }
            month_num = month_map.get(month.lower()[:3])
            if month_num:
                parsed_date = date(doc_date.year, month_num, int(day))
            else:
                parsed_date = doc_date
        except:
            parsed_date = doc_date
            
        data.append({
            'Date': parsed_date,
            'Marketing_Year': first_year,
            'Index_Name': 'Keqiao-China Textile Index',
            'Value': value,
            'Change': change,
            'Unit': 'May 2007 = 100'
        })

    df = pd.DataFrame(data)
    
    required_columns = ['Date', 'Marketing_Year', 'Index_Name', 'Value', 'Change', 'Unit']
    for col in required_columns:
        if col not in df.columns:
            df[col] = np.nan if col in ['Value', 'Change'] else ''
    
    return df[required_columns] if not df.empty else pd.DataFrame(columns=required_columns)

def is_numeric_value(token):
    """Check if token is a numeric value (price)"""
    return re.match(r'^[\d.]+$', token) is not None or token in ['NQ', 'NO']

def is_change_value(token):
    """Check if token is a change value"""
    if token in ['Unch', 'NQ', 'NO', 'unch']:
        return True
    return re.match(r'^[+-]?[\d.]+$', token) is not None

def is_shpt_value(token):
    """Check if token looks like a shipment value (e.g., '9/8', '11/12', '1/2')"""
    return re.match(r'^\d{1,2}/\d{1,2}$', token) is not None

def parse_value(token):
    """Parse price values - keep special values as strings"""
    if token in ['NQ', 'NO']:
        return token
    try:
        return float(token)
    except ValueError:
        return None

def parse_change(token):
    """Parse change values"""
    if token in ['Unch', 'unch']:
        return 0.0
    if token in ['NQ', 'NO']:
        return None
    try:
        return float(token)
    except ValueError:
        return None

def extract_single_year_data_enhanced(tokens, start_idx):
    """Enhanced version to handle PDF format better"""
    if start_idx >= len(tokens):
        return None
    
    result = {
        'price': None,
        'change': None,
        'shpt': '',
        'next_index': start_idx + 1
    }
    
    if start_idx < len(tokens):
        result['price'] = parse_value(tokens[start_idx])
        current_idx = start_idx + 1
    else:
        return None
    
    if result['price'] in ['NQ', 'NO']:
        result['change'] = None
        result['shpt'] = ''
        return result
    
    while current_idx < len(tokens) and current_idx < start_idx + 4:
        token = tokens[current_idx]
        
        if result['change'] is None and is_change_value(token):
            result['change'] = parse_change(token)
        elif result['shpt'] == '' and is_shpt_value(token):
            result['shpt'] = token
        elif is_numeric_value(token) and current_idx > start_idx:
            break
            
        current_idx += 1
    
    result['next_index'] = current_idx
    return result

def create_compositions_csv_dual_index_system(text, doc_date):
    """Create compositions CSV for PDFs with DUAL INDEX SYSTEM"""
    data = []
    lines = text.split('\n')
    
    current_section = None
    current_year = None
    current_index_type = None
    in_data_section = False
    in_long_staple_section = False
    
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue
            
        if 'DISCLAIMER' in line or 'Disclaimer' in line or 'Subscription' in line:
            continue
        
        if 'LONG STAPLE VARIETIES' in line.upper():
            in_long_staple_section = True
            in_data_section = False
            continue
        elif 'DUAL INDEX SYSTEM' in line:
            in_long_staple_section = False
            in_data_section = True
            continue
        elif 'NON-INDEX PRICES' in line:
            in_long_staple_section = False
            current_section = 'NON_INDEX'
            continue
            
        if in_long_staple_section:
            continue
        
        a_index_pattern = r'(20\d{2}/\d{2,4}\*?)\s+A\s+\([^)]*\)\s+([\d.]+)\s+([-+]?[\d.]+|Unch)'
        a_match = re.search(a_index_pattern, line)
        
        if a_match:
            current_year = standardize_year(a_match.group(1))
            current_index_type = 'A Index'
            price = a_match.group(2)
            change_str = a_match.group(3)
            change = '0.0' if change_str == 'Unch' else change_str
            
            data.append({
                'Date': doc_date,
                'Marketing_Year': current_year,
                'Growth': 'A Index Main',
                'Spot_price': price,
                'Spot_change': change,
                'Spot_shpt': '',
                'forward_price': np.nan,
                'forward_change': np.nan,
                'forward_shpt': ''
            })
            current_section = 'A_INDEX'
            continue
        
        b_index_pattern = r'(20\d{2}/\d{2,4}\*?)\s+B\s+([\d.]+|NQ)\s*([-+]?[\d.]+|Unch|NQ)?'
        b_match = re.search(b_index_pattern, line)
        
        if b_match:
            current_year = standardize_year(b_match.group(1))
            current_index_type = 'B Index'
            price_str = b_match.group(2)
            change_str = b_match.group(3) if b_match.group(3) else '0'
            
            if price_str != 'NQ':
                price = price_str
                change = '0.0' if change_str in ['Unch', 'NQ'] else (change_str if change_str else '0.0')
                
                data.append({
                    'Date': doc_date,
                    'Marketing_Year': current_year,
                    'Growth': 'B Index Main',
                    'Spot_price': price,
                    'Spot_change': change,
                    'Spot_shpt': '',
                    'forward_price': np.nan,
                    'forward_change': np.nan,
                    'forward_shpt': ''
                })
            current_section = 'B_INDEX'
            continue
        
        if ('Composition' in line and 'Price' in line and 'Change' in line) or \
           ('Price' in line and 'Change' in line and 'Shpt' in line):
            continue
            
        if current_section and current_year and in_data_section and not in_long_staple_section:
            if (re.search(r'[A-Za-z]', line) and 
                (re.search(r'[\d.]+', line) or 'NQ' in line) and
                not re.search(r'^\*\*', line) and
                len(line.split()) >= 3):
                
                parsed_data = parse_dual_index_data_line_enhanced(line, doc_date, current_year, current_index_type, current_section)
                if parsed_data:
                    filtered_data = [d for d in parsed_data if not is_long_staple_variety(d['Growth'])]
                    data.extend(filtered_data)
    
    df = pd.DataFrame(data)
    if not df.empty:
        required_columns = ['Date', 'Marketing_Year', 'Growth', 'Spot_price', 'Spot_change', 'Spot_shpt', 'forward_price', 'forward_change', 'forward_shpt']
        for col in required_columns:
            if col not in df.columns:
                df[col] = np.nan if 'price' in col or 'change' in col else ''
        df = df[required_columns]
        df = df.sort_values(['Marketing_Year', 'Growth']).reset_index(drop=True)
    
    return df

def parse_dual_index_data_line_enhanced(line, doc_date, year, index_type, section_type):
    """Enhanced parsing for dual index system format"""
    data = []
    if not line or len(line.strip()) < 5:
        return data
    
    if line.startswith('**') or line.startswith('*'):
        return data
    
    tokens = line.split()
    if len(tokens) < 3:
        return data

    first_number_idx = find_first_data_index(tokens)
    if first_number_idx == -1 or first_number_idx == 0:
        return data

    composition = ' '.join(tokens[:first_number_idx]).strip()
    if not is_valid_composition(composition):
        return data

    if section_type == 'NON_INDEX':
        composition = f"Non-Index - {composition}"

    data_tokens = tokens[first_number_idx:]
    
    year_data = extract_single_year_data(data_tokens, 0)
    if year_data and year_data['price'] is not None:
        record = {
            'Date': doc_date,
            'Marketing_Year': year,
            'Growth': composition,
            'Spot_price': year_data['price'],
            'Spot_change': year_data['change'] if year_data['change'] is not None else 0.0,
            'Spot_shpt': year_data['shpt'],
            'forward_price': np.nan,
            'forward_change': np.nan,
            'forward_shpt': ''
        }
        data.append(record)

    return data

def create_compositions_csv_single_year(text, doc_date):
    """Create compositions CSV for PDFs with single year data"""
    data = []
    lines = text.split('\n')
    table_started = False
    current_year = standardize_year(extract_single_year_from_text(text))
    current_section = None
    in_long_staple_section = False
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if 'DISCLAIMER' in line or 'Disclaimer' in line or 'Subscription' in line or 'Quotations in US cents' in line:
            continue
        
        if 'LONG STAPLE VARIETIES' in line.upper():
            in_long_staple_section = True
            continue
        elif any(header in line.upper() for header in ['HIGHER GRADES', 'LOWER GRADES', 'NOTES:']):
            in_long_staple_section = False
            current_section = line.upper()
            continue
        
        if in_long_staple_section:
            continue
        
        year_match = re.search(r'(\d{4}/\d{2,4}\*?)', line)
        if year_match:
            current_year = standardize_year(year_match.group(1))
        
        if 'A Index' in line and not table_started:
            table_started = True
            index_data = parse_a_index_line(line, doc_date, current_year)
            if index_data:
                data.extend(index_data)
            continue
        
        if not table_started:
            continue
        if 'Composition' in line and 'Price' in line:
            continue
        
        if current_section != 'LONG STAPLE VARIETIES':
            parsed_data = parse_data_line_single_year(line, doc_date, current_year)
            filtered_data = [d for d in parsed_data if not is_long_staple_variety(d['Growth'])]
            data.extend(filtered_data)
    
    df = pd.DataFrame(data)
    if not df.empty:
        required_columns = ['Date', 'Marketing_Year', 'Growth', 'Spot_price', 'Spot_change', 'Spot_shpt', 'forward_price', 'forward_change', 'forward_shpt']
        for col in required_columns:
            if col not in df.columns:
                df[col] = np.nan if 'price' in col or 'change' in col else ''
        df = df[required_columns]
        df = df.sort_values(['Growth', 'Marketing_Year']).reset_index(drop=True)
    
    return df

def parse_a_index_line(line, doc_date, year):
    """Parse A Index line for single year PDFs"""
    data = []
    index_match = re.search(r'A Index.*?(\d+\.\d+)\s+([-+]?\d+\.\d+|Unch)', line)
    if index_match:
        price = index_match.group(1)
        change_str = index_match.group(2)
        change = '0.0' if change_str == 'Unch' else change_str
        
        data.append({
            'Date': doc_date,
            'Marketing_Year': year,
            'Growth': 'A Index Main',
            'Spot_price': price,
            'Spot_change': change,
            'Spot_shpt': '',
            'forward_price': np.nan,
            'forward_change': np.nan,
            'forward_shpt': ''
        })
    
    return data

def parse_data_line_single_year(line, doc_date, year):
    """Parse data line for single year format"""
    data = []
    if not line or len(line.strip()) < 5:
        return data
    
    tokens = line.split()
    if len(tokens) < 2:
        return data

    first_number_idx = find_first_data_index(tokens)
    if first_number_idx == -1 or first_number_idx == 0:
        return data

    composition = ' '.join(tokens[:first_number_idx]).strip()
    if not is_valid_composition(composition):
        return data

    if is_long_staple_variety(composition):
        return data

    data_tokens = tokens[first_number_idx:]
    
    year_data = extract_single_year_data(data_tokens, 0)
    if year_data and year_data['price'] is not None:
        record = {
            'Date': doc_date,
            'Marketing_Year': year,
            'Growth': composition,
            'Spot_price': year_data['price'],
            'Spot_change': year_data['change'] if year_data['change'] is not None else 0.0,
            'Spot_shpt': year_data['shpt'],
            'forward_price': np.nan,
            'forward_change': np.nan,
            'forward_shpt': ''
        }
        data.append(record)

    return data
