import pandas as pd
import calendar
import re

def parse_metadata(text_stream):
    """
    Scans the start of the file stream for Station Name, Lat, Lon, and Elev.
    """
    meta = {
        'Station_Name': None,
        'Latitude': None,
        'Longitude': None,
        'Elevation': None
    }

    # Regex 1: Station Name (Looks for text between 'NAME :' and 'LAT')
    # non-greedy match (.*?) stops as soon as it sees LAT
    name_match = re.search(r'STATION NAME\s*:\s*(.*?)\s+LAT', text_stream)
    if name_match:
        meta['Station_Name'] = name_match.group(1).strip()

    # Regex 2: Lat, Lon, Elev
    # We look for patterns like "LAT: 8.33N" or "ELEV: 92.5M"
    lat_match = re.search(r'LAT\s*:\s*([0-9.]+[NS]?)', text_stream)
    if lat_match:
        meta['Latitude'] = lat_match.group(1).strip()

    lon_match = re.search(r'LON\s*:\s*([0-9.]+[EW]?)', text_stream)
    if lon_match:
        meta['Longitude'] = lon_match.group(1).strip()
        
    elev_match = re.search(r'ELEV\s*:\s*([0-9.]+[M]?)', text_stream)
    if elev_match:
        meta['Elevation'] = elev_match.group(1).strip()
        
    return meta

def transform_single_file(filename, text_stream):
    """
    Parses one file's content: extracts metadata first, then all daily data.
    """
    # 1. Get Station Metadata
    metadata = parse_metadata(text_stream)
    print(f"[TRANSFORM] Processing {filename} -> Station: {metadata.get('Station_Name')}")

    # 2. Setup Data Parsing (Same Regex Logic as before)
    header_pattern = re.compile(r'(\d{4})\s*-\s*(\d{1,2})')
    records = []

    for match in header_pattern.finditer(text_stream):
        year = int(match.group(1))
        month = int(match.group(2))
        
        days_in_month = calendar.monthrange(year, month)[1]
        current_pos = match.end()

        for day in range(1, 32):
            if day > days_in_month:
                break
            
            chunk = text_stream[current_pos : current_pos + 9]
            if len(chunk) < 9:
                break

            raw_val = chunk[0:6]
            flag_char = chunk[6:7]
            
            precip = None
            try:
                clean_val = raw_val.strip()
                if clean_val and '-9.9' not in clean_val:
                    precip = float(clean_val)
            except ValueError:
                precip = None
                
            flag = flag_char.strip()
            if '-9.9' in raw_val and not flag:
                flag = 'M'

            # 3. Create Record with Metadata attached
            record = {
                'Station_Name': metadata['Station_Name'],
                'Latitude': metadata['Latitude'],
                'Longitude': metadata['Longitude'],
                'Elevation': metadata['Elevation'],
                'Year': year,
                'Month': month,
                'Day': day,
                'Precipitation_mm': precip,
                'Flag': flag if flag else None
            }
            records.append(record)
            
            current_pos += 9

    return records