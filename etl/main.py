import pandas as pd
from extract import get_file_streams
from transform import transform_single_file
from load import load_to_csv

def main():
    all_data = []

    # 1. Extract & Transform Loop
    # We process file-by-file so we don't mix up station metadata
    for filename, stream_content in get_file_streams(r"C:\uni\3rd Year\Statistical_programming\group project\zip data\zip data\Rainfall  data Sri Lanka\70data"):
        
        file_records = transform_single_file(filename, stream_content)
        all_data.extend(file_records)

    # 2. Create DataFrame
    if all_data:
        df = pd.DataFrame(all_data)
        
        # Create Date Column
        df['Date'] = pd.to_datetime(df[['Year', 'Month', 'Day']])
        
        # Reorder for nice output
        cols = [
            'Station_Name', 'Latitude', 'Longitude', 'Elevation', 
            'Date', 'Year', 'Month', 'Day', 'Precipitation_mm', 'Flag'
        ]
        # Only select columns that exist (in case metadata failed)
        df = df[[c for c in cols if c in df.columns]]
        
        # 3. Load
        load_to_csv(df, "combined_weather_data.csv")
    else:
        print("[MAIN] No data found in any files.")

if __name__ == "__main__":
    main()