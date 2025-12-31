import pandas as pd

def load_to_csv(df, output_filename):
    if df.empty:
        print("[LOAD] Warning: No data extracted.")
        return
        
    print(f"[LOAD] Saving {len(df)} rows to {output_filename}...")
    df.to_csv(output_filename, index=False)
    print("[LOAD] Success.")