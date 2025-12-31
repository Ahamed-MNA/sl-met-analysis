import glob
import os

def get_file_streams(directory_path="."):
    """
    Yields the content of each .dat file found in the directory.
    Returns a generator of tuples: (filename, content_string)
    """
    dat_files = glob.glob(os.path.join(directory_path, "*.dat"))
    
    print(f"[EXTRACT] Found {len(dat_files)} files.")
    
    for filename in dat_files:
        try:
            with open(filename, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
                # Flatten newlines into spaces to enable stream processing
                clean_content = content.replace('\n', ' ').replace('\r', '')
                yield filename, clean_content
        except Exception as e:
            print(f"[EXTRACT] Error reading {filename}: {e}")