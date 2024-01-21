import os

input_dir = "data/data"
output_file = "data/concatenated.csv"

with open(output_file, "w") as outfile:
    for file in os.listdir(input_dir):
        if not file.endswith('.csv'):
            continue
        file_path = os.path.join(input_dir, file)
        try:
            with open(file_path) as infile:
                while True:
                    chunk = infile.read(1024 * 1024 * 512)
                    if not chunk:
                        break
                    outfile.write(chunk)
        except:
            print(f"Error reading {file_path}")
