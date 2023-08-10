import os
import pandas as pd
import glob


def clean_json_files(directory):
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".json"):
                file_path = os.path.join(root, file)
                with open(file_path, "r") as f:
                    lines = f.readlines()

                cleaned_lines = []
                inside_json = False
                for line in lines:
                    if "{" in line:
                        inside_json = True
                    if inside_json:
                        cleaned_lines.append(line)
                        if "}" in line:
                            break

                cleaned_json = "".join(cleaned_lines)
                with open(file_path, "w") as f:
                    f.write(cleaned_json)


if __name__ == "__main__":
    json_directory = "./input_data/starter/transactions"  # Replace with your JSON directory
    clean_json_files(json_directory)

    transaction_files = glob.glob("./input_data/starter/transactions/**/*.json", recursive=True)
    dfs = []
    for file in transaction_files:
        try:
            df = pd.read_json(file)
            dfs.append(df)
        except Exception as json_err:
            print(f"Error reading JSON file {file}: {json_err}")
    if not dfs:
        raise ValueError("No valid JSON files found.")
    df = pd.concat(dfs, ignore_index=True)

    output_csv_file = "output_data.csv"
    df.to_csv(output_csv_file, index=False)
    print(f"DataFrame saved to {output_csv_file}")
