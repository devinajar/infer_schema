import pandas as pd
import json
import re

def clean_column_name(column_name):
    # Extract text inside {} if present, otherwise keep the original name
    match = re.search(r'\{(.*?)\}', column_name)
    if match:
        cleaned_name = match.group(1)
    else:
        cleaned_name = column_name
    return cleaned_name.strip()

def infer_field_type(series):
    for value in series:
        if pd.to_numeric(value, errors='coerce') == value:
            if pd.to_numeric(value, downcast='integer', errors='coerce') == value:
                return 'INTEGER'
            elif pd.to_numeric(value, downcast='float', errors='coerce') == value:
                return 'FLOAT'
            else:
                return 'STRING'
        elif str(value).lower() in ['true', 'false']:
            return 'BOOLEAN'
        else:
            return 'STRING'
    return 'STRING'

def infer_bigquery_schema(csv_filename):
    # Read the CSV file using pandas
    df = pd.read_csv(csv_filename)

    # Clean column names and identify header fields
    schema = []
    for column_name, dtype in df.dtypes.items():
        cleaned_name = clean_column_name(column_name)
        if cleaned_name != column_name:
            data_series = df[column_name]
            field_type = infer_field_type(data_series)
            schema.append({"name": cleaned_name, "type": field_type})  # Treat as data field
        else:
            schema.append({"name": column_name, "type": "IGNORE"})   # Treat as header field

    return schema

def save_schema_to_json(schema, json_filename):
    with open(json_filename, 'w') as json_file:
        json.dump(schema, json_file, indent=4)

if __name__ == "__main__":
    csv_filename = "first.csv"  # Replace with your CSV filename
    json_filename = "first_schema.json"  # Replace wsth the desired output JSON filename

    inferred_schema = infer_bigquery_schema(csv_filename)
    cleaned_schema = [field for field in inferred_schema if field["type"] != "IGNORE"]
    save_schema_to_json(cleaned_schema, json_filename)

    print("Schema inference complete. JSON schema saved to", json_filename)

