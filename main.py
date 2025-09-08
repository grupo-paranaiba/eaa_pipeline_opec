import os
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from flask import Flask, jsonify

# Inicializa Flask (para Cloud Functions)
app = Flask(__name__)

"""Token da API via variável de ambiente"""

ADSIM_BEARER = os.environ.get("ADSIM_BEARER")
if not ADSIM_BEARER:
    raise ValueError("Variável de ambiente ADSIM_BEARER não encontrada!")

def fetch_activities(start_iso, end_iso, limit=100):
    """Busca dados da API ADSIM entre start_iso e end_iso"""
    url = "https://api.adsim.co/crm-r/api/v2/activity"
    headers = {
        "accept": "application/x-ndjson",
        "Authorization": f"Bearer {ADSIM_BEARER}"
    }
    params = {
        "start": start_iso,
        "end": end_iso,
        "ignoreDeleted": "true",
        "limit": limit
    }

    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        raise ValueError({
            "status": "error",
            "message": f"{response.status_code} Client Error for URL: {response.url}",
            "response": response.text
        })
    
    # NDJSON → lista de dicts
    records = [json.loads(line) for line in response.text.strip().split("\n") if line]
    return records

def transform(records):
    """Transforma os dados para DataFrame do BigQuery"""
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # Converte colunas de data
    date_cols = ["startDate", "endDate", "doneDate"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Desaninha JSONs aninhados (type, organization, person, user, company)
    nested_cols = ["type", "organization", "person", "user", "company"]
    for col in nested_cols:
        if col in df.columns:
            nested_df = pd.json_normalize(df[col])
            nested_df.columns = [f"{col}_{subcol}" for subcol in nested_df.columns]
            df = pd.concat([df.drop(columns=[col]), nested_df], axis=1)

    return df

def load_to_bigquery(df, table_id="databaseparanaiba.adsim_dataset.activity"):
    """Carrega o DataFrame no BigQuery"""
    if df.empty:
        return "Sem dados para carregar"
    client = bigquery.Client()
    job = client.load_table_from_dataframe(df, table_id)
    job.result()
    return f"Carregado {job.output_rows} linhas na tabela {table_id}"

# Endpoint HTTP para Cloud Function
@app.route('/', methods=['GET'])
def adsim_activity_etl():
    # Calcula período dos últimos 3 meses
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=90)  # 3 meses atrás

    all_records = []
    batch_size_days = 1
    current_start = start_date

    while current_start < end_date:
        current_end = min(current_start + timedelta(days=batch_size_days), end_date)
        start_iso = current_start.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        end_iso = current_end.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        
        try:
            batch_records = fetch_activities(start_iso, end_iso)
            all_records.extend(batch_records)
            print(f"Batch {start_iso} → {end_iso}: {len(batch_records)} registros")
        except ValueError as e:
            return jsonify(e.args[0]), 500

        current_start = current_end

    df = transform(all_records)
    load_result = load_to_bigquery(df)

    return jsonify({"status": "success", "records_loaded": len(df), "message": load_result})
   