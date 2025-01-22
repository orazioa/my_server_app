# -*- coding: utf-8 -*-
from flask import Flask, request, jsonify,  send_file
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime
from io import BytesIO
import gridfs
import secrets
import base64
import os
import math
import requests
from datetime import datetime

app = Flask(__name__)

# Stringa di connessione a MongoDB Atlas
mongo_uri = os.getenv("MONGO_URI")
if not mongo_uri:
    raise Exception("La variabile MONGO_URI non è configurata!")

# Connessione al database MongoDB Atlas
client = MongoClient(mongo_uri)
db = client["my_database"]  # Nome del database trasferito
users_collection = db["users"]
client_collection = db["clients"]
airports_collection = db["airports"]
fs = gridfs.GridFS(db)  # GridFS per file grandi

def generate_api_key():
    """Genera una API Key univoca."""
    return secrets.token_hex(16)

@app.route('/create_user', methods=['POST'])
def create_user():
    data = request.json
    if not data or 'username' not in data or 'email' not in data:
        return jsonify({"error": "Dati utente mancanti"}), 400

    username = data['username']
    email = data['email']

    existing_user = users_collection.find_one({"email": email})
    if existing_user:
        return jsonify({"error": "Questa email esiste gia'"}), 400

    api_key = generate_api_key()
    user = {
        "username": username,
        "email": email,
        "api_key": api_key,
        "files": {  # Inizializza la struttura dei file
            "pdfs": [],
            "images": [],
            "excels": []
        }
    }
    users_collection.insert_one(user)

    return jsonify({
        "message": "Utente creato con successo",
        "user": {
            "username": username,
            "email": email,
            "api_key": api_key
        }
    }), 201

@app.route('/associate_user_to_client', methods=['POST'])
def associate_user_to_client():
    data = request.json
    if not data or 'nome_cliente' not in data or 'username' not in data:
        return jsonify({"error": "Dati mancanti"}), 400

    nome_cliente = data['nome_cliente']
    username = data['username']

    # Trova il cliente
    cliente = client_collection.find_one({"nome": nome_cliente})
    if not cliente:
        return jsonify({"error": "Cliente non trovato"}), 404

    # Trova l'utente tramite lo username
    user = users_collection.find_one({"username": username})
    if not user:
        return jsonify({"error": "Utente non trovato"}), 404

    api_key = user["api_key"]  # Ottieni l'API key dell'utente

    # Aggiungi l'utente alla lista dei clienti se non è già presente
    if api_key not in cliente.get("utenti", []):
        client_collection.update_one(
            {"nome": nome_cliente},
            {"$push": {"utenti": api_key}}
        )
        return jsonify({"message": f"Utente {username} associato al cliente {nome_cliente}"}), 200
    else:
        return jsonify({"error": f"L'utente {username} è già associato a questo cliente"}), 400

@app.route('/create_client', methods=['POST'])
def create_client():
    data = request.json
    if not data or 'nome' not in data:
        return jsonify({"error": "Dati cliente mancanti"}), 400

    nome = data['nome']

    # Controlla se il cliente esiste già
    existing_client = client_collection.find_one({"nome": nome})
    if existing_client:
        return jsonify({"error": "Il cliente con questo nome esiste già"}), 400

    # Inizializza il cliente con una struttura vuota per i dati energetici
    cliente = {
        "nome": nome,
        "utenti": [],  # Lista di utenti associati al cliente
        "dati": [],  # Inizializzato come array
        "timestamp": datetime.utcnow()  # Timestamp corrente
    }

    client_collection.insert_one(cliente)
    return jsonify({
        "message": "Cliente creato con successo",
        "cliente": {"nome": nome, "dati": cliente["dati"]}
    }), 201

def validate_api_key(api_key):
    """Valida l'API Key e restituisce l'utente associato."""
    if not api_key:
        return None, {"error": "API Key mancante"}, 401

    user = users_collection.find_one({"api_key": api_key})
    if not user:
        return None, {"error": "API Key non valida"}, 401

    return user, None, None

def get_associated_client(api_key):
    """Trova il cliente associato all'utente tramite l'API Key."""
    cliente = client_collection.find_one({"utenti": api_key})
    if not cliente:
        return None, {"error": "L'utente non è associato a nessun cliente"}, 403

    return cliente, None, None

def get_distance_with_api(from_code, to_code):
    """Calcola la distanza utilizzando l'API esterna."""
    api_key = os.getenv("API_AERO_KEY")  # La tua chiave API per l'API esterna
    if not api_key:
        raise Exception("API Key per l'API esterna non configurata")

    units = "km"  # Puoi scegliere 'mi' per miglia
    url = f"http://airport.api.aero/airport/distance/{from_code}/{to_code}?user_key={api_key}&units={units}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if 'distance' in data:
                return float(data['distance'])  # Ritorna la distanza in km
        print(f"API esterna fallita per {from_code}-{to_code}. Risposta: {response.status_code}")
    except Exception as e:
        print(f"Errore durante la chiamata all'API esterna per {from_code}-{to_code}: {str(e)}")
    raise Exception(f"Impossibile calcolare la distanza per {from_code}-{to_code} utilizzando l'API")

def get_and_validate_request_data():
    """Recupera e valida i dati della richiesta."""
    data = request.json
    if not data or not isinstance(data, dict) or 'anno' not in data or 'dati' not in data:
        return None, None, {"error": "Formato dati non valido. Deve essere un dizionario con 'anno' e 'dati'"}, 400
    try:
        anno = int(data['anno'])
    except ValueError:
        return None, None, {"error": "Anno non valido. Deve essere un numero intero"}, 400
    return data, anno, None, None

def process_flight_items_with_notes(items, anno):
    """Processa i dati relativi ai voli e calcola l'impatto per l'anno specificato."""
    valid_data = []
    total_flight_impact = 0
    discarded_files = []

    for item in items:
        # Controlla se il campo 'date' è presente
        if 'date' not in item:
            discarded_files.append(item['document_name'])
            continue

        # Controlla se la data appartiene all'anno specificato
        flight_date = datetime.strptime(item['date'], "%Y-%m-%d")
        if flight_date.year != anno:
            discarded_files.append(item['document_name'])
            continue

        # Calcola la distanza utilizzando l'API esterna
        try:
            distance = get_distance_with_api(item['travel']['from'], item['travel']['to'])
        except Exception as e:
            print(f"Errore: {str(e)}")
            discarded_files.append(item['document_name'])
            continue

        # Calcola l'impatto del volo (distanza * numero di passeggeri)
        flight_impact = distance * item['num_of_travelers']
        total_flight_impact += flight_impact

        # Aggiungi i dati validi
        valid_data.append({
            "document_name": item["document_name"],
            "date": item["date"],
            "travel": item["travel"],
            "num_of_travelers": item["num_of_travelers"],
            "distance": distance,
            "impact": flight_impact
        })

    return valid_data, total_flight_impact, discarded_files

def process_gas_items_with_notes(items, anno):
    """Processa i dati relativi al gas e restituisce i documenti scartati."""
    valid_data = []
    total_gas = 0
    discarded_files = []

    for item in items:
        # Controlla se 'period', 'start_date' e 'end_date' sono presenti
        if 'period' not in item or 'start_date' not in item['period'] or 'end_date' not in item['period']:
            discarded_files.append(item['document_name'])
            continue

        # Controlla se il periodo appartiene all'anno specificato
        start_date = datetime.strptime(item['period']['start_date'], "%Y-%m-%d")
        end_date = datetime.strptime(item['period']['end_date'], "%Y-%m-%d")
        if start_date.year != anno and end_date.year != anno:
            discarded_files.append(item['document_name'])
            continue

        # Somma il consumo
        valid_data.append(item)
        total_gas += item['consumption_sMc']['value']

    return valid_data, total_gas, discarded_files


def process_electricity_items_with_notes(items, anno):
    """Processa i dati relativi all'elettricità e restituisce i documenti scartati."""
    valid_data = []
    total_electricity = 0
    discarded_files = []

    for item in items:
        # Controlla se 'period', 'start_date' e 'end_date' sono presenti
        if 'period' not in item or 'start_date' not in item['period'] or 'end_date' not in item['period']:
            discarded_files.append(item['document_name'])
            continue

        # Controlla se il periodo appartiene all'anno specificato
        start_date = datetime.strptime(item['period']['start_date'], "%Y-%m-%d")
        end_date = datetime.strptime(item['period']['end_date'], "%Y-%m-%d")
        if start_date.year != anno and end_date.year != anno:
            discarded_files.append(item['document_name'])
            continue

        # Somma il consumo
        valid_data.append(item)
        total_electricity += item['total_electricity_consumption']['value']

    return valid_data, total_electricity, discarded_files


@app.route('/add_energy_data', methods=['POST'])
def add_energy_data():
    api_key = request.headers.get("X-API-KEY")

    # Valida l'API Key
    user, error, status_code = validate_api_key(api_key)
    if error:
        return jsonify(error), status_code

    # Trova il cliente associato
    cliente, error, status_code = get_associated_client(api_key)
    if error:
        return jsonify(error), status_code

    # Recupera e valida i dati della richiesta
    data, anno, error, status_code = get_and_validate_request_data()
    if error:
        return jsonify(error), status_code

    # Genera il timestamp corrente
    timestamp = datetime.utcnow()

    # Variabili per i risultati
    discarded_files = []

    # Processa i dati in base al document_type
    document_type = data.get('document_type', '').upper()
    if document_type == 'BUSINESS_TRAVEL':
        flight_data, total_flight_impact, flight_discarded = process_flight_items_with_notes(data['dati'], anno)
        discarded_files.extend(flight_discarded)
        response = {
            "message": f"Nuovo documento creato per il cliente {cliente['nome']}",
            "document_type": "BUSINESS_TRAVEL",
            "value": total_flight_impact,
            "unit": "km",
            "note": "File extraction failed: " + ", ".join(flight_discarded) if flight_discarded else None
        }
    elif document_type == 'GAS':
        gas_data, total_gas, gas_discarded = process_gas_items_with_notes(data['dati'], anno)
        discarded_files.extend(gas_discarded)
        response = {
            "message": f"Nuovo documento creato per il cliente {cliente['nome']}",
            "document_type": "GAS",
            "value": total_gas,
            "unit": "sMc",
            "note": "File extraction failed: " + ", ".join(gas_discarded) if gas_discarded else None
        }
    elif document_type == 'ELECTRICITY':
        electricity_data, total_electricity, electricity_discarded = process_electricity_items_with_notes(data['dati'], anno)
        discarded_files.extend(electricity_discarded)
        response = {
            "message": f"Nuovo documento creato per il cliente {cliente['nome']}",
            "document_type": "ELECTRICITY",
            "value": total_electricity,
            "unit": "kWh",
            "note": "File extraction failed: " + ", ".join(electricity_discarded) if electricity_discarded else None
        }
    else:
        return jsonify({"error": "Tipo di documento non supportato"}), 400

    # Prepara il nuovo documento per il cliente
    nuovo_documento = create_client_document(
        cliente, timestamp, user,
        flight_data if document_type == 'BUSINESS_TRAVEL' else [],
        electricity_data if document_type == 'ELECTRICITY' else [],
        gas_data if document_type == 'GAS' else []
    )

    # Inserisci il nuovo documento nella collezione `client`
    client_collection.insert_one(nuovo_documento)

    return jsonify(response), 201


def create_client_document(cliente, timestamp, user, flight_data, electricity_data, gas_data):
    """Crea il documento per il cliente."""
    return {
        "nome": cliente["nome"],
        "timestamp": timestamp,
        "username": user["username"],
        "utenti": cliente["utenti"],
        "dati": {
            "TotalFlightDist": flight_data,
            "Elettricità": electricity_data,
            "Gas": gas_data
        }
    }


@app.route('/get_client_data', methods=['GET'])
def get_client_data():
    api_key = request.headers.get("X-API-KEY")
    if not api_key:
        return jsonify({"error": "API Key mancante"}), 401

    user = users_collection.find_one({"api_key": api_key})
    if not user:
        return jsonify({"error": "API Key non valida"}), 401

    nome_cliente = request.args.get('nome')
    if not nome_cliente:
        return jsonify({"error": "Nome cliente mancante"}), 400

    documenti_cliente = list(client_collection.find({"nome": nome_cliente}))
    if not documenti_cliente:
        return jsonify({"error": "Cliente non trovato"}), 404

    associato = any(api_key in documento.get("utenti", []) for documento in documenti_cliente)
    if not associato:
        return jsonify({"error": "L'utente non è autorizzato a visualizzare i dati di questo cliente"}), 403

    # Trova l'ultimo documento usando una data minima di default
    ultimo_documento = max(
        documenti_cliente,
        key=lambda doc: doc.get("timestamp", datetime.min)
    )

    utenti_associati = users_collection.find({"api_key": {"$in": ultimo_documento.get("utenti", [])}})
    utenti = [{"username": utente["username"], "email": utente["email"]} for utente in utenti_associati]

    return jsonify({
        "cliente": ultimo_documento["nome"],
        "timestamp": ultimo_documento.get("timestamp"),  # Usa get() per evitare errori
        "username": ultimo_documento.get("username"),  # Usa get() per evitare errori
        "dati": ultimo_documento.get("dati", {}),
        "utenti": utenti
    }), 200


@app.route('/upload', methods=['POST'])
def upload_files():
    api_key = request.headers.get("X-API-KEY")
    if not api_key:
        return jsonify({"error": "API Key mancante"}), 401

    user = users_collection.find_one({"api_key": api_key})
    if not user:
        return jsonify({"error": "API Key non valida"}), 401

    if 'file' not in request.files:
        return jsonify({"error": "Nessun file trovato nella richiesta"}), 400

    files = request.files.getlist('file')
    uploaded_files = []

    for file in files:
        filename = file.filename
        content_type = file.content_type
        file_content = file.read()
        file_size = len(file_content)

        file_id = ObjectId()
        file_data = {
            "file_id": file_id,
            "filename": filename,
            "content_type": content_type,
            "uploaded_at": datetime.utcnow()
        }

        if file_size <= 16 * 1024 * 1024:  # File piccoli
            file_data["content"] = file_content
        else:  # File grandi
            gridfs_id = fs.put(file_content, filename=filename, contentType=content_type)
            file_data["gridfs_id"] = gridfs_id

        # Determina la categoria dei file
        if content_type.startswith('image/'):
            category = "files.images"
        elif content_type == 'application/pdf':
            category = "files.pdfs"
        elif content_type in ['application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'application/vnd.ms-excel']:
            category = "files.excels"
        else:
            return jsonify({"error": f"Tipo di file non supportato: {filename}"}), 400

        # Aggiungi il file nella categoria corretta
        users_collection.update_one(
            {"api_key": api_key},
            {"$push": {category: file_data}}
        )
        uploaded_files.append({"filename": filename, "file_id": str(file_id)})

    return jsonify({"user": user["username"], "uploaded_files": uploaded_files}), 200



@app.route('/get_user_files', methods=['GET'])
def get_user_files():
    # Recupera la chiave API dagli header
    api_key = request.headers.get("X-API-KEY")
    if not api_key:
        return jsonify({"error": "API Key mancante"}), 401

    # Verifica se l'API Key è valida
    user = users_collection.find_one({"api_key": api_key})
    if not user:
        return jsonify({"error": "API Key non valida"}), 401

    # Funzione per formattare i file e convertire ObjectId in stringa
    def format_files(files):
        formatted_files = []
        for file in files:
            formatted_file = {key: str(value) if isinstance(value, ObjectId) else value
                              for key, value in file.items() if key != "content"}
            formatted_files.append(formatted_file)
        return formatted_files

    # Restituisce solo i metadati dei file
    return jsonify({
        "username": user["username"],
        "files": {
            "pdfs": format_files(user["files"]["pdfs"]),
            "images": format_files(user["files"]["images"]),
            "excels": format_files(user["files"]["excels"]),
        },
    }), 200

@app.route('/download', methods=['GET'])
def download_file():
    api_key = request.headers.get("X-API-KEY")
    if not api_key:
        return jsonify({"error": "API Key mancante"}), 401

    user = users_collection.find_one({"api_key": api_key})
    if not user:
        return jsonify({"error": "API Key non valida"}), 401

    file_id = request.args.get('file_id')
    if not file_id:
        return jsonify({"error": "file_id mancante"}), 400

    # Cerca il file in tutte le categorie
    file = None
    for category in ["pdfs", "images", "excels"]:
        file = next((f for f in user["files"][category] if str(f["file_id"]) == file_id), None)
        if file:
            break

    if not file:
        return jsonify({"error": "File non trovato"}), 404

    try:
        if "gridfs_id" in file:
            gridfs_file = fs.get(file["gridfs_id"])
            return send_file(
                BytesIO(gridfs_file.read()),
                as_attachment=True,
                download_name=file["filename"],
                mimetype=file["content_type"]
            )
        elif "content" in file:
            return send_file(
                BytesIO(file["content"]),
                as_attachment=True,
                download_name=file["filename"],
                mimetype=file["content_type"]
            )
        else:
            return jsonify({"error": "Il file non contiene dati validi"}), 500
    except Exception as e:
        return jsonify({"error": f"Errore durante il download del file: {str(e)}"}), 500

@app.route('/routes', methods=['GET'])
def list_routes():
    import urllib
    output = []
    for rule in app.url_map.iter_rules():
        methods = ','.join(rule.methods)
        line = urllib.parse.unquote(f"{rule} {methods}")
        output.append(line)
    return jsonify(routes=output)

@app.route('/')
def home():
    return "Hello, Render!"

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)