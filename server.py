# -*- coding: utf-8 -*-
from flask import Flask, request, jsonify,  send_file
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime
from io import BytesIO
import gridfs
import secrets
import base64
from datetime import datetime

app = Flask(__name__)

# Configurazione MongoDB
MONGODB_URI = 'mongodb://localhost:27017/'
DATABASE_NAME = 'local'
client = MongoClient(MONGODB_URI)
db = client[DATABASE_NAME]
users_collection = db["users"]
client_collection = db["client"]
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
        "dati": []  # Inizializzato come array
    }

    client_collection.insert_one(cliente)
    return jsonify({
        "message": "Cliente creato con successo",
        "cliente": {"nome": nome, "dati": cliente["dati"]}
    }), 201


@app.route('/add_energy_data', methods=['POST'])
def add_energy_data():
    api_key = request.headers.get("X-API-KEY")
    if not api_key:
        return jsonify({"error": "API Key mancante"}), 401

    # Verifica se l'utente è valido
    user = users_collection.find_one({"api_key": api_key})
    if not user:
        return jsonify({"error": "API Key non valida"}), 401

    # Trova il cliente associato all'utente
    cliente = client_collection.find_one({"utenti": api_key})
    if not cliente:
        return jsonify({"error": "L'utente non è associato a nessun cliente"}), 403

    # Recupera il JSON dai dati della richiesta
    data = request.json
    if not data or not isinstance(data, dict) or 'anno' not in data or 'dati' not in data:
        return jsonify({"error": "Formato dati non valido. Deve essere un dizionario con 'anno' e 'dati'"}), 400

    # Verifica l'anno
    try:
        anno = int(data['anno'])
    except ValueError:
        return jsonify({"error": "Anno non valido. Deve essere un numero intero"}), 400

    # Genera il timestamp corrente
    timestamp = datetime.utcnow()

    # Prepara un nuovo documento per il cliente
    nuovo_documento = {
        "nome": cliente["nome"],
        "timestamp": timestamp,
        "username": user["username"],
        "utenti": cliente["utenti"],
        "dati": {
            "Elettricità": [],
            "Gas": [],
            "Diesel": []
        }
    }

    # Processa i dati energetici e aggiungili nelle categorie appropriate
    total_sum = 0  # Variabile per calcolare la somma totale
    for item in data['dati']:
        if 'document_name' not in item or 'period' not in item or 'start_date' not in item['period'] or 'end_date' not in item['period']:
            return jsonify({"error": f"Dati mancanti o incompleti per l'elemento: {item}"}), 400

        # Verifica se i dati appartengono all'anno specificato
        data_inizio = datetime.strptime(item['period']['start_date'], "%Y-%m-%d")
        data_fine = datetime.strptime(item['period']['end_date'], "%Y-%m-%d")
        if data_inizio.year != anno and data_fine.year != anno:
            continue

        if 'total_electricity_consumption' in item:
            categoria = "Elettricità"
            dettagli = {
                "document_name": item["document_name"],
                "period": item["period"],
                "consumption": item["total_electricity_consumption"]
            }
            total_sum += item["total_electricity_consumption"]["value"]
        elif 'consumption_sMc' in item:
            categoria = "Gas"
            dettagli = {
                "document_name": item["document_name"],
                "period": item["period"],
                "consumption": item["consumption_sMc"]
            }
            total_sum += item["consumption_sMc"]["value"]
        elif 'total_diesel_consumption' in item:
            categoria = "Diesel"
            dettagli = {
                "document_name": item["document_name"],
                "period": item["period"],
                "consumption": item["total_diesel_consumption"]
            }
            total_sum += item["total_diesel_consumption"]["value"]
        else:
            return jsonify({"error": f"Nessuna categoria valida trovata per l'elemento: {item}"}), 400

        # Aggiungi i dettagli alla categoria appropriata
        nuovo_documento["dati"][categoria].append(dettagli)

    # Inserisci il nuovo documento nella collezione `client`
    client_collection.insert_one(nuovo_documento)

    return jsonify({
        "message": f"Nuovo documento creato per il cliente {cliente['nome']} con dati energetici",
        "username": user["username"],
        "timestamp": timestamp,
        "anno": anno,
        "somma_totale": total_sum
    }), 201

@app.route('/get_category_sum', methods=['GET'])
def get_category_sum():
    api_key = request.headers.get("X-API-KEY")
    if not api_key:
        return jsonify({"error": "API Key mancante"}), 401

    # Verifica se l'utente è valido
    user = users_collection.find_one({"api_key": api_key})
    if not user:
        return jsonify({"error": "API Key non valida"}), 401

    # Trova il cliente associato all'utente
    cliente = client_collection.find_one({"utenti": api_key})
    if not cliente:
        return jsonify({"error": "L'utente non è associato a nessun cliente"}), 403

    # Recupera i parametri dalla query string
    categoria = request.args.get('categoria')
    anno_inizio = request.args.get('anno_inizio')
    anno_fine = request.args.get('anno_fine')

    # Controlla i parametri obbligatori
    if not categoria or not anno_inizio or not anno_fine:
        return jsonify({"error": "Parametri mancanti. Specificare categoria, anno_inizio e anno_fine"}), 400

    # Verifica che gli anni siano numeri validi
    try:
        anno_inizio = int(anno_inizio)
        anno_fine = int(anno_fine)
    except ValueError:
        return jsonify({"error": "Formato anno non valido. Deve essere un numero intero"}), 400

    if anno_inizio > anno_fine:
        return jsonify({"error": "anno_inizio non può essere successivo a anno_fine"}), 400

    # Controlla se la categoria è valida
    if categoria not in ["Elettricità", "Gas", "Diesel"]:
        return jsonify({"error": "Categoria non valida. Scegliere tra Elettricità, Gas o Diesel"}), 400

    # Recupera l'ultimo documento creato dall'utente
    ultimo_documento = client_collection.find_one(
        {"utenti": api_key},
        sort=[("timestamp", -1)]  # Ordina per timestamp decrescente
    )

    if not ultimo_documento or not ultimo_documento.get("dati"):
        return jsonify({"error": "Nessun dato energetico trovato"}), 404

    # Recupera i dati della categoria specificata
    dati_categoria = ultimo_documento["dati"].get(categoria, [])

    if not dati_categoria:
        return jsonify({"error": f"Nessun dato trovato per la categoria {categoria}"}), 404

    # Filtra i dati della categoria specificata nel periodo richiesto
    somma = 0
    for dato in dati_categoria:
        data_inizio = datetime.strptime(dato["period"]["start_date"], "%Y-%m-%d")
        data_fine = datetime.strptime(dato["period"]["end_date"], "%Y-%m-%d")

        # Controlla se il periodo del dato è compreso nell'intervallo di anni richiesto
        if anno_inizio <= data_inizio.year <= anno_fine or anno_inizio <= data_fine.year <= anno_fine:
            somma += dato["consumption"]["value"]

    return jsonify({
        "categoria": categoria,
        "anno_inizio": anno_inizio,
        "anno_fine": anno_fine,
        "somma": somma
    }), 200

@app.route('/get_client_data', methods=['GET'])
def get_client_data():
    # Recupera l'API Key dagli header
    api_key = request.headers.get("X-API-KEY")
    if not api_key:
        return jsonify({"error": "API Key mancante"}), 401

    # Trova l'utente tramite l'API Key
    user = users_collection.find_one({"api_key": api_key})
    if not user:
        return jsonify({"error": "API Key non valida"}), 401

    # Recupera il nome del cliente dai parametri della richiesta
    nome_cliente = request.args.get('nome')
    if not nome_cliente:
        return jsonify({"error": "Nome cliente mancante"}), 400

    # Trova i documenti relativi al cliente specificato
    documenti_cliente = list(client_collection.find({"nome": nome_cliente}))
    if not documenti_cliente:
        return jsonify({"error": "Cliente non trovato"}), 404

    # Verifica che l'utente sia associato al cliente
    associato = any(api_key in documento.get("utenti", []) for documento in documenti_cliente)
    if not associato:
        return jsonify({"error": "L'utente non è autorizzato a visualizzare i dati di questo cliente"}), 403

    # Trova l'ultimo documento creato per il cliente
    ultimo_documento = max(documenti_cliente, key=lambda doc: doc["timestamp"])

    # Recupera gli utenti associati tramite le API key memorizzate
    utenti_associati = users_collection.find({"api_key": {"$in": ultimo_documento.get("utenti", [])}})
    utenti = [{"username": utente["username"], "email": utente["email"]} for utente in utenti_associati]

    # Restituisce i dati del cliente
    return jsonify({
        "cliente": ultimo_documento["nome"],
        "timestamp": ultimo_documento["timestamp"],
        "username": ultimo_documento["username"],
        "dati": ultimo_documento["dati"],
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
    app.run(host='0.0.0.0', port=5000)