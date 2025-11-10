#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import xmlrpc.client
import base64
import pandas as pd
import os

# -----------------------------
# Odoo connection settings
# -----------------------------

# Local server
url = 'http://localhost:17175'
db = 'his_stage_02072025'
username = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'

# One set of constants (kept to match your style)
ODOO_URL  = url
ODOO_DB   = db
ODOO_USER = username
ODOO_PASS = password
DRY_RUN   = False   # True = print what would happen, do not create in Odoo

# Input Excel
excel_path = r'new_file.xlsx'

# -----------------------------
# Authenticate
# -----------------------------
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
uid = common.authenticate(db, username, password, {})
if not uid:
    raise Exception("Authentication failed")
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')

# -----------------------------
# Find "Sitemap" folder in Documents
# -----------------------------
folder_ids = models.execute_kw(
    db, uid, password,
    'documents.folder', 'search',
    [[['name', 'ilike', 'Sitemap']]],
    {'limit': 1}
)
if not folder_ids:
    raise Exception("Sitemap folder not found")
folder_id = folder_ids[0]

# -----------------------------
# Read Excel file
# -----------------------------
df = pd.read_excel(excel_path)

# For resolving relative file paths
excel_dir = os.path.dirname(os.path.abspath(excel_path))

creator_ids = df['UploadedByID'].dropna().astype(str).unique().tolist()
user_map = {
    str(r['crm_client_id']): r['id']
    for r in models.execute_kw(db, uid, password, 'res.users', 'search_read',
                               [[('crm_client_id', 'in', creator_ids),'|',('active','=', True),('active','=', False)]], {'fields': ['id', 'crm_client_id']})
}

# -----------------------------
# Loop through each row
# -----------------------------
for _, row in df.iterrows():
    document_id = str(row.get('DocumentID', '')).strip()
    file_path   = str(row.get('FileName', '')).strip()
    mime_type = str(row.get('MIMEType', '')).strip()
    creator_id = str(row.get('UploadedByID', '')).strip()
    user_id = user_map.get(creator_id, False)



    # Resolve relative paths based on excel location
    if not os.path.isabs(file_path):
        file_path = os.path.abspath(os.path.join(excel_dir, file_path))


    if not os.path.isfile(file_path):
        print(f"[SKIP] File not found: {file_path}")
        continue

        # Read and encode file
    with open(file_path, 'rb') as f:
        file_data_b64 = base64.b64encode(f.read()).decode('utf-8')
    file_name = os.path.basename(file_path)




    # Create document (link via attachment_id; put in Sitemap folder; set owner to current user)
    doc_vals = {
        'name': file_name,
        'crm_document_id':document_id,
        'folder_id': folder_id,
        'mimetype': mime_type,
        'datas': file_data_b64,
        'folder_id': int(folder_id),
        'owner_id': uid
    }


    document_id = models.execute_kw(
        db, uid, password,
        'documents.document', 'create',
        [doc_vals]
    )



