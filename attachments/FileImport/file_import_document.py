#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import xmlrpc.client
import base64
import pandas as pd
import os
import mimetypes

# -----------------------------
# Odoo connection settings
# -----------------------------
# url = 'http://localhost:17175'
# db = 'his_stage_20092025'
# username = 'kaja@blackbadger.biz'
# password = 'kaja@blackbadger.biz'

# Odoo connection details
url = 'https://hotel-internet-services-stage-12503805.dev.odoo.com'
db = 'hotel-internet-services-stage-12503805'
username = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'

ODOO_URL  = url
ODOO_DB   = db
ODOO_USER = username
ODOO_PASS = password
DRY_RUN   = False

excel_path = r'import_file.xlsx'

# -----------------------------
# Authenticate
# -----------------------------
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
uid = common.authenticate(db, username, password, {})
if not uid:
    raise Exception("Authentication failed")
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')



# -----------------------------
# Read Excel
# -----------------------------
df = pd.read_excel(excel_path)
df = df.head(3)
excel_dir = os.path.dirname(os.path.abspath(excel_path))


creator_ids = df['UploadedByID'].dropna().astype(str).unique().tolist()
user_map = {
    str(r['crm_client_id']): r['id']
    for r in models.execute_kw(db, uid, password, 'res.users', 'search_read',
                               [[('crm_client_id', 'in', creator_ids),'|',('active','=', True),('active','=', False)]], {'fields': ['id', 'crm_client_id']})
}
doc_vals_list = []
# -----------------------------
# Loop rows
# -----------------------------
for _, row in df.iterrows():

    ext_doc_id = str(row.get('DocumentID', '')).strip()
    file_path = str(row.get('FileName', '')).strip()
    folder = str(row.get('Folder', '')).strip()
    parent_folder = str(row.get('ParentFolder', '')).strip()
    mime_type = str(row.get('MIMEType', '')).strip()
    creator_id = str(row.get('UploadedByID', '')).strip()
    create_date_raw = row.get('UploadedDate', '')
    user_id = user_map.get(creator_id, False)

    # -----------------------------
    # Find "Sitemap" folder in Documents
    # -----------------------------
    folder_ids = models.execute_kw(
        db, uid, password,
        'documents.folder', 'search',
        [[['name', '=', folder], ['parent_folder_id.name', '=', parent_folder]]],
        {'limit': 1}
    )
    if folder_ids:
        folder_id = folder_ids[0]


    else:
        parent_folder_id = models.execute_kw(
            db, uid, password,
            'documents.folder', 'search',
            [[['name', '=', parent_folder]]],
            {'limit': 1}
        )
        folder_id = models.execute_kw(
            db, uid, password,
            'documents.folder', 'create',
            [{
                'name': folder,
                'parent_folder_id': parent_folder_id[0],
            }]
        )

    try:
        create_date_parsed = pd.to_datetime(create_date_raw, errors='coerce')
        if pd.isnull(create_date_parsed):
            create_date = None
        else:
            create_date = create_date_parsed.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        create_date = None

    if not file_path:
        print("[SKIP] Empty FileName")
        continue

    if not os.path.isabs(file_path):
        file_path = os.path.abspath(os.path.join(excel_dir, file_path))

    if not os.path.isfile(file_path):
        print(f"[SKIP] File not found: {file_path}")
        continue

    # --- MIME: force correct values for pdf/txt; fallback to guess ---
    ext = os.path.splitext(file_path)[1].lower()
    if ext == '.pdf':
        mimetype_val = 'application/pdf'
    elif ext in ('.txt', '.log'):
        mimetype_val = 'text/plain'
    else:
        guessed, _ = mimetypes.guess_type(file_path)
        mimetype_val = guessed or 'application/octet-stream'
    # ----------------------------------------------------------------

    with open(file_path, 'rb') as f:
        file_data_b64 = base64.b64encode(f.read()).decode('utf-8')
    file_name = os.path.basename(file_path)

    if DRY_RUN:
        print(f"[DRY] Would create doc+attachment: {file_name} (mime={mimetype_val}) -> folder {folder_id}")
        continue

    # 1) Create the document
    doc_id = models.execute_kw(
        db, uid, password,
        'documents.document', 'create',
        [{
            'name': file_name,
            'crm_document_id': ext_doc_id,
            'folder_id': folder_id,
            'owner_id': uid,
        }]
    )

    # 2) Create the attachment linked to that document
    att_id = models.execute_kw(
        db, uid, password,
        'ir.attachment', 'create',
        [{
            'name': file_name,
            'datas': file_data_b64,
            'mimetype': mimetype_val,
            'res_model': 'documents.document',
            'res_id': doc_id,
            'type': 'binary',
        }]
    )

    # 3) Set attachment on the document
    models.execute_kw(
        db, uid, password,
        'documents.document', 'write',
        [[doc_id], {'attachment_id': att_id}]
    )
    print(f"[OK] {file_name} -> attachment:{att_id} document:{doc_id}")
    if not user_id:
        print(f"User is not available in Odoo - {creator_id}. Create date also not updated")
    if doc_id and att_id and user_id:

        doc_vals_list.append({
            'document_id': doc_id,
            'attachment_id': att_id,
            'create_uid': user_id,
            'create_date': create_date
        })
if doc_vals_list:
    models.execute_kw(
        db, uid, password,
        'documents.document', 'bulk_update_create_date',
        [doc_vals_list]
    )


