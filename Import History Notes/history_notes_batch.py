import xmlrpc.client
import pandas as pd

import re
# Odoo connection details

url = 'http://localhost:17175'
db = 'his_stage_01112025'
user = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'


# Odoo connection details
#
# url = 'https://hotel-internet-services-stage-12503805.dev.odoo.com'
# db = 'hotel-internet-services-stage-12503805'
# user = 'kaja@blackbadger.biz'
# password = 'kaja@blackbadger.biz'

# # Production
url = 'https://touchstone1.odoo.com'
db = 'hotel-internet-services-live-10380387'
username = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'

# XML-RPC clients with allow_none=True
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', allow_none=True)
uid = common.authenticate(db, user, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', allow_none=True)

# File path
file_path = "history-records-all.csv"

# Load full CSV and ensure 'Import Status' column exists
df = pd.read_csv(file_path)
if 'Import Status' not in df.columns:
    df['Import Status'] = ''

def convert_value(val):
    if pd.isna(val):
        return ""  # Keep empty string if null
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d")  # Convert dates to string
    return str(val)
def clean_value(val):
    """Remove invalid XML control characters."""
    if isinstance(val, str):
        # Remove ASCII control chars 0x00–0x1F except tab/newline/carriage return
        val = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", val)
    return val

for col in df.columns:
    df[col] = df[col].map(convert_value)
    df[col] = df[col].map(clean_value)


# Process full file (your original logic)
subset_df = df.copy()

# Step 1: Collect unique values
type_names = subset_df['Type'].dropna().unique().tolist()
creator_ids = subset_df['Creator CRM ID'].dropna().astype(str).unique().tolist()
contact_ids = subset_df['CRM Contact ID'].dropna().astype(str).unique().tolist()
client_ids = subset_df['CRM Client ID'].dropna().astype(str).unique().tolist()

# Step 2: Batch search_read
type_map = {
    r['name'].lower(): r['id']
    for r in models.execute_kw(
        db, uid, password, 'type.of.note', 'search_read',
        [[('name', 'in', type_names),'|',('active','=',True),('active','=',False)]],
        {'fields': ['id', 'name']}
    )
}

user_map = {
    str(r['crm_client_id']): r['id']
    for r in models.execute_kw(
        db, uid, password, 'res.users', 'search_read',
        [[('crm_client_id', 'in', creator_ids),'|',('active','=',True),('active','=',False)]],
        {'fields': ['id', 'crm_client_id']}
    )
}

partner_map = {
    str(r['crm_client_id']): r['id']
    for r in models.execute_kw(
        db, uid, password, 'res.partner', 'search_read',
        [[('crm_client_id', 'in', client_ids)]],
        {'fields': ['id', 'crm_client_id']}
    )
}

property_map = {
    str(r['his_crm_client_id']): r['id']
    for r in models.execute_kw(
        db, uid, password, 'site.site', 'search_read',
        [[('his_crm_client_id', 'in', client_ids)]],
        {'fields': ['id', 'his_crm_client_id']}
    )
}

contact_map = {
    str(r['crm_client_id']): r['id']
    for r in models.execute_kw(
        db, uid, password, 'res.partner', 'search_read',
        [[('crm_client_id', 'in', contact_ids)]],
        {'fields': ['id', 'crm_client_id']}
    )
}

# Step 3: Build records
records_to_import = []
status_comments = {}
missing_users = {}

for index, row in subset_df.iterrows():
    history_id = str(row.get('CRM History Record ID', '')).strip()
    type_name = str(row.get('Type', '')).lower()
    type_name_org = str(row.get('Type', ''))
    creator_id = str(row.get('Creator CRM ID', '')).strip()
    creator_name = str(row.get('Created By Name', '')).strip()
    contact_id = str(row.get('CRM Client ID', '')).strip()
    contact_partner_id = str(row.get('CRM Contact ID', '')).strip()
    client_id = str(row.get('CRM Client ID', '')).strip()

    type_id = type_map.get(type_name, False)
    user_id = user_map.get(creator_id, False)
    partner_id = partner_map.get(contact_id, False)
    contact_partner_id = contact_map.get(contact_partner_id, False)
    site_id = property_map.get(client_id, False)

    create_date_raw = row.get('Date Entered', '')
    crm_ticket_id = row.get('Ticket ID', '')
    if pd.isna(crm_ticket_id):
        crm_ticket_id = ''

    try:
        dt = pd.to_datetime(create_date_raw, errors='coerce')
        create_date = None if pd.isnull(dt) else dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        create_date = None

    comment = ""
    if not type_id:
        comment += "Type not found. "
    if not user_id:
        comment += "User not found. "
        missing_users[creator_id] = creator_name

    if not partner_id:
        comment += "Company not found. "
    if not contact_partner_id:
        comment += "Contact not found. "
    if not site_id:
        comment += "Site not found. "

    raw_record = {
        'crm_history_record': history_id,
        'name': '' if pd.isna(row.get('Summary')) else str(row.get('Summary')).strip(),
        'description': '' if pd.isna(row.get('Details')) else str(row.get('Details')).strip(),
        'email_to_text': '' if pd.isna(row.get('Email to Text')) else str(row.get('Email to Text')).strip(),
        'partner_id': partner_id,
        'contact_partner_id': contact_partner_id,
        'type_of_note_id': type_id,
        'site_id': site_id,
        'create_date': create_date,
        'create_uid': user_id,
        'crm_ticket_id': crm_ticket_id
    }

    records_to_import.append(raw_record)
    status_comments[index] = comment.strip()

# ------------------------------------------------------------
# BATCH PROCESSING (1000 records per batch)
# ------------------------------------------------------------
BATCH_SIZE = 1000
final_results = []

print(f"Total notes to import: {len(records_to_import)}")
print(f"Processing in batches of {BATCH_SIZE}...\n")

for start in range(53000, len(records_to_import), BATCH_SIZE):
    end = start + BATCH_SIZE
    batch_records = records_to_import[start:end]

    print(f"➡ Processing batch {start} → {start + len(batch_records)}")

    batch_result = models.execute_kw(
        db, uid, password,
        'custom.notes', 'import_bulk_notes',
        [batch_records]
    )
    # except Exception as e:
    #     batch_result = [{'error': str(e)} for _ in batch_records]

    final_results.extend(batch_result)
    print(f"✔ Completed batch ({len(batch_records)})\n")

# Step 7: Save updated file
# for idx, msg in status_comments.items():
#     df.at[idx, 'Import Status'] = msg.strip() if msg else ''
#
# df.to_csv(file_path, index=False)
