import xmlrpc.client
import base64
import pandas as pd
import os

# -----------------------------
# Odoo connection settings
# -----------------------------

# Odoo connection details
url = 'http://localhost:17175'
db = 'his_stage_02072025'
username = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'


ODOO_URL     = "http://localhost:17175"
ODOO_DB      = "his_stage_02072025"
ODOO_USER    = "kaja@blackbadger.biz"
ODOO_PASS    = "kaja@blackbadger.biz"
DRY_RUN      = False       # True = only build files, don't create in Odoo


# Odoo connection details
# url = 'https://hotel-internet-services-stage-12503805.dev.odoo.com'
# db = 'hotel-internet-services-stage-12503805'
# username = 'kaja@blackbadger.biz'
# password = 'kaja@blackbadger.biz'

excel_path = r'attachment.xlsx'

# -----------------------------
# Authenticate
# -----------------------------
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
uid = common.authenticate(db, username, password, {})
if not uid:
    raise Exception("Authentication failed")

models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')

# -----------------------------
# Find Site Map folder in Documents
# -----------------------------
folder_ids = models.execute_kw(
    db, uid, password,
    'documents.folder', 'search',
    [[['name', 'ilike', 'Sitemap']]],
    {'limit': 1}
)
if not folder_ids:
    raise Exception("Site Map folder not found")
folder_id = folder_ids[0]

# -----------------------------
# Read Excel file
# -----------------------------
df = pd.read_excel(excel_path)

# -----------------------------
# Loop through each row
# -----------------------------
for _, row in df.iterrows():
    survey_name = str(row['Survey name']).strip()
    file_path = str(row['File']).strip()
    survey_type_name = str(row['Type']).strip()

    # Find site survey record by name
    site_survey_ids = models.execute_kw(
        db, uid, password,
        'his.site.survey', 'search',
        [[('site_survey', '=', survey_name)]],
        {'limit': 1}
    )
    if not site_survey_ids or not os.path.isfile(file_path):
        continue

    site_survey_id = site_survey_ids[0]

    # Get property_id and partner_id
    survey_data = models.execute_kw(
        db, uid, password,
        'his.site.survey', 'read',
        [site_survey_id],
        {'fields': ['property_id']}
    )
    property_info = survey_data[0]['property_id']
    property_id = property_info[0] if property_info else False

    partner_id = False
    if property_id:
        property_data = models.execute_kw(
            db, uid, password,
            'site.site', 'read',
            [property_id],
            {'fields': ['partner_id']}
        )
        partner_info = property_data[0]['partner_id']
        partner_id = partner_info[0] if partner_info else False

    # Create attachment
    with open(file_path, 'rb') as f:
        file_data = base64.b64encode(f.read()).decode('utf-8')
    file_name = os.path.basename(file_path)

    attachment_id = models.execute_kw(
        db, uid, password,
        'ir.attachment', 'create',
        [{
            'name': file_name,
            'datas': file_data,
            'res_model': 'his.site.survey',
            'res_id': site_survey_id,
        }]
    )

    # Get multi.file.upload.type ID
    type_records = models.execute_kw(
        db, uid, password,
        'multi.file.upload.type', 'search_read',
        [[('name', '=', survey_type_name)]],
        {'fields': ['id'], 'limit': 1}
    )
    type_id = type_records[0]['id'] if type_records else False

    # Create document in Documents
    document_id = models.execute_kw(
        db, uid, password,
        'documents.document', 'create',
        [{
            'name': file_name,
            'folder_id': folder_id,
            'owner_id': uid,
            'datas': file_data,
            # 'res_model': 'his.site.survey',
            # 'res_id': site_survey_id,
            'site_survey_id': site_survey_id,
            'property_id': property_id,
            'partner_id': partner_id,
            'multi_upload_type_id': type_id
        }]
    )

    # Link document to site.survey.other.docs
    models.execute_kw(
        db, uid, password,
        'site.survey.other.docs', 'create',
        [{
            'name': file_name,
            'document_id': document_id,
            'site_survey_id': site_survey_id,
        }]
    )
