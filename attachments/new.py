import pandas as pd
import xmlrpc.client
import base64
# Load the CSV file
file_path = "document_test_5.csv"  # update path if needed
df = pd.read_csv(file_path)


# Odoo connection details
# url = 'http://localhost:17175'
# db = 'his_stage_02072025'
# user = 'kaja@blackbadger.biz'
# password = 'kaja@blackbadger.biz'


# Odoo connection details
url = 'https://hotel-internet-services-stage-12503805.dev.odoo.com'
db = 'hotel-internet-services-stage-12503805'
user = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'

# XML-RPC clients with allow_none=True
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', allow_none=True)
uid = common.authenticate(db, user, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', allow_none=True)

# Process only first 2 rows
subset_df = df.head(100)
# subset_df = df.iloc[100:200]
# subset_df = df.copy()

# Step 1: Collect unique values for batch lookup
creator_ids = subset_df['UploadedByID'].dropna().astype(str).unique().tolist()
client_ids = subset_df['ClientID'].dropna().astype(str).unique().tolist()
history_ids = subset_df['HistoryID'].dropna().astype(str).unique().tolist()
ticket_ids = subset_df['TicketID'].dropna().astype(str).unique().tolist()

# Step 2: Batch search_read and build maps

user_map = {
    str(r['crm_client_id']): r['id']
    for r in models.execute_kw(db, uid, password, 'res.users', 'search_read',
                               [[('crm_client_id', 'in', creator_ids),'|',('active','=', True),('active','=', False)]], {'fields': ['id', 'crm_client_id']})
}
# print("creator_ids")
# print(creator_ids)
# print("user_map")
# print(user_map)
partner_map = {
    str(r['crm_client_id']): r['id']
    for r in models.execute_kw(db, uid, password, 'res.partner', 'search_read',
                               [[('crm_client_id', 'in', client_ids)]], {'fields': ['id', 'crm_client_id']})
}
# print("client_ids")
# print(client_ids)
# print("partner_map")
# print(partner_map)
property_map = {
    str(r['his_crm_client_id']): r['id']
    for r in models.execute_kw(db, uid, password, 'site.site', 'search_read',
                               [[('his_crm_client_id', 'in', client_ids)]], {'fields': ['id', 'his_crm_client_id']})
}
# print("client_ids")
# print(client_ids)
# print("property_map")
# print(property_map)
history_map = {
    str(r['crm_history_record']): r['id']
    for r in models.execute_kw(db, uid, password, 'custom.notes', 'search_read',
                               [[('crm_history_record', 'in', history_ids)]], {'fields': ['id', 'crm_history_record']})
}
# print("history_ids")
# print(history_ids)
# print("history_map")
# print(history_map)
ticket_map = {
    str(r['crm_ticket_id']): r['id']
    for r in models.execute_kw(db, uid, password, 'custom.notes', 'search_read',
                               [[('crm_ticket_id', 'in', ticket_ids)]], {'fields': ['id', 'crm_ticket_id']})
}
# print("ticket_ids")
# print(ticket_ids)

# print("ticket_map")
# print(ticket_map)

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

# Step 3: Build records for import
records_to_import = []
status_comments = {}
missing_users = {}  # initialize as a dictionary

# Loop through each row and store values into variables
for index, row in subset_df.iterrows():
    DocumentID = row['DocumentID']
    FileName = row['FileName']
    MIMEType = row['MIMEType']
    FileSize = row['FileSize']
    UploadedDate = row['UploadedDate']
    UploadedByID = row['UploadedByID']
    ClientID = row['ClientID']
    HistoryID = row['HistoryID']
    FolderID = row['FolderID']
    LastUpdatedDate = row['LastUpdatedDate']
    LinkAddr = row['LinkAddr']
    TypeMarriottID = row['TypeMarriottID']
    ShowOnCustomerPortal = row['ShowOnCustomerPortal']
    TicketID = row['TicketID']
    ContractID = row['ContractID']
    RFStepID = row['RFStepID']
    ExtraID = row['ExtraID']
    FileContent1 = row['FileContent1']
    QuestionaireQuestionID = row['QuestionaireQuestionID']
    MD5Hash = row['MD5Hash']

    hex_content = row['FileContent1']
    if isinstance(hex_content, str) and hex_content.startswith("0x"):
        # Convert hex string (remove 0x) to bytes
        binary_data = bytes.fromhex(hex_content[2:])
        # Encode to base64
        file_data = base64.b64encode(binary_data).decode('utf-8')

        print(f"Row {index + 1} file_data: {file_data[:60]}...")  # preview

    # Example: print row values
    # print(f"Row {index + 1}:")
    client_id = partner_map.get(ClientID, False)
    # print("client_id")
    # print(client_id)
    property_id = property_map.get(ClientID, False)
    # print("property_id")
    # print(property_id)
    attachment_id = False
    if client_id:
        attachment_id = models.execute_kw(
            db, uid, password,
            'ir.attachment', 'create',
            [{
                'name': FileName,
                'datas': file_data,
                'res_model': 'res.partner',
                'res_id': client_id,
            }]
        )
    else:
        print("Client not matched - %s" %ClientID)

    if not property_id:
        print("property not matched - %s" % ClientID)
    if not attachment_id and property_id:
        attachment_id = models.execute_kw(
            db, uid, password,
            'ir.attachment', 'create',
            [{
                'name': FileName,
                'datas': file_data,
                'res_model': 'site.site',
                'res_id': property_id,
            }]
        )
    # print("attachment_id")
    # print(attachment_id)

    # # Get multi.file.upload.type ID
    # type_records = models.execute_kw(
    #     db, uid, password,
    #     'multi.file.upload.type', 'search_read',
    #     [[('name', '=', survey_type_name)]],
    #     {'fields': ['id'], 'limit': 1}
    # )
    # type_id = type_records[0]['id'] if type_records else False

    if attachment_id:
        res_id = client_id
        res_model = 'res.partner'
        message = "Document Linked to Client - %s.  Document CRM Id - %s. Document Name - %s." %(client_id, DocumentID, FileName)

        if property_id:
            res_id = property_id
            res_model = 'site.site'
            message = "Document Linked to Property - %s. Document CRM Id - %s. Document Name - %s." % (property_id, DocumentID, FileName)

        document_id = models.execute_kw(
            db, uid, password,
            'documents.document', 'create',
            [{
                'name': FileName,
                'folder_id': folder_id,
                'owner_id': uid,
                'attachment_id': attachment_id,
                'res_model': res_model,
                'res_id': res_id,
                # 'site_survey_id': res_model,
                'property_id': property_id,
                'partner_id': client_id,
                # 'multi_upload_type_id': type_id
            }]
        )
    else:
        message = "No Document created. Document CRM Id - %s. Client CRM Id - %s. Property CRM Id - %s" % (DocumentID, ClientID, ClientID)

    print(message)
    print("-" * 50)





    # # Get multi.file.upload.type ID
    # type_records = models.execute_kw(
    #     db, uid, password,
    #     'multi.file.upload.type', 'search_read',
    #     [[('name', '=', survey_type_name)]],
    #     {'fields': ['id'], 'limit': 1}
    # )
    # type_id = type_records[0]['id'] if type_records else False
    #
    # # Create document in Documents
    # document_id = models.execute_kw(
    #     db, uid, password,
    #     'documents.document', 'create',
    #     [{
    #         'name': file_name,
    #         'folder_id': folder_id,
    #         'owner_id': uid,
    #         'attachment_id': attachment_id,
    #         'res_model': 'his.site.survey',
    #         'res_id': site_survey_id,
    #         'site_survey_id': site_survey_id,
    #         'property_id': property_id,
    #         'partner_id': partner_id,
    #         'multi_upload_type_id': type_id
    #     }]
    # )

