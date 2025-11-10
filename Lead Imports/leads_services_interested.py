import xmlrpc.client
import pandas as pd

# Odoo connection details
url = 'http://localhost:17175'
db = 'his_stage_20092025'
user = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'


# Odoo connection details
url = 'https://hotel-internet-services-stage-12503805.dev.odoo.com'
db = 'hotel-internet-services-stage-12503805'
user = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'

# XML-RPC clients
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', allow_none=True)
uid = common.authenticate(db, user, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', allow_none=True)

# File path
file_path = "leads-services-interested 9-22-25.xlsx"

# Load Excel
df = pd.read_excel(file_path)

# Function to handle nulls and dates
def convert_value(val):
    if pd.isna(val):
        return False
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d")
    return str(val).strip()

# Convert DataFrame → dict list
records = df.applymap(convert_value).to_dict(orient="records")

# Call Odoo
try:
    result = models.execute_kw(
        db, uid, password,
        'crm.lead', 'import_bulk_leads_services_interested',
        [records]
    )
except Exception as e:
    print(f"Error during import: {e}")
    result = [{"error": str(e)}]

# Update Import Status
for idx in range(len(df)):
    status = ""
    if idx < len(result) and result[idx].get("status"):
        status = result[idx]["status"].strip()
    elif idx < len(result) and result[idx].get("error"):
        status = f"Error: {result[idx]['error']}"
    df.at[idx, "Import Status"] = status

# Save updated Excel
output_file = "leads-services-interested-imported.xlsx"
df.to_excel(output_file, index=False)

print(f"Updated file saved as: {output_file}")
