import xmlrpc.client
import pandas as pd

# Odoo connection details

url = 'http://localhost:17175'
db = 'his_stage_01112025'
user = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'


# Odoo connection details
#
url = 'https://hotel-internet-services-stage-12503805.dev.odoo.com'
db = 'hotel-internet-services-stage-12503805'
user = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'

# XML-RPC clients with allow_none=True
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', allow_none=True)
uid = common.authenticate(db, user, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', allow_none=True)

# File path
file_path = "site-survey-11-06-2025 - top 50.csv"



# Load CSV file
df = pd.read_csv(file_path)
# df = df.head(2)
# Function to handle nulls and dates
def convert_value(val):
    if pd.isna(val):
        return ""   # Keep empty string if null
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d")  # Convert date to string
    return str(val)  # Convert everything else to string

# Apply conversion to every value
records = df.applymap(convert_value).to_dict(orient="records")

# Print first 2 rows as example
# print(records[:2])
records = records[:2]
try:
    result = models.execute_kw(
        db, uid, password,
        'site.survey', 'import_bulk_site_surveys',
        [records]
    )
except Exception as e:
    result = [{'error': str(e)} for _ in records]

print(result)


for idx, msg in enumerate(result):
    if msg and msg.get("status"):
        df.at[idx, "Import Status"] = msg["status"].strip()
    else:
        df.at[idx, "Import Status"] = ""


# Step 7: Save updated file
df.to_csv(file_path, index=False)
