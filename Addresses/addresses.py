import xmlrpc.client
import pandas as pd
import re

# Odoo connection details

url = 'http://localhost:1717'
db = 'his_09112025'
user = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'

# Odoo connection details
url = 'http://localhost:17175'
db = 'his_stage_01112025'
username = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'


# Odoo connection details
url = 'https://hotel-internet-services-stage-12503805.dev.odoo.com'
db = 'hotel-internet-services-stage-12503805'
username = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'

# Production
url = 'https://touchstone1.odoo.com'
db = 'hotel-internet-services-live-10380387'
username = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'


common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', allow_none=True)
uid = common.authenticate(db, user, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', allow_none=True)

# File path
file_path = "Billing and Shipping Addresses.csv"
df = pd.read_csv(file_path)
# df = df.head(1000)
# df = df.iloc[2999:4000]

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

records = df.to_dict(orient="records")

try:
    result = models.execute_kw(
        db, uid, password,
        'res.partner', 'import_bulk_addresses',
        [records]
    )
except Exception as e:
    result = [{'error': str(e)} for _ in records]

print(result)
