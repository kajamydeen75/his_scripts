import xmlrpc.client
import pandas as pd

# Odoo connection details

url = 'http://localhost:1717'
db = 'his_09112025'
user = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'


common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', allow_none=True)
uid = common.authenticate(db, user, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', allow_none=True)

# File path
file_path = "properties.csv"
df = pd.read_csv(file_path)
df = df.head(3)

def convert_value(val):
    if pd.isna(val):
        return ""  # Keep empty string if null
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d")  # Convert dates to string
    return str(val)

for col in df.columns:
    df[col] = df[col].map(convert_value)

records = df.to_dict(orient="records")


try:
    result = models.execute_kw(
        db, uid, password,
        'site.site', 'import_bulk_site_properties',
        [records]
    )
except Exception as e:
    result = [{'error': str(e)} for _ in records]

print(result)
