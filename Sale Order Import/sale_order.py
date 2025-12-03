import xmlrpc.client
import pandas as pd

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

# Connect to Odoo
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', allow_none=True)
uid = common.authenticate(db, user, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', allow_none=True)

# File path
file_path = "quotes-and-sales-orders-all-2.csv"

# Read CSV
df = pd.read_csv(file_path)
# df = df.head(5)

# Conversion helper
def convert_value(val):
    if pd.isna(val):
        return ""  # Keep empty string if null
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d")  # Convert dates to string
    return str(val)

# ✅ Apply conversion without deprecated `applymap`
for col in df.columns:
    df[col] = df[col].map(convert_value)

# Convert to list of dicts
records = df.to_dict(orient="records")

print(records, "✅ Conversion complete!")

try:
    result = models.execute_kw(
        db, uid, password,
        'sale.order', 'import_bulk_sale_orders',
        [records]
    )
except Exception as e:
    result = [{'error': str(e)} for _ in records]

print(result)
