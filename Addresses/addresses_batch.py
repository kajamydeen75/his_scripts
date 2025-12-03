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

# ------------------------------------------------------------
# DO NOT MODIFY USERNAME/PASSWORD/URL LINES ABOVE
# ------------------------------------------------------------

common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', allow_none=True)
uid = common.authenticate(db, user, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', allow_none=True)

# ------------------------------------------------------------
# LOAD CSV
# ------------------------------------------------------------
file_path = "Billing and Shipping Addresses.csv"
df = pd.read_csv(file_path)

# ------------------------------------------------------------
# CLEANING FUNCTIONS
# ------------------------------------------------------------
def convert_value(val):
    if pd.isna(val):
        return ""
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d")
    return str(val)

def clean_value(val):
    """Remove invalid XML control characters."""
    if isinstance(val, str):
        val = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", val)
    return val

for col in df.columns:
    df[col] = df[col].map(convert_value)
    df[col] = df[col].map(clean_value)

# ------------------------------------------------------------
# BATCH PROCESSING
# ------------------------------------------------------------
BATCH_SIZE = 1000
total_rows = len(df)
final_results = []

print(f"Total rows: {total_rows}")
print(f"Processing in batches of {BATCH_SIZE}...\n")

for i in range(0, total_rows, BATCH_SIZE):
    batch_df = df.iloc[i:i + BATCH_SIZE]
    batch_records = batch_df.to_dict(orient="records")

    print(f"➡ Processing batch: {i} to {i + len(batch_df)}")

    try:
        batch_result = models.execute_kw(
            db, uid, password,
            'res.partner', 'import_bulk_addresses',
            [batch_records]
        )
    except Exception as e:
        batch_result = [{'error': str(e)} for _ in batch_records]

    final_results.extend(batch_result)
    print(f"✔ Completed batch ({len(batch_df)} records)\n")

# ------------------------------------------------------------
# PRINT ALL RESULTS
# ------------------------------------------------------------
print("FINAL RESULT:")
print(final_results)
