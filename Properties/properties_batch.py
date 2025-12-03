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

#
# # Odoo connection details
# url = 'https://hotel-internet-services-stage-12503805.dev.odoo.com'
# db = 'hotel-internet-services-stage-12503805'
# username = 'kaja@blackbadger.biz'
# password = 'kaja@blackbadger.biz'
#
# # Production
url = 'https://touchstone1.odoo.com'
db = 'hotel-internet-services-live-10380387'
username = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'


common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', allow_none=True)
uid = common.authenticate(db, user, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', allow_none=True)

# File path
file_path = "properties-all-2.csv"
df = pd.read_csv(file_path)

# df = df.head(10)
# df = df.iloc[3400:4000]

print(df)

def convert_value(val):
    if pd.isna(val):
        return ""  # Keep empty string if null
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d")  # Convert dates to string
    return str(val)

def clean_value(val):
    """Remove invalid XML control characters."""
    if isinstance(val, str):
        val = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", val)
    return val

# Clean values
for col in df.columns:
    df[col] = df[col].map(convert_value)
    df[col] = df[col].map(clean_value)

# ------------------------------------------------------------
# BATCH PROCESSING (1000 records per batch)
# ------------------------------------------------------------
BATCH_SIZE = 100
total_rows = len(df)
final_results = []

print(f"Total rows: {total_rows}")
print(f"Processing in {BATCH_SIZE} record batches...\n")

for start in range(0, total_rows, BATCH_SIZE):
    end = start + BATCH_SIZE
    batch_df = df.iloc[start:end]
    batch_records = batch_df.to_dict(orient="records")

    print(f"➡ Processing batch {start} → {start + len(batch_df)}")

    batch_result = models.execute_kw(
        db, uid, password,
        'site.site', 'import_bulk_site_properties',
        [batch_records]
    )
    # except Exception as e:
    #     batch_result = [{'error': str(e)} for _ in batch_records]

    final_results.extend(batch_result)
    print(f"✔ Completed batch ({len(batch_df)} records)\n")

# ------------------------------------------------------------
# FINAL RESULTS
# ------------------------------------------------------------
print("FINAL RESULTS:")
print(final_results)
