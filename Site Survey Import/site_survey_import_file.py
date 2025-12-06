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

# url = 'https://touchstone1.odoo.com'
# db = 'hotel-internet-services-live-10380387'
# username = 'kaja@blackbadger.biz'
# password = 'kaja@blackbadger.biz'

# XML-RPC clients with allow_none=True
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', allow_none=True)
uid = common.authenticate(db, user, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', allow_none=True)

# File path
file_path = "site-surveys-all.csv"

# Load CSV file
df = pd.read_csv(file_path)

# Function to handle nulls and dates
def convert_value(val):
    if pd.isna(val):
        return ""   # Keep empty string if null
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d")  # Convert date to string
    return str(val)

def clean_value(val):
    """Remove invalid XML control characters."""
    if isinstance(val, str):
        val = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", val)
    return val

# Apply cleaning & preprocessing
for col in df.columns:
    df[col] = df[col].map(convert_value)
    df[col] = df[col].map(clean_value)

# ------------------------------------------------------------
# BATCH PROCESSING (1000 RECORDS PER BATCH)
# ------------------------------------------------------------
BATCH_SIZE = 100
total_rows = len(df)
final_results = []

print(f"Total rows: {total_rows}")
print(f"Processing in batches of {BATCH_SIZE}...\n")

for start in range(0, total_rows, BATCH_SIZE):
    end = start + BATCH_SIZE
    batch_df = df.iloc[start:end]

    batch_records = batch_df.to_dict(orient="records")

    print(f"➡ Processing batch {start} → {start + len(batch_df)}")

    batch_result = models.execute_kw(
        db, uid, password,
        'site.survey', 'import_bulk_site_surveys',
        [batch_records]
    )


    final_results.extend(batch_result)

    print(f"✔ Completed batch ({len(batch_df)} records)\n")

# ------------------------------------------------------------
# (Optional) Update Import Status in DataFrame
# Uncomment if needed
# ------------------------------------------------------------
# if "Import Status" not in df.columns:
#     df["Import Status"] = ""
#
# for idx, msg in enumerate(final_results):
#     if isinstance(msg, dict) and msg.get("status"):
#         df.at[idx, "Import Status"] = msg["status"].strip()
#     elif isinstance(msg, dict) and msg.get("error"):
#         df.at[idx, "Import Status"] = "Error: " + msg["error"]
#     else:
#         df.at[idx, "Import Status"] = ""
#
# df.to_csv(file_path, index=False)
# print("CSV updated with Import Status")

print("🎉 Import completed successfully!")
