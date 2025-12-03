import xmlrpc.client
import pandas as pd

# Odoo connection details

url = 'http://localhost:17175'
db = 'his_stage_01112025'
user = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'


# Odoo connection details

# url = 'https://hotel-internet-services-stage-12503805.dev.odoo.com'
# db = 'hotel-internet-services-stage-12503805'
# user = 'kaja@blackbadger.biz'
# password = 'kaja@blackbadger.biz'

# Production
url = 'https://touchstone1.odoo.com'
db = 'hotel-internet-services-live-10380387'
username = 'kaja@blackbadger.biz'
password = 'd553aaf3bb2f31d22601508ca7f7745d55f5d71e'


# XML-RPC clients with allow_none=True
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', allow_none=True)
uid = common.authenticate(db, user, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', allow_none=True)

# File path
file_path = "leads-all history-records.csv"

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
for col in df.columns:
    df[col] = df[col].map(convert_value)

# ------------------------------------------------------------
# BATCH PROCESSING (1000 per batch)
# ------------------------------------------------------------
BATCH_SIZE = 100
total_rows = len(df)
final_results = []

print(f"Total rows: {total_rows}")
print(f"Processing batches of {BATCH_SIZE}...\n")

for start in range(10900, total_rows, BATCH_SIZE):
    end = start + BATCH_SIZE
    batch_df = df.iloc[start:end]

    batch_records = batch_df.to_dict(orient="records")

    print(f"➡ Processing batch {start} → {start + len(batch_df)}")

    batch_result = models.execute_kw(
        db, uid, password,
        'custom.notes', 'import_bulk_notes_leads',
        [batch_records]
    )


    final_results.extend(batch_result)

    print(f"✔ Completed batch ({len(batch_df)} records)\n")

# ------------------------------------------------------------
# UPDATE Import Status COLUMN
# ------------------------------------------------------------
if "Import Status" not in df.columns:
    df["Import Status"] = ""

for idx, msg in enumerate(final_results):
    if msg and isinstance(msg, dict) and msg.get("status"):
        df.at[idx, "Import Status"] = msg["status"].strip()
    elif msg and msg.get("error"):
        df.at[idx, "Import Status"] = f"Error: {msg.get('error')}"
    else:
        df.at[idx, "Import Status"] = ""

# ------------------------------------------------------------
# SAVE updated CSV
# ------------------------------------------------------------
df.to_csv(file_path, index=False)

print("🎉 Import completed and file saved successfully!")
