import xmlrpc.client
import pandas as pd

# Odoo connection details

url = 'http://localhost:17175'
db = 'his_stage_01112025'
user = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'



#
# # Odoo connection details
# #
# url = 'https://hotel-internet-services-stage-12503805.dev.odoo.com'
# db = 'hotel-internet-services-stage-12503805'
# user = 'kaja@blackbadger.biz'
# password = 'kaja@blackbadger.biz'
#
url = 'https://touchstone1.odoo.com'
db = 'hotel-internet-services-live-10380387'
username = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'

# XML-RPC
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', allow_none=True)
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', allow_none=True)

# ------------------------------------------------------------
# FILE PATH
# ------------------------------------------------------------
file_path = "opportunity-all 12-1.csv"

# Load CSV
df = pd.read_csv(file_path)

# ------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------
def convert_value(val):
    """Convert all fields into safe string values."""
    if pd.isna(val):
        return ""
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d")
    return str(val)


def process_batch(batch_df):
    """Convert DataFrame batch to dict and call Odoo import method."""
    batch_records = batch_df.applymap(convert_value).to_dict(orient="records")

    return models.execute_kw(
        db, uid, password,
        'crm.lead', 'import_bulk_opportunity',
        [batch_records]
    )


# ------------------------------------------------------------
# PROCESS IN BATCHES
# ------------------------------------------------------------
BATCH_SIZE = 100
total_rows = len(df)
batches = (total_rows // BATCH_SIZE) + 1

print(f"Total rows: {total_rows}")
print(f"Processing in {batches} batches of {BATCH_SIZE} records...\n")

final_results = []

for i in range(0, total_rows, BATCH_SIZE):
    batch_df = df.iloc[i:i + BATCH_SIZE]
    print(f"➡ Processing batch {i} to {i + len(batch_df)}")

    batch_result = process_batch(batch_df)
    final_results.extend(batch_result)

    print(f"✔ Completed batch ({len(batch_df)} records)\n")


# ------------------------------------------------------------
# SAVE RESULTS BACK TO CSV
# ------------------------------------------------------------
df["Import Status"] = ""

for idx, msg in enumerate(final_results):
    if msg and msg.get("status"):
        df.at[idx, "Import Status"] = msg["status"].strip()

df.to_csv(file_path, index=False)
print("🎉 Done! Import completed and CSV updated.")
