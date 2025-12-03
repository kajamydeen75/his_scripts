import xmlrpc.client
import pandas as pd

# ------------------------------------------------------------
# ODOO CONNECTION DETAILS
# ------------------------------------------------------------
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
uid = common.authenticate(db, user, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', allow_none=True)

# ------------------------------------------------------------
# LOAD CSV
# ------------------------------------------------------------
file_path = "companies-all.csv"
df = pd.read_csv(file_path)

print("Columns:", df.columns.tolist())

# If filtering later:
# missing_df = df[df["Missed Items"].eq("Missing")]
missing_df = df.copy()

print("\nRecords to process:")
print(missing_df)

# ------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------
def convert_value(val):
    """Return safe string for Odoo."""
    if pd.isna(val):
        return ""
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d")
    return str(val)


def process_batch(batch_df):
    """Convert df batch → list of dicts → call Odoo."""
    records = batch_df.applymap(convert_value).to_dict(orient="records")

    try:
        return models.execute_kw(
            db,
            uid,
            password,
            'res.partner',
            'import_bulk_companies',
            [records]
        )
    except Exception as e:
        return [{"error": str(e)} for _ in records]


# ------------------------------------------------------------
# PROCESS IN BATCHES
# ------------------------------------------------------------
BATCH_SIZE = 100
total_rows = len(missing_df)
final_results = []

print(f"\nTotal rows to process: {total_rows}")
print(f"Processing in batches of {BATCH_SIZE}...\n")

for i in range(4550, total_rows, BATCH_SIZE):
    batch_df = missing_df.iloc[i:i + BATCH_SIZE]
    print(f"➡ Processing batch {i} to {i + len(batch_df)}")

    batch_result = process_batch(batch_df)
    final_results.extend(batch_result)

    print(f"✔ Batch completed ({len(batch_df)} records)\n")


# ------------------------------------------------------------
# UPDATE ORIGINAL CSV (ONLY FOR PROCESSED ROWS)
# ------------------------------------------------------------
df["Import Status"] = ""

for idx, msg in zip(missing_df.index, final_results):
    if isinstance(msg, dict) and msg.get("status"):
        df.at[idx, "Import Status"] = msg["status"].strip()

# ------------------------------------------------------------
# SAVE CSV
# ------------------------------------------------------------
df.to_csv(file_path, index=False)
print("🎉 Import completed — CSV updated successfully!")
