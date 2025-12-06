import xmlrpc.client
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

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


# Production
url = 'https://touchstone1.odoo.com'
db = 'hotel-internet-services-live-10380387'
user = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'

# XML-RPC clients with allow_none=True
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', allow_none=True)
uid = common.authenticate(db, user, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', allow_none=True)

# File path
file_path = "leads-all client-table.csv"

# Load CSV file
df = pd.read_csv(file_path)

# Function to handle nulls and dates
def convert_value(val):
    if pd.isna(val):
        return ""   # Keep empty string if null
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d")  # Convert date to string
    return str(val)  # Convert everything else to string

# Apply conversion
for col in df.columns:
    df[col] = df[col].map(convert_value)

# ------------------------------------------------------------
# BATCH PREPARATION
# ------------------------------------------------------------
BATCH_SIZE = 100
MAX_THREADS = 5   # run 5 batches simultaneously

total_rows = len(df)
final_results = []

print(f"Total rows: {total_rows}")
print(f"Processing in batches of {BATCH_SIZE}...\n")

# Prepare batches
batches = [
    df.iloc[i:i + BATCH_SIZE].to_dict(orient="records")
    for i in range(10, total_rows, BATCH_SIZE)
]


# ------------------------------------------------------------
# THREAD WORKER (NO try/except)
# ------------------------------------------------------------
def process_batch(batch_index, batch_records):
    print(f"➡ Thread-{batch_index}: Processing {len(batch_records)} records")

    # each thread creates its own XML-RPC model client
    models_thread = xmlrpc.client.ServerProxy(
        f'{url}/xmlrpc/2/object', allow_none=True
    )

    # NO TRY/EXCEPT → errors will propagate and stop the script
    result = models_thread.execute_kw(
        db, uid, password,
        'crm.lead', 'import_bulk_leads_client',
        [batch_records]
    )

    print(f"✔ Thread-{batch_index}: Completed")
    return result


# ------------------------------------------------------------
# EXECUTE THREADS
# ------------------------------------------------------------
with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    futures = {
        executor.submit(process_batch, idx, batch): idx
        for idx, batch in enumerate(batches)
    }

    for future in as_completed(futures):
        output = future.result()      # if any exception occurs → script stops
        final_results.extend(output)


# ------------------------------------------------------------
# FINAL RESULTS
# ------------------------------------------------------------
print("\n🎉 ALL BATCHES COMPLETED\n")
print(final_results)

#
# # ------------------------------------------------------------
# # UPDATE Import Status COLUMN
# # ------------------------------------------------------------
# if 'Import Status' not in df.columns:
#     df['Import Status'] = ""
#
# for idx, msg in enumerate(final_results):
#     if msg and isinstance(msg, dict) and msg.get("status"):
#         df.at[idx, "Import Status"] = msg["status"].strip()
#     else:
#         df.at[idx, "Import Status"] = ""
#
# # ------------------------------------------------------------
# # SAVE UPDATED CSV
# # ------------------------------------------------------------
# df.to_csv(file_path, index=False)
# print("🎉 Done! File updated and saved.")
