import xmlrpc.client
import pandas as pd
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

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
#
# # Odoo connection details
# url = 'https://hotel-internet-services-stage-12503805.dev.odoo.com'
# db = 'hotel-internet-services-stage-12503805'
# username = 'kaja@blackbadger.biz'
# password = 'kaja@blackbadger.biz'
#
# # # Production
url = 'https://touchstone1.odoo.com'
db = 'hotel-internet-services-live-10380387'
username = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'


common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', allow_none=True)
uid = common.authenticate(db, user, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', allow_none=True)

# File path
file_path = "individuals-all-2.csv"
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

# Clean the DataFrame
for col in df.columns:
    df[col] = df[col].map(convert_value)
    df[col] = df[col].map(clean_value)

# ------------------------------------------------------------
# BATCH PROCESSING
# ------------------------------------------------------------
BATCH_SIZE = 100
MAX_THREADS = 5

total_rows = len(df)
final_results = []

print(f"Total rows: {total_rows}")
print(f"Processing in batches of {BATCH_SIZE}...\n")

# Build list of batches
batches = [
    df.iloc[i:i + BATCH_SIZE].to_dict(orient="records")
    for i in range(0, total_rows, BATCH_SIZE)
]


# ------------------------------------------------------------
# MULTITHREAD WORKER — NO TRY/EXCEPT
# ------------------------------------------------------------
def process_batch(batch_index, batch_records):

    print(f"➡ Thread-{batch_index}: Starting batch ({len(batch_records)} records)")

    # Each thread uses its own XML-RPC proxy
    models_thread = xmlrpc.client.ServerProxy(
        f"{url}/xmlrpc/2/object", allow_none=True
    )

    # ANY ERROR HERE WILL STOP THE WHOLE SCRIPT
    result = models_thread.execute_kw(
        db, uid, password,
        'res.partner', 'import_bulk_individuals',
        [batch_records]
    )

    print(f"✔ Thread-{batch_index}: Completed")
    return result


# ------------------------------------------------------------
# EXECUTE MULTITHREAD IMPORT
# ------------------------------------------------------------
with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    futures = {
        executor.submit(process_batch, idx, batch): idx
        for idx, batch in enumerate(batches)
    }

    for future in as_completed(futures):
        result = future.result()  # ERROR HERE WILL STOP EVERYTHING
        final_results.extend(result)

# ------------------------------------------------------------
# FINAL RESULT OUTPUT
# ------------------------------------------------------------
print("\n🎉 ALL BATCHES COMPLETED\n")
print("FINAL RESULTS:")
print(final_results)
