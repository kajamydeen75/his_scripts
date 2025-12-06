import xmlrpc.client
import pandas as pd
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
# ------------------------------------------------------
# Odoo connection
# ------------------------------------------------------
# url = 'http://localhost:17175'
# db = 'his_stage_01112025'
# user = 'kaja@blackbadger.biz'
# password = 'kaja@blackbadger.biz'

# Odoo connection details
#
# url = 'https://hotel-internet-services-stage-12503805.dev.odoo.com'
# db = 'hotel-internet-services-stage-12503805'
# username = 'kaja@blackbadger.biz'
# password = 'kaja@blackbadger.biz'
# ------------------------------------------------------------
# Odoo connection
# ------------------------------------------------------------
url = 'https://touchstone1.odoo.com'
db = 'hotel-internet-services-live-10380387'
username = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'

common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', allow_none=True)
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', allow_none=True)

# ------------------------------------------------------------
# Read CSV
# ------------------------------------------------------------
file_path = "properties-all-2.csv"
df = pd.read_csv(file_path)

def convert_value(val):
    if pd.isna(val):
        return ""
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d")
    return str(val)

def clean_value(val):
    if isinstance(val, str):
        return re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", val)
    return val

# Clean values
for col in df.columns:
    df[col] = df[col].map(convert_value).map(clean_value)

total_rows = len(df)
print(f"Total rows: {total_rows}")

# ------------------------------------------------------------
# Batch preparation
# ------------------------------------------------------------
BATCH_SIZE = 100
MAX_THREADS = 5
RETRY_COUNT = 3

batches = [
    df.iloc[i:i + BATCH_SIZE].to_dict(orient="records")
    for i in range(2000, total_rows, BATCH_SIZE)
]

print(f"Total batches: {len(batches)}\n")


# ------------------------------------------------------------
# Worker function for each thread WITH RETRY
# ------------------------------------------------------------
def process_batch(batch_index, batch_records):

    print(f"➡ Thread-{batch_index}: Starting batch ({len(batch_records)} records)")
    attempt = 1

    while attempt <= RETRY_COUNT:
        try:
            # Each thread uses its own models proxy
            models_thread = xmlrpc.client.ServerProxy(
                f"{url}/xmlrpc/2/object", allow_none=True
            )

            result = models_thread.execute_kw(
                db, uid, password,
                "site.site", "import_bulk_site_properties",
                [batch_records]
            )

            print(f"✔ Thread-{batch_index}: Completed on attempt {attempt}")
            return result

        except Exception as e:
            print(f"❌ Thread-{batch_index}: Attempt {attempt} failed → {e}")

            if attempt == RETRY_COUNT:
                print(f"❌ Thread-{batch_index}: Giving up after {RETRY_COUNT} attempts.")
                return [{"error": str(e)}]

            time.sleep(3)
            attempt += 1


# ------------------------------------------------------------
# MULTITHREADED EXECUTION
# ------------------------------------------------------------
final_results = {}

with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    futures = {
        executor.submit(process_batch, idx, batch): idx
        for idx, batch in enumerate(batches)
    }

    for future in as_completed(futures):
        idx = futures[future]
        final_results[idx] = future.result()


# ------------------------------------------------------------
# FINAL RESULTS
# ------------------------------------------------------------
print("\n🎉 ALL BATCHES COMPLETE\n")
print(final_results)
