import csv
import xmlrpc.client
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# ------------------------------------------------------
# Odoo connection
# ------------------------------------------------------
url = 'http://localhost:17175'
db = 'his_stage_01112025'
username = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'

# Odoo connection details
#
# url = 'https://hotel-internet-services-stage-12503805.dev.odoo.com'
# db = 'hotel-internet-services-stage-12503805'
# username = 'kaja@blackbadger.biz'
# password = 'kaja@blackbadger.biz'

# # Production
url = 'https://touchstone1.odoo.com'
db = 'hotel-internet-services-live-10380387'
username = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'

common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', allow_none=True)
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', allow_none=True)


# ------------------------------------------------------
# READ CSV WITHOUT PANDAS (PRESERVE ROW ORDER EXACTLY)
# ------------------------------------------------------
file_path = "quotes-line-items-all 12-1 R2 (1).csv"

records = []
with open(file_path, newline='', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    for row in reader:
        # Convert None → ""
        clean_row = {k: (v or "").strip() for k, v in row.items()}
        records.append(clean_row)

print(f"Total rows read: {len(records)}")


# ------------------------------------------------------
# MANUAL GROUP BY (ORDER PRESERVED)
# ------------------------------------------------------
GROUP_FIELD = "CRM Proposal ID"

groups = {}         # {proposal: [records]}
proposal_order = [] # list in exact CSV row order

for row in records:
    key = row.get(GROUP_FIELD, "")
    if key not in groups:
        groups[key] = []
        proposal_order.append(key)
    groups[key].append(row)

print(f"Unique proposals: {len(proposal_order)}")


# ------------------------------------------------------
# BATCH PROCESSING (10 PROPOSALS PER BATCH)
# ------------------------------------------------------
GROUP_BATCH_SIZE = 100

batches = [
    proposal_order[i : i + GROUP_BATCH_SIZE]
    for i in range(0, len(proposal_order), GROUP_BATCH_SIZE)
]

print(f"Total batches: {len(batches)}\n")

final_results = []


# ------------------------------------------------------
# MULTITHREAD + RETRY FUNCTION
# ------------------------------------------------------
RETRY_COUNT = 3

def process_batch(batch_index, proposal_list):
    print(f"➡ Thread-{batch_index}: Processing batch ({len(proposal_list)} proposals)")

    merged_records = []
    for proposal_id in proposal_list:
        # **ORDER-PRESERVED append**
        merged_records.extend(groups[proposal_id])

    attempt = 1
    while attempt <= RETRY_COUNT:
        try:
            # each thread needs its own XML-RPC connection
            models_thread = xmlrpc.client.ServerProxy(
                f'{url}/xmlrpc/2/object', allow_none=True
            )

            result = models_thread.execute_kw(
                db, uid, password,
                'sale.order', 'import_proposal_catalog_items',
                [merged_records]
            )

            print(f"✔ Thread-{batch_index}: Completed (attempt {attempt})")
            return result

        except Exception as e:
            print(f"❌ Thread-{batch_index}: Attempt {attempt} failed → {e}")

            if attempt == RETRY_COUNT:
                print(f"❌ Thread-{batch_index}: Giving up after {RETRY_COUNT} attempts.")
                return [{"error": str(e)}]

            time.sleep(3)
            attempt += 1


# ------------------------------------------------------
# EXECUTE IN PARALLEL
# ------------------------------------------------------
MAX_THREADS = 7

with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    futures = {
        executor.submit(process_batch, idx + 1, batch): idx + 1
        for idx, batch in enumerate(batches)
    }

    for future in as_completed(futures):
        result = future.result()
        final_results.extend(result)


# ------------------------------------------------------
# FINAL RESULTS
# ------------------------------------------------------
print("\n🎉 ALL BATCHES COMPLETED")
print(final_results)
