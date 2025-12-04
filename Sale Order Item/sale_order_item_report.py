import csv
import xmlrpc.client

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
    for i in range(6400, len(proposal_order), GROUP_BATCH_SIZE)
]

print(f"Total batches: {len(batches)}\n")

final_results = []

# ------------------------------------------------------
# SEND EACH BATCH TO ODOO
# ------------------------------------------------------
for batch_index, proposal_list in enumerate(batches, start=1):
    print(f"➡ Processing batch {batch_index} ({len(proposal_list)} proposals)")

    merged_records = []
    for proposal_id in proposal_list:
        # **ORDER-PRESERVED append**
        merged_records.extend(groups[proposal_id])

    batch_result = models.execute_kw(
        db, uid, password,
        'sale.order', 'import_proposal_catalog_items',
        [merged_records]
    )

    final_results.extend(batch_result)
    print(f"✔ Completed batch {batch_index} ({len(merged_records)} rows)\n")
