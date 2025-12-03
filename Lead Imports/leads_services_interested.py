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

# XML-RPC clients
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', allow_none=True)
uid = common.authenticate(db, user, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', allow_none=True)

# File path
file_path = "leads-all services-interested-in.csv"

# Load Excel
df = pd.read_csv(file_path)

# Function to handle nulls and dates
def convert_value(val):
    if pd.isna(val):
        return False
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d")
    return str(val).strip()

# Apply conversion
for col in df.columns:
    df[col] = df[col].map(convert_value)

# ------------------------------------------------------------
# BATCH PROCESSING (100 RECORDS)
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
        'crm.lead', 'import_bulk_leads_services_interested',
        [batch_records]
    )


    final_results.extend(batch_result)

    print(f"✔ Completed batch ({len(batch_df)} records)\n")

# # ------------------------------------------------------------
# # UPDATE Import Status
# # ------------------------------------------------------------
# df["Import Status"] = ""
#
# for idx in range(len(df)):
#     if idx < len(final_results) and final_results[idx].get("status"):
#         df.at[idx, "Import Status"] = final_results[idx]["status"].strip()
#     elif idx < len(final_results) and final_results[idx].get("error"):
#         df.at[idx, "Import Status"] = f"Error: {final_results[idx]['error']}"
#     else:
#         df.at[idx, "Import Status"] = ""
#
# # ------------------------------------------------------------
# # SAVE updated Excel
# # ------------------------------------------------------------
# output_file = "leads-services-interested-imported.xlsx"
# df.to_excel(output_file, index=False)
#
# print(f"Updated file saved as: {output_file}")
