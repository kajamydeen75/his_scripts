import xmlrpc.client
import pandas as pd

# Odoo connection details

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

# File path
file_path = "quotes-and-sales-orders-all 12-1.csv"

# Read CSV
df = pd.read_csv(file_path)
# df = df.head(5)

# Conversion helper
def convert_value(val):
    if pd.isna(val):
        return ""  # Keep empty string if null
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d")  # Convert dates to string
    return str(val)

# Apply conversion
for col in df.columns:
    df[col] = df[col].map(convert_value)

# ------------------------------------------------------------
# BATCH PROCESSING STARTS HERE
# ------------------------------------------------------------
BATCH_SIZE = 100
total_rows = len(df)
final_results = []

print(f"Sale Order: {total_rows}")
print(f"Total rows: {total_rows}")
print(f"Processing in {BATCH_SIZE} record batches...\n")

for start in range(11100, total_rows, BATCH_SIZE):
    end = start + BATCH_SIZE
    batch_df = df.iloc[start:end]
    batch_records = batch_df.to_dict(orient="records")

    print(f"➡ Processing batch {start} → {start + len(batch_df)}")

    # try:
    batch_result = models.execute_kw(
        db, uid, password,
        'sale.order', 'import_bulk_sale_orders',
        [batch_records]
    )
    # except Exception as e:
    #     batch_result = [{'error': str(e)} for _ in batch_records]

    final_results.extend(batch_result)

    print(f"✔ Completed batch ({len(batch_df)} records)\n")

# ------------------------------------------------------------
# FINAL RESULTS
# ------------------------------------------------------------
print("FINAL RESULTS:")
print(final_results)
