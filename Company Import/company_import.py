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

# XML-RPC clients
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', allow_none=True)
uid = common.authenticate(db, user, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', allow_none=True)

# ------------------------------------------------------------
# Load CSV and Filter Missing Items Only
# ------------------------------------------------------------
file_path = "companies-all.csv"
df = pd.read_csv(file_path)

print("Columns:", df.columns.tolist())

# Filter rows where Missed Items = Missing
# missing_df = df[df["Missed Items"].eq("Missing")]
missing_df = df

print("\nMissing Records:")
print(missing_df)


# ------------------------------------------------------------
# Prepare Records for Odoo Import
# ------------------------------------------------------------
def convert_value(val):
    """Convert values to strings, handle nulls and dates."""
    if pd.isna(val):
        return ""
    if isinstance(val, pd.Timestamp):
        return val.strftime("%Y-%m-%d")
    return str(val)

records = missing_df.applymap(convert_value).to_dict(orient="records")

# ------------------------------------------------------------
# Call Odoo Import Method
# ------------------------------------------------------------
try:
    result = models.execute_kw(
        db,
        uid,
        password,
        'res.partner',
        'import_bulk_companies',
        [records]
    )
except Exception as e:
    print("Error:", e)
    result = [{'error': str(e)}]

print("\nOdoo Response:")
print(result)

# # ------------------------------------------------------------
# # Update Only Missing Rows in Original DataFrame
# # ------------------------------------------------------------
# df.loc[missing_df.index, "Import Status"] = [
#     r.get("status", "") if isinstance(r, dict) else "" for r in result
# ]
#
# # ------------------------------------------------------------
# # Save Updated CSV
# # ------------------------------------------------------------
# df.to_csv(file_path, index=False)
# print("\nUpdated CSV saved!")
