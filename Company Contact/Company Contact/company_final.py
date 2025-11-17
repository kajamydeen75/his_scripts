import xmlrpc.client
import pandas as pd

# Odoo connection details
url = 'http://localhost:17175'
db = 'his_stage_02072025'
username = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'


# Odoo connection details
# url = 'https://hotel-internet-services-stage-12503805.dev.odoo.com'
# db = 'hotel-internet-services-stage-12503805'
# username = 'kaja@blackbadger.biz'
# password = 'kaja@blackbadger.biz'

# Connect to Odoo
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', allow_none=True)
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', allow_none=True)

# Load Excel
df = pd.read_csv('companies-not-prospects.csv')
if 'Status' not in df.columns:
    df['Status'] = ''
# subset_df = df.head(20)

subset_df = df.copy()

# Extract unique values
sales_reps = subset_df['Sales Rep'].dropna().unique().tolist()
companies = subset_df['Company'].dropna().unique().tolist()
states = subset_df['State'].dropna().unique().tolist()
countries = subset_df['Country'].dropna().unique().tolist()
tags = subset_df['Tags'].dropna().unique().tolist()

# Maps
user_map = {
    r['name']: r['id']
    for r in models.execute_kw(db, uid, password, 'res.users', 'search_read',
                               [[('name', 'in', sales_reps)]], {'fields': ['id', 'name']})
}
company_map = {
    r['name']: r['id']
    for r in models.execute_kw(db, uid, password, 'res.partner', 'search_read',
                               [[('name', 'in', companies), ('is_company', '=', True)]], {'fields': ['id', 'name']})
}
state_map = {
    r['code']: r['id']
    for r in models.execute_kw(db, uid, password, 'res.country.state', 'search_read',
                               [[('code', 'in', states)]], {'fields': ['id', 'code']})
}
country_map = {
    r['name']: r['id']
    for r in models.execute_kw(db, uid, password, 'res.country', 'search_read',
                               [[('name', 'in', countries)]], {'fields': ['id', 'name']})
}
tag_map = {
    r['name']: r['id']
    for r in models.execute_kw(db, uid, password, 'res.partner.category', 'search_read',
                               [[('name', 'in', tags)]], {'fields': ['id', 'name']})
}

# Prepare partner data
partners_to_import = []
for index, row in subset_df.iterrows():
    company_name = str(row.get('CRM Client Id', '')).strip()
    company_name = str(row.get('Name', '')).strip()
    email1 = str(row.get('Email', '')).strip()
    email2 = str(row.get('Email 2', '')).strip()
    phone = str(row.get('Phone', '')).strip()
    country = str(row.get('Country', '')).strip()
    state = str(row.get('State', '')).strip()
    tag = str(row.get('Tags', '')).strip()
    sales_rep = str(row.get('Sales Rep', '')).strip()
    address = str(row.get('Address', '')).strip()
    address2 = row.get('Address 2', '')
    print(address2)
    address2 = '' if pd.isna(address2) else str(address2).strip()
    print(address2)
    # stop
    city = str(row.get('City', '')).strip()
    zip_code = str(row.get('Zip', '')).strip()
    website = str(row.get('Website', '')).strip()
    comment = str(row.get('Closed?', '')).strip()
    title = str(row.get('Title ', '')).strip()

    create_date_raw = row.get('Created Date', '')
    try:
        parsed = pd.to_datetime(create_date_raw, errors='coerce')
        create_date = parsed.strftime('%Y-%m-%d %H:%M:%S') if not pd.isnull(parsed) else None
    except:
        create_date = None


    status_msgs = []

    company_id = company_map.get(company_name)
    # contact_domain = [
    #     ('parent_id', '=', company_id),
    #     ('firstname', '=', fname),
    #     ('lastname', '=', lname)
    # ]
    if company_id:
        status_msgs.append("Company Updated")
    else:
        status_msgs.append("Company Created")

    # existing_contact_ids = models.execute_kw(db, uid, password, 'res.partner', 'search', [contact_domain])
    #
    # if existing_contact_ids:
    #     status_msgs.append("Contact Updated")
    # else:
    #     status_msgs.append("Contact Created")
    #
    # df.at[index, 'Status'] = '\n'.join(msg + '.' for msg in status_msgs)

    partner_vals = {
        'company_name': company_name,
        'fname': fname,
        'lname': lname,
        'email': email1,
        'secondary_email': email2,
        'phone': phone,
        'country_id': country_map.get(country),
        'state_id': state_map.get(state),
        'tags': [tag_map[tag]] if tag in tag_map else [],
        'user_id': user_map.get(sales_rep),
        'street': address,
        'street2': address2,
        'city': city,
        'zip': zip_code,
        'website': website,
        'comment': comment,
        'title': title,
        'create_date': create_date,
    }

    partners_to_import.append(partner_vals)

# Call Odoo method
try:
    result = models.execute_kw(
        db, uid, password,
        'res.partner', 'import_bulk_partners',
        [partners_to_import]
    )
    print("Partners processed:", result)
except Exception as e:
    print("Failed to process partners:", str(e))

df.to_excel("Updated_Book1.xlsx", index=False)
