import xmlrpc.client
import pandas as pd


# Odoo connection details
url = 'https://hotel-internet-services-stage-12503805.dev.odoo.com'
db = 'hotel-internet-services-stage-12503805'
user = 'kaja@blackbadger.biz' # Username Odoo, please change
password = 'kaja@blackbadger.biz323232'  # Password Odoo,  please change

# XML-RPC clients with allow_none=True
common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common', allow_none=True)
uid = common.authenticate(db, user, password, {})
models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object', allow_none=True)

# Step 1: Get the site IDs
property_crm_ids = ['0A5E2666-90E1-425C-8AA5-E7CC07FDDCC8']

site_data = models.execute_kw(
    db, uid, password,
    'site.site', 'search_read',
    [[('his_crm_client_id', 'in', property_crm_ids)]],
    {'fields': ['id', 'name']}
)
site_ids = [site['id'] for site in site_data]

# Step 2: Get contact type IDs
type_name = 'Outage POC'
type_data = models.execute_kw(
    db, uid, password,
    'res.contact.type', 'search_read',
    [[('name', '=', type_name)]],
    {'fields': ['id', 'name']}
)
type_ids = [t['id'] for t in type_data]

# Step 3: Use the domain to search site.res.partner
domain = [
    ('site_id', 'in', site_ids),
    ('contact_type_ids', 'in', type_ids),  # assumes this structure
]

fields = ['partner_id','parent_id', 'site_id']

result = models.execute_kw(
    db, uid, password,
    'site.res.partner', 'search_read',
    [domain],
    {'fields': fields}
)

# Print results
for rec in result:
    print(rec)