import xmlrpc.client
import pandas as pd
import psycopg2

url = 'http://localhost:1717'
odoo_database = 'his_20_04_2025'
odoo_user = 'admin'
odoo_password = 'admin'

common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
uid = common.authenticate(odoo_database, odoo_user, odoo_password, {})
model = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')

file_path = "C:/Users/Bhavi/Downloads/2.xlsx"
download_path = "C:/Users/Bhavi/Downloads"
df = pd.read_excel(file_path)

conn = psycopg2.connect(
    dbname='his_20_04_2025',
    user='odoo17',
    password='odoo17',
    host='localhost',
    port='5432'

)

comments = []  # This will store row-by-row messages

cursor = conn.cursor()

for index, row in df.iterrows():
    history_record_id = str(row['CRM History Record ID'])
    crm_client_id = str(row['CRM Client ID'])
    creator_crm_id = str(row['Creator CRM ID'])
    summary = str(row['Summary'])
    details = str(row['Details'])
    type_str = str(row['Type'])
    email_to_text = str(row['Email to Text'])
    create_date = str(row['Date Entered'])
    crm_contact_id = str(row['CRM Contact ID'])

    history_ids = model.execute_kw(odoo_database, uid, odoo_password, 'custom.notes', 'search',
                                   [[('crm_history_record', '=', history_record_id)]])
    type_ids = model.execute_kw(odoo_database, uid, odoo_password, 'type.of.note', 'search',
                                [[('name', 'ilike', type_str)]])
    user_ids = model.execute_kw(odoo_database, uid, odoo_password, 'res.users', 'search',
                                [[('crm_client_id', '=', creator_crm_id)]])
    partners_ids = model.execute_kw(odoo_database, uid, odoo_password, 'res.partner', 'search',
                                    [[('crm_client_id', '=', crm_contact_id)]])
    property_ids = model.execute_kw(odoo_database, uid, odoo_password, 'site.site', 'search',
                                    [[('his_crm_client_id', '=', crm_client_id)]])

    comment = ""

    if not type_ids:
        comment += "Type not found. \n"
    if not partners_ids:
        comment += "Company not found. \n"
    if not user_ids:
        comment += "User not found. \n"
    if not property_ids:
        comment += "Property not found. \n"

    type_id = type_ids[0] if type_ids else None
    partner_id = partners_ids[0] if partners_ids else None
    user_id = user_ids[0] if user_ids else None
    property_id = property_ids[0] if user_ids else None

    if history_ids:
        query_custom_notes = """
            UPDATE custom_notes
            SET
            name = %s,
            description = %s,
            email_to_text = %s,
            partner_id = %s,
            type_of_note_id = %s,
            site_id = %s,
            create_date = %s,
            create_uid = %s
            WHERE crm_history_record = %s
        """

        cursor.execute(query_custom_notes,
                       (summary, details, email_to_text, partner_id, type_id, property_id, create_date, user_id,
                        history_record_id))
    else:
        insert_query = """
            INSERT INTO custom_notes
            (crm_history_record, name, description, email_to_text, partner_id, type_of_note_id, site_id, create_date, create_uid)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query,
                       (history_record_id, summary, details, email_to_text, partner_id, type_id, property_id,
                        create_date, user_id))

    conn.commit()
    comments.append(comment)

# Clean up trailing newlines and make sure Excel-friendly line breaks are used
df["Import Status"] = [c.strip().replace("\n", "\r\n") for c in comments]

# Write to Excel
df.to_excel(download_path + "/History_record_result.xlsx", index=False)
