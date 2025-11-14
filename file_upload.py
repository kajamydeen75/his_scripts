# -*- coding: utf-8 -*-
"""
Direct XML-RPC upload to Odoo — auto-generates a large fake file in memory.
No physical file needed on disk.
"""

import xmlrpc.client
import base64
import os

# ---------------------------------------------------------------------------
# CONFIGURATION
odoo_url = 'http://localhost:17175'
db = 'his_stage_01112025'
username = 'kaja@blackbadger.biz'
password = 'kaja@blackbadger.biz'

# odoo_url = 'https://hotel-internet-services-stage-12503805.dev.odoo.com'
# db = 'hotel-internet-services-stage-12503805'
# username = 'kaja@blackbadger.biz'
# password = 'kaja@blackbadger.biz'

file_size_mb = 100                         # Size of fake file (MB)
file_name = f"auto_generated_{file_size_mb}MB.bin"
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# AUTHENTICATION
# ---------------------------------------------------------------------------
print("Connecting to Odoo...")
common = xmlrpc.client.ServerProxy(f"{odoo_url}/xmlrpc/2/common")
uid = common.authenticate(db, username, password, {})

if not uid:
    raise Exception("❌ Authentication failed! Check credentials.")

models = xmlrpc.client.ServerProxy(f"{odoo_url}/xmlrpc/2/object")
print(f"✅ Authenticated as UID {uid}")

# ---------------------------------------------------------------------------
# GENERATE LARGE DATA IN MEMORY
# ---------------------------------------------------------------------------
print(f"Generating {file_size_mb} MB of fake binary data...")

# Create random-like content (repeated pattern for speed)
pattern = b"OD" * 512  # 1 KB pattern
repeat_count = int((file_size_mb * 1024 * 1024  ) / len(pattern))
fake_binary = pattern * repeat_count

print(f"Generated {len(fake_binary) / 1024 / 1024:.2f} MB in memory.")

# ---------------------------------------------------------------------------
# ENCODE AND UPLOAD
# ---------------------------------------------------------------------------
print("Encoding data to base64 (this may take a few seconds)...")
file_data_b64 = base64.b64encode(fake_binary).decode("utf-8")

print("Uploading via XML-RPC to Odoo...")
attachment_id = models.execute_kw(
    db,
    uid,
    password,
    "ir.attachment",
    "create",
    [
        {
            "name": file_name,
            "datas": file_data_b64,
            "type": "binary",
            "res_model": "ir.attachment",
            "res_id": 0,
            "mimetype": "application/octet-stream",
        }
    ],
)

print(f"✅ Upload complete! Attachment ID: {attachment_id}")
