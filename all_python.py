# -----------------------------------------
# runner.py
# Run python files inside subfolders
# -----------------------------------------

import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))


def run_module_in_folder(folder, module):
    folder_path = os.path.join(ROOT, folder)
    sys.path.append(folder_path)

    # Save current working directory
    old_cwd = os.getcwd()

    try:
        # Change to the folder so batch file relative paths work
        os.chdir(folder_path)

        # Import the module
        __import__(module)

    finally:
        # Restore original working directory
        os.chdir(old_cwd)


# -----------------------------
# RUN ALL SCRIPTS
# -----------------------------

# run_module_in_folder("Company Import", "company_import_batch")
# print("Company Import Completed")

# run_module_in_folder("Addresses", "addresses_batch")
# print("Addresses Import Completed")

run_module_in_folder("Properties", "properties_batch")
print("Properties Import Completed")

# run_module_in_folder("Individuals", "individual_batch")
# print("Individuals Import Completed")


# run_module_in_folder("Opportunity Import", "opportunity_import")
# print("Opportunity Import Completed")

run_module_in_folder("Site Survey Import", "sale_order_batch")
print("Sale Order Import Completed")


# run_module_in_folder("Lead Imports", "leads_client")
# print("Lead Client Import Completed")

# run_module_in_folder("Lead Imports", "leads_person")
# print("Lead Person Import Completed")

# run_module_in_folder("Lead Imports", "leads_services_interested")
# print("Lead Services Interested Import Completed")



run_module_in_folder("Sale Order Import", "sale_order_batch")
print("Sale Order Import Completed")

run_module_in_folder("Sale Order Item", "sale_order_item_report")
print("Sale Order  Item Import Completed")

# run_module_in_folder("Company Import", "company_import_batch")
# print("Company Import Completed")






# run_module_in_folder("Import History Notes", "history_notes_batch")
# print("Lead Client Import Completed")


# run_module_in_folder("Lead Imports", "leads_history_record")
# print("Lead Client Import Completed")

print("\nAll scripts completed!")
