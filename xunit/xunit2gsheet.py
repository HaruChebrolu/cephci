"""
Sheet Structure:
  The script maintains four worksheets in the Google Sheet:
    1. Properties - Metadata about test runs (build, version, etc.)
    2. Summary - Aggregated test suite results (pass/fail counts)
    3. Test Case Status - Individual test case statuses
    4. Test Case Timings - Individual test case execution times

Features:
  - Automatic column width adjustment based on content
  - Automatic component detection from suite names
  - Color-coded status columns (Passed/Failed/Error/Skipped)
  - Version-aware sorting of test results (newest first)
  - Smart merging of new results with existing data
  - Automatic spreadsheet renaming with build number

Note:
  - The script preserves existing data while adding new version columns
  - Test cases are matched by name and polarion-testcase-id when available
  - First run creates the worksheets if they don't exist

Service Account Setup:
  1. Create service account in Google Cloud Console
  2. Download JSON credentials file as 'service_account.json'
  3. Share your Google Sheet with the service account email
  4. Place JSON file in the same directory as this script

Reference:
  Google Sheets API documentation:
  https://developers.google.com/sheets/api/guides/concepts
"""

import os
import re
import sys
import xml.etree.ElementTree as ET

import gspread
import pandas as pd
from googleapiclient.discovery import build
from docopt import docopt
from google.oauth2.service_account import Credentials
from gspread_formatting import cellFormat, format_cell_range, TextFormat, set_frozen, format_cell_range

# --- Constants ---
SHEET_PROPERTIES = "Properties"
SHEET_SUMMARY = "Summary"
SHEET_TEST_STATUS = "Test Case Status"
SHEET_TEST_TIMINGS = "Test Case Timings"

PREFERRED_PROPERTIES = [
    "polarion-project-id",
    "polarion-testrun-id",
    "polarion-group-id",
    "build",
    "ceph-version",
    "suite-name",
    "distro",
    "container-tag",
    "run-id",
    "cloud-type",
    "invoked-by",
    "ceph-ansible-version",
    "conf-file",
    "container-registry",
    "container-image",
    "compose-id",
    "instance-name",
]

DOC = """
    xunit2gsheet.py - Upload xUnit test results to Google Sheets

    Usage:
        xunit2gsheet.py --gsheet <gsheet_id> --resultsdir <xml_folder_path> \
            --version <ceph_version>
        xunit2gsheet.py (-h | --help)

    Options:
        -h --help                        Show this help message
        --gsheet <gsheet_id>             Google Sheets ID to update
        --resultsdir <xml_folder_path>   Path to xUnit XML test results folder
        --version <ceph_version>         Ceph version

    Requirements:
        - A JSON file named 'service_account.json' must be present
        - Python packages: gspread, pandas, xml.etree.ElementTree, google-auth
    """

# ==== CONFIGURATION ====
SERVICE_ACCOUNT_FILE = "service_account.json"  # Path to your JSON key
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
from gspread_formatting import *


def get_gsheet_client(credentials_path):
    """
    Authenticates with Google Sheets API using service account credentials.
    Args:
        credentials_path (str): Path to service account JSON credentials file
    Returns:
        gspread.Client: Authenticated Google Sheets client
    """
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
    ]
    creds = Credentials.from_service_account_file(credentials_path, scopes=scopes)
    return gspread.authorize(creds)


def get_or_create_worksheet(spreadsheet, sheet_name):
    """
    Gets a worksheet by name or creates it if it doesn't exist.
    Args:
        spreadsheet (gspread.Spreadsheet): The parent spreadsheet
        sheet_name (str): Name of worksheet to get/create
    Returns:
        gspread.Worksheet: The requested worksheet
    """
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="20")
    return worksheet


def parse_xml_properties(xml_file_path):
    """
    Extracts all <property> name/value pairs from an XML file.
    Args:
        xml_file (str): Path to the xUnit XML file
    """
    properties = {}
    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
    except ET.ParseError as e:
        print(f"Error parsing XML file {xml_file_path}: {e}")
        return None

    # Find all <property> tags in the XML
    for prop in root.findall(".//property"):
        name = prop.get("name")
        value = prop.get("value")
        if name:
            properties[name.strip()] = value.strip() if value else ""
    return properties

def parse_testsuite_stats(xml_file):
    """Extract stats for each <testsuite> in the XML."""
    stats = []
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        for suite in root.findall(".//testsuite"):
            name = suite.get("name", "")
            total = int(suite.get("tests", 0))
            failures = int(suite.get("failures", 0))
            errors = int(suite.get("errors", 0))
            skipped = int(suite.get("skipped", 0))
            time_sec = float(suite.get("time", 0))
            passed_count = total - (failures + errors + skipped)
            success_rate_float = (passed_count / total * 100) if total > 0 else 0.0
            success_rate_str = f"{success_rate_float:.2f}%"

            stats.append({
                "Test Suite Name": name,
                "Total Tests": total,
                "Passed": passed_count,
                "Failures": failures,
                "Errors": errors,
                "Skipped": skipped,
                "Success Rate": success_rate_str,
                "Time (seconds)": time_sec,
            })
    except Exception as e:
        print(f"❌ Error parsing {xml_file}: {e}")

    return stats

def parse_testcase_details(xml_file):
    """Extract each test case name, its Polarion ID, and status per test suite."""
    testcases_data = []
    components = ["rgw", "rbd", "nfs", "cephfs", "rados", "nvmeof"]

    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        for suite in root.findall(".//testsuite"):
            suite_name = suite.get("name", "")

            # Determine component from testcase name
            for comp in components:
                if comp.lower() in suite_name.lower():
                    component = comp
                    break

            for case in suite.findall(".//testcase"):
                testcase_name = case.get("name", "")

                # Default values
                polarion_id = ""
                status = "Passed"

                # Get Polarion ID if exists
                prop = case.find(".//property[@name='polarion-testcase-id']")
                if prop is not None:
                    polarion_id = prop.get("value", "").strip()

                # Determine test status
                failure_tag = case.find("failure")
                if failure_tag is not None:
                    status = "Failed"

                testcases_data.append({
                    "Test Suite Name": suite_name,
                    "Component": component,
                    "Test Case Name": testcase_name,
                    "polarion-testcase-id": polarion_id,
                    "Status": status,
                })

    except Exception as e:
        print(f"❌ Error parsing testcases from {xml_file}: {e}")

    return testcases_data

def parse_testcase_timings(xml_file):
    """Extract each test case name, its Polarion ID, and status, execution time per test suite."""
    testcases_data = []
    components = ["rgw", "rbd", "nfs", "cephfs", "rados", "nvmeof"]

    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        for suite in root.findall(".//testsuite"):
            suite_name = suite.get("name", "")

            # Determine component from testcase name
            for comp in components:
                if comp.lower() in suite_name.lower():
                    component = comp
                    break

            for case in suite.findall(".//testcase"):
                testcase_name = case.get("name", "")

                # Get time taken (float)
                try:
                    time_taken = float(case.get("time", 0))
                except ValueError:
                    time_taken = 0.0
                testcases_data.append({
                    "Test Suite Name": suite_name,
                    "Component": component,
                    "Test Case Name": testcase_name,
                    "Time (seconds)": time_taken,
                })

    except Exception as e:
        print(f"❌ Error parsing testcases from {xml_file}: {e}")

    return testcases_data
def update_gsheet(sheet_name, data, columns_order=None):
    """Write extracted XML properties into Google Sheet."""
    try:
        client = get_gsheet_client(SERVICE_ACCOUNT_FILE)
        spreadsheet = client.open_by_key(gsheet_id)
        gsheet = get_or_create_worksheet(spreadsheet, sheet_name)
        
        # Determine header
        if columns_order:
            header = columns_order
        else:
            # Collect all unique property keys (columns)
            all_keys = set()
            for row in data:
                all_keys.update(row.keys())
            header = sorted(all_keys)

        # Prepare data rows
        rows = []
        for row_data in data:
            row = [row_data.get(k, "") for k in header]
            rows.append(row)

        # Clear and rewrite sheet
        gsheet.clear()
        gsheet.append_row(header)
        gsheet.append_rows(rows)

        # Make header row bold
        header_format = cellFormat(textFormat=TextFormat(bold=True))
        format_cell_range(gsheet, "1:1", header_format)
        
        # Auto-resize columns using Google Sheets API
        creds = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=SCOPES
        )
        service = build('sheets', 'v4', credentials=creds)
        sheet_id = gsheet.id  # sheet/tab ID

        request_body = {
            "requests": [
                {
                    "autoResizeDimensions": {
                        "dimensions": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": 0,
                            "endIndex": len(header)  # number of columns
                        }
                    }
                }
            ]
        }

        service.spreadsheets().batchUpdate(
            spreadsheetId=gsheet_id,
            body=request_body
        ).execute()

        print(f"✅ Google Sheet '{spreadsheet.title}' updated successfully with bold headers and auto-resized columns.")

        print(f"Google Sheet '{spreadsheet.title}' updated successfully.")
    except Exception as e:
        print(f"❌ Failed to update Google Sheet '{sheet_name}': {e}")


def update_dynamic_gsheet(sheet_name, data, ceph_version_arg, value_type="Time"):
    """
    Update Google Sheet with dynamic columns per CEPh version.
    - value_type: "Time" or "Status"
    - Adds a new column like 'Time (seconds)(19.2.0-190)' or 'Status (19.2.0-190)'
    - Preserves existing data and updates existing rows.
    """
    # Define formats for different statuses
    client = get_gsheet_client(SERVICE_ACCOUNT_FILE)
    spreadsheet = client.open_by_key(gsheet_id)
    ws = get_or_create_worksheet(spreadsheet, sheet_name)

    # Get current sheet data
    all_values = ws.get_all_values()
    if all_values:
        header = all_values[0]
        existing_rows = all_values[1:]
    else:
        header = ["Test Suite Name", "Test Case Name", "Component"]
        existing_rows = []

    # Remove source_file if exists
    if "source_file" in header:
        header.remove("source_file")

    # Determine dynamic column
    col_name = f"{value_type} ({ceph_version_arg})"
    if col_name not in header:
        header.append(col_name)

    # Update header in sheet
    ws.update(range_name="A1", values=[header])

    # Freeze header & bold
    set_frozen(ws, 1)
    fmt = cellFormat(textFormat=textFormat(bold=True))
    format_cell_range(ws, f"A1:{gspread.utils.rowcol_to_a1(1, len(header))}", fmt)

    # Map existing rows
    idx_suite = header.index("Test Suite Name")
    idx_case = header.index("Test Case Name")
    row_map = {}
    for i, row in enumerate(existing_rows):
        key = (row[idx_suite], row[idx_case])
        row_map[key] = row

    # Build updated rows
    updated_rows = []
    new_rows = []

    for entry in data:
        key = (entry["Test Suite Name"], entry["Test Case Name"])
        row = []
        for h_idx, h in enumerate(header):
            if h == col_name:
                value = entry.get(value_type if value_type == "Time" else "Status", "")
            elif h in entry:
                value = entry[h]
            else:
                value = ""
            row.append(value)

        if key in row_map:
            # Merge with existing row safely
            existing_row = row_map[key]
            merged_row = []
            for h_idx, h in enumerate(header):
                existing_val = existing_row[h_idx] if h_idx < len(existing_row) else ""
                merged_row.append(row[h_idx] if row[h_idx] != "" else existing_val)
            updated_rows.append((key, merged_row))
        else:
            new_rows.append(row)

    # Batch update existing rows
    for i, (key, merged_row) in enumerate(updated_rows):
        row_idx = i + 2  # 1-indexed + header
        ws.update(range_name=f"A{row_idx}", values=[merged_row])

    # Append new rows
    if new_rows:
        ws.append_rows(new_rows)

    # --- Conditional formatting for Status column ---
    if value_type == "Status":
        status_col_index = header.index(col_name)
        # Apply green for "Passed", red for others
        fmt_passed = cellFormat(backgroundColor=color(0.85, 0.92, 0.83)) # green
        fmt_failed = cellFormat(backgroundColor=color(0.96, 0.8, 0.8))  # red

        start_row = 2
        end_row = len(existing_rows) + len(new_rows) + 1
        if end_row >= start_row:
            # Green for Passed
            format_cell_range(
                ws,
                f"{gspread.utils.rowcol_to_a1(start_row, status_col_index+1)}:"
                f"{gspread.utils.rowcol_to_a1(end_row, status_col_index+1)}",
                cellFormat(backgroundColor=color(0.85, 0.92, 0.83))
            )
            for r_idx, row in enumerate(existing_rows + new_rows, start=start_row):
                cell_val = row[status_col_index]
                fmt = fmt_passed if cell_val == "Passed" else fmt_failed
                format_cell_range(ws,
                    f"{gspread.utils.rowcol_to_a1(r_idx, status_col_index+1)}",
                    fmt
                )

    print(f"✅ Google Sheet '{sheet_name}' updated successfully for {value_type} columns.")


if __name__ == "__main__":
    all_properties = []
    all_stats = []
    all_testcases = []
    all_testcase_execution_times = []
    args = docopt(DOC)
    
    # --- Configuration ---
    GOOGLE_CREDENTIALS_PATH = "service_account.json"
    if not os.path.exists(GOOGLE_CREDENTIALS_PATH):
        print(
            f"Error: Google credentials file not"
            f"found at '{GOOGLE_CREDENTIALS_PATH}'"
        )
        sys.exit(1)
    gsheet_id = args["--gsheet"]
    xml_folder_path = args["--resultsdir"]
    ceph_version_arg = args["--version"]
    
    if not os.path.isdir(xml_folder_path):
        print("❌ Invalid xml folder path.")
        exit(1)
    
    for filename in os.listdir(xml_folder_path):
        if filename.endswith(".xml"):
            xml_file_path = os.path.join(xml_folder_path, filename)
            print(
                f"Parsing {xml_file_path} for version"
                f"{ceph_version_arg} into GSheet ID {gsheet_id}"
            )
            
            # File properties
            props = parse_xml_properties(xml_file_path)
            props["source_file"] = filename  # track which file it came from
            all_properties.append(props)
            
            # Test suite stats
            stats = parse_testsuite_stats(xml_file_path)
            for s in stats:
                s["source_file"] = filename
            all_stats.extend(stats)
            
            # Test case details
            testcases_data = parse_testcase_details(xml_file_path)
            for t in testcases_data:
                t["source_file"] = filename
            all_testcases.extend(testcases_data)
            
            # Test case execution times
            timings_data = parse_testcase_timings(xml_file_path)
            for t in timings_data:
                t["source_file"] = filename
            all_testcase_execution_times.extend(timings_data)
    
    if all_properties and all_stats and all_testcases and all_testcase_execution_times:
        print(f"Updating Google Sheet ID {gsheet_id}...")
        update_gsheet(SHEET_PROPERTIES, all_properties)
        columns = ["Test Suite Name",
                   "Total Tests",
                   "Passed",
                   "Failures",
                   "Errors",
                   "Skipped",
                   "Success Rate",
                   f"Time (seconds)"
                   ]
        update_gsheet(SHEET_SUMMARY, all_stats, columns_order=columns)
        update_dynamic_gsheet(SHEET_TEST_STATUS, all_testcases, ceph_version_arg, value_type="Status")
        update_dynamic_gsheet(SHEET_TEST_TIMINGS, all_testcase_execution_times, ceph_version_arg, value_type="Time (seconds)")

    else:
        print("Error: Failed to process XML file, GSheet not updated.")
