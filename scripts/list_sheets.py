#!/usr/bin/env python3
"""
Quick script to list available Google Sheets to find the correct name.
"""

import gspread

def list_spreadsheets():
    """List all available spreadsheets."""
    try:
        gc = gspread.service_account(filename='keys/trr-backend-df2c438612e1.json')
        
        # List all spreadsheets
        spreadsheets = gc.list_spreadsheet_files()
        
        print("Available spreadsheets:")
        for i, sheet in enumerate(spreadsheets):
            print(f"{i+1}. {sheet['name']} (ID: {sheet['id']})")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    list_spreadsheets()
