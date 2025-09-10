#!/usr/bin/env python3

import os
import gspread
from google.oauth2.service_account import Credentials

def fix_wwhl_headers():
    """Fix the WWHLinfo sheet headers to include the missing columns"""
    
    # Google Sheets setup  
    key_file = '/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json'
    creds = Credentials.from_service_account_file(key_file, scopes=[
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ])
    gc = gspread.authorize(creds)
    
    # Open the spreadsheet by name
    workbook = gc.open("Realitease2025Data")
    
    # Get WWHLinfo sheet
    try:
        wwhl_worksheet = workbook.worksheet("WWHLinfo")
        print("✅ Found WWHLinfo sheet")
    except gspread.exceptions.WorksheetNotFound:
        print("❌ WWHLinfo sheet not found")
        return False
    
    # Check current headers
    try:
        current_headers = wwhl_worksheet.row_values(1)
        print(f"📊 Current headers: {current_headers}")
        
        # The correct 7-column headers
        correct_headers = [
            "CastName",                        # Column A 
            "Cast IMDbID",                     # Column B
            "Cast TMDbID",                     # Column C - MISSING
            "Episode Marker",                  # Column D
            "OtherGuestNames",                 # Column E - MISSING
            "IMDb CastIDs of Other Guests",    # Column F
            "TMDb CastIDs of Other Guests"     # Column G
        ]
        
        print(f"🎯 Correct headers: {correct_headers}")
        
        # Update headers
        wwhl_worksheet.update('A1:G1', [correct_headers])
        print("✅ Successfully updated WWHLinfo headers to 7 columns!")
        
        # Verify the update
        updated_headers = wwhl_worksheet.row_values(1)
        print(f"✅ Verified headers: {updated_headers}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error updating headers: {e}")
        return False

if __name__ == "__main__":
    print("🔧 Fixing WWHLinfo sheet headers...")
    success = fix_wwhl_headers()
    if success:
        print("🎉 Headers fixed successfully!")
    else:
        print("❌ Failed to fix headers")
