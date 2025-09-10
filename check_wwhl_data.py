#!/usr/bin/env python3
import gspread
from google.oauth2.service_account import Credentials

# Authenticate with Google Sheets
creds = Credentials.from_service_account_file('keys/trr-backend-df2c438612e1.json')
client = gspread.authorize(creds)
workbook = client.open('Realitease Backend Database')
worksheet = workbook.worksheet('WWHLinfo')

# Get first 10 rows to check data
data = worksheet.get_all_values()
print('Headers:', data[0])
print(f'\nTotal rows: {len(data)}')

print('\nFirst 5 data rows:')
for i in range(1, min(6, len(data))):
    row = data[i]
    # Show episode marker, guest names, and IMDb IDs
    episode = row[1] if len(row) > 1 else ''
    guests = row[5] if len(row) > 5 else ''
    imdb_ids = row[7] if len(row) > 7 else ''
    print(f'{episode}: Guests={guests[:50]}{"..." if len(guests) > 50 else ""} | IMDb={imdb_ids[:50]}{"..." if len(imdb_ids) > 50 else ""}')

# Check for rows with guests but no IMDb IDs
print('\nChecking for missing IMDb IDs...')
missing_count = 0
total_with_guests = 0

for i in range(1, len(data)):
    row = data[i]
    if len(row) > 7:
        guests = row[5] if len(row) > 5 else ''
        imdb_ids = row[7] if len(row) > 7 else ''
        
        if guests and guests.strip():  # Has guest stars
            total_with_guests += 1
            if not imdb_ids or not imdb_ids.strip():  # Missing IMDb IDs
                missing_count += 1
                if missing_count <= 5:  # Show first 5 examples
                    print(f'  {row[1]}: {guests[:60]}... (NO IMDb IDs)')

print(f'\nSummary:')
print(f'Episodes with guests: {total_with_guests}')
print(f'Episodes missing IMDb IDs: {missing_count}')
print(f'Percentage missing: {(missing_count/total_with_guests*100):.1f}%' if total_with_guests > 0 else 'N/A')
