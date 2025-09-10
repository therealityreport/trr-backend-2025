#!/usr/bin/env python3
"""
Script Cleanup Utility
Moves deprecated/unused scripts to an archive folder while keeping essential pipeline scripts.
"""

import os
import shutil
from pathlib import Path

# Essential scripts to keep
ESSENTIAL_SCRIPTS = {
    # Complete Pipeline
    "fetch_show_info.py",
    "fetch_cast_info.py", 
    "build_update_info.py",
    "create_viable_cast_sheet.py",
    "build_realitease_info.py",
    
    # ViableCast Episode/Season Extraction
    "tmdb_final_extractor.py",
    "v2UniversalSeasonExtractorMiddleDownAllFormats.py",
    "v3UniversalSeasonExtractorBottomUpAllFormats.py",
    
    # Utilities
    "test_gsheets.py",
    "list_sheets.py",
    
    # Keep this cleanup script itself
    "cleanup_scripts.py"
}

# Person details scripts (moved to Person Details folder)
PERSON_DETAILS_SCRIPTS = {
    "fetch_famous_birthdays.py",
    "fetch_person_details.py",
    "fetch_person_details_wikidata.py", 
    "fetch_missing_person_info.py",
    "enhance_cast_info_imdb.py",
    "find_missing_cast_imdb.py",
    "find_missing_cast_selective.py",
    "add_cast_imdb_column.py"
}

def main():
    scripts_dir = Path(__file__).parent
    archive_dir = scripts_dir / "archived_scripts"
    
    # Create archive directory
    archive_dir.mkdir(exist_ok=True)
    
    # Get all Python scripts
    all_scripts = list(scripts_dir.glob("*.py"))
    
    moved_count = 0
    kept_count = 0
    
    print("🧹 Starting script cleanup...")
    print(f"📁 Archive directory: {archive_dir}")
    print()
    
    for script in all_scripts:
        if script.name in ESSENTIAL_SCRIPTS:
            print(f"✅ KEEPING: {script.name}")
            kept_count += 1
        else:
            try:
                # Move to archive
                shutil.move(str(script), str(archive_dir / script.name))
                print(f"📦 ARCHIVED: {script.name}")
                moved_count += 1
            except Exception as e:
                print(f"❌ ERROR moving {script.name}: {e}")
    
    print()
    print("🎉 Cleanup complete!")
    print(f"✅ Scripts kept: {kept_count}")
    print(f"📦 Scripts archived: {moved_count}")
    print()
    print("📋 Essential scripts remaining:")
    for script in sorted(ESSENTIAL_SCRIPTS):
        if script != "cleanup_scripts.py":
            print(f"   • {script}")

if __name__ == "__main__":
    main()
