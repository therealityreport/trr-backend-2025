# ViableCast Sheet Scripts

This folder contains scripts that work with the **ViableCast** sheet in the Realitease2025Data workbook.

## Working Scripts (September 3, 2025)

### Season/Episode Extraction
- **`v3UniversalSeasonExtractorBottomUpAllFormats.py`** - Enhanced IMDb scraper that processes from bottom of sheet upward
  - Uses Selenium WebDriver with anti-detection
  - Enhanced year-based season parsing (e.g., "2007" banner detection)
  - Flexible episode detection with multiple selectors
  - Batches updates every 10 rows
  - Currently running in production

- **`v2UniversalSeasonExtractorMiddleDownAllFormats.py`** - Enhanced IMDb scraper starting from row 6346 downward
  - Same enhanced functionality as v3
  - Starts at row 6346 and processes down to end of sheet
  - Batches updates every 15 rows
  - Currently running in production parallel to v3

- **`tmdb_final_extractor.py`** - TMDb API-based episode and season data extractor
  - Uses TMDb API instead of web scraping
  - Processes from top of sheet downward
  - Skips rows with existing data
  - Alternative to IMDb-based extractors

- **`universal_season_extractor_all_formats.py`** - General-purpose season extractor
  - Handles multiple episode/season formats
  - Flexible parsing for various reality shows
  - Production-ready alternative extractor

- **`create_viable_cast_sheet.py`** - Creates ViableCast sheet from CastInfo/UpdateInfo
  - Essential pipeline step 4
  - Filters and prepares data for episode extraction
  - Manages data flow between sheets

### Data Management
- **`append_viable_cast.py`** - Appends new viable cast members from CastInfo and UpdateInfo sheets
  - Filters and deduplicates entries
  - Backfills missing TMDb IDs and show names
  - Manages data flow between sheets

## Sheet Purpose
The ViableCast sheet contains processed reality TV cast data with episode counts and season information extracted from IMDb.

## Current Status
Both v2 and v3 extractors are running simultaneously to process the entire ViableCast sheet efficiently.
