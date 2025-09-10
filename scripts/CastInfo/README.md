# CastInfo Sheet Scripts

This folder contains scripts that work with the **CastInfo** sheet in the Realitease2025Data workbook.

## Working Scripts (September 3, 2025)

### Data Collection
- **`fetch_cast_info.py`** - Main cast information builder with TMDb API integration
  - Fetches cast data from TMDb API
  - Updates existing CastInfo entries
  - Supports batch processing and filtering
  - Production-ready with error handling

- **`fetch_cast_info_simple.py`** - Simplified version for bottom-up processing
  - Streamlined data collection without complex filters
  - Multi-source ID resolution
  - Designed for clean, incremental updates

### Data Maintenance
- **`normalize_show_names.py`** - Fixes show names based on ShowInfo
  - Replaces ShowName values with official names from ShowInfo
  - Uses Show IMDbID for matching
  - Logs warnings for missing show data

- **`castinfo_update_summary.py`** - Provides CastInfo update summaries
  - Reports on data changes and updates
  - Useful for tracking data quality

- **`remove_single_show_cast.py`** - Removes cast from specific shows
  - Cleanup utility for data maintenance
  - Handles bulk removals safely

## Sheet Purpose
The CastInfo sheet contains detailed cast member information including:
- Cast TMDb IDs
- Person IMDb IDs  
- Show associations
- Episode counts and season data

## Data Flow
CastInfo serves as a source for ViableCast and UpdateInfo sheets through the append_viable_cast.py script.
