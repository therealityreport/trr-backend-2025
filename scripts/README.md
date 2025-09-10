# Realitease 2025 Data Pipeline

## 📋 Complete Pipeline Overview

This streamlined collection contains **only the essential scripts** for the complete Realitease data processing pipeline from show addition to final output.

## 🎯 Step-by-Step Pipeline

### **Step 1: Add New Shows** 
```bash
cd "SHOW INFO"
python3 fetch_show_info.py
```
- **Purpose**: Add new reality shows to the `ShowInfo` sheet
- **Input**: TMDb and IMDb lists (automatically fetched)
- **Output**: Populated ShowInfo sheet with show metadata
- **Location**: `SHOW INFO/fetch_show_info.py`

### **Step 2: Extract Cast Information**
```bash
python3 fetch_cast_info.py
```
- **Purpose**: Extract cast members from shows in ShowInfo
- **Input**: ShowInfo sheet
- **Output**: Populated CastInfo sheet with cast details

### **Step 2.5: Enhance Person Details (Optional)**
```bash
# Add birthday and biographical data
python3 "Person Details/fetch_famous_birthdays.py"

# Enhance with IMDb data  
python3 "Person Details/enhance_cast_info_imdb.py"

# Fill any missing person info
python3 "Person Details/fetch_missing_person_info.py"
```
- **Purpose**: Enrich cast data with birthdays, IMDb IDs, biographical info
- **Input**: CastInfo sheet
- **Output**: Enhanced CastInfo with complete person profiles
- **Sources**: Famous Birthdays, IMDb, Wikidata

### **Step 3: Build Update Information**
```bash
python3 build_update_info.py
```
- **Purpose**: Create UpdateInfo sheet for tracking changes
- **Input**: CastInfo sheet
- **Output**: UpdateInfo sheet

### **Step 4: Create Viable Cast**
```bash
python3 create_viable_cast_sheet.py
```
- **Purpose**: Filter and prepare cast for episode extraction
- **Input**: UpdateInfo sheet
- **Output**: ViableCast sheet ready for processing

### **Step 5: Extract Episodes & Seasons (Choose ONE)**

#### **Option A: TMDB API-based (Recommended)**
```bash
python3 tmdb_final_extractor.py
```
- **Best for**: Reliability, speed, no web scraping issues
- **Method**: TMDB API using Person IDs and Show IDs
- **Rate**: ~1 row per 2 seconds
- **Reliability**: Very high

#### **Option B: IMDb Web Scraping (Enhanced)**
```bash
python3 v2UniversalSeasonExtractorMiddleDownAllFormats.py
```
- **Best for**: Detailed episode data, when TMDB lacks info
- **Method**: Selenium-based IMDb scraping (top → bottom)
- **Features**: Enhanced popup parsing, episode marker detection

#### **Option C: IMDb Bottom-Up Processing**
```bash
python3 v3UniversalSeasonExtractorBottomUpAllFormats.py
```
- **Best for**: Processing from bottom of sheet upward
- **Method**: Selenium-based IMDb scraping (bottom → top)
- **Use case**: Complementary to v2 for full coverage

### **Step 6: Final Aggregation**
```bash
python3 build_realitease_info.py
```
- **Purpose**: Generate final RealiteaseInfo output
- **Input**: Completed ViableCast sheet
- **Output**: Final aggregated RealiteaseInfo sheet

## 🛠️ Utility Scripts

### **Google Sheets Testing**
```bash
python3 test_gsheets.py
```
- Test Google Sheets connectivity and permissions

### **Sheet Management**
```bash
python3 list_sheets.py
```
- List all available sheets in the workbook

### **Script Management**
```bash
python3 cleanup_scripts.py
```
- Archive unused scripts (already run once)

## 📊 Data Flow Diagram

```
ShowInfo → CastInfo → UpdateInfo → ViableCast → RealiteaseInfo
    ↓        ↓          ↓           ↓            ↓
   Step1    Step2      Step3      Step4        Step6
                                     ↓
                                  Step5: Choose extraction method
                                     ↓
                           [TMDB API] OR [IMDb v2] OR [IMDb v3]
```

## 🎭 ViableCast Processing Strategy

For **Step 5** (episode/season extraction), you have three options:

1. **Start with TMDB** (`tmdb_final_extractor.py`) - Most reliable
2. **Fill gaps with IMDb v2** (`v2Universal...`) - Enhanced scraping
3. **Use v3 for bottom-up** (`v3Universal...`) - Complementary processing

## 📁 Folder Structure

### **Main Scripts** (11 scripts)
- Core pipeline scripts for show processing

### **Person Details/** (8 scripts)
- Person data enhancement and biographical information
- Birthday extraction, IMDb integration, missing data detection
- See `Person Details/README.md` for detailed documentation

### **archived_scripts/** (88 scripts)
- Deprecated and experimental scripts
- Safely archived for reference

## ⚡ Quick Start

### **Complete Pipeline (Basic)**
```bash
cd "SHOW INFO" && python3 fetch_show_info.py  # Add shows
cd .. && python3 fetch_cast_info.py           # Extract cast
python3 build_update_info.py                  # Build updates
python3 create_viable_cast_sheet.py           # Create viable cast
python3 tmdb_final_extractor.py               # Extract episodes (recommended)
python3 build_realitease_info.py              # Final output
```

### **Complete Pipeline (Enhanced with Person Details)**
```bash
cd "SHOW INFO" && python3 fetch_show_info.py           # Add shows
cd .. && python3 fetch_cast_info.py                   # Extract cast
python3 "Person Details/fetch_famous_birthdays.py"     # Add birthdays
python3 "Person Details/enhance_cast_info_imdb.py"     # Add IMDb data
python3 build_update_info.py                           # Build updates
python3 create_viable_cast_sheet.py                    # Create viable cast
python3 tmdb_final_extractor.py                        # Extract episodes
python3 build_realitease_info.py                       # Final output
```

## 🔧 Environment Requirements

- Python 3.9+
- Google Sheets API credentials
- TMDB API key (for tmdb_final_extractor.py)
- Chrome/ChromeDriver (for IMDb scrapers)

## 📈 Success Metrics

- **ShowInfo**: Shows successfully added
- **CastInfo**: Cast members extracted
- **ViableCast**: ~6,300+ rows ready for processing
- **Episode Extraction**: 97%+ completion rate
- **RealiteaseInfo**: Final aggregated output

---

*This streamlined pipeline processes reality TV show data from initial show addition through final cast episode aggregation.*

## 📅 Recent Updates (September 3, 2025)

### ✅ Successfully Organized Production Scripts
Today's session organized all working scripts into logical folder structure:

#### **Active Production Scripts**
- **ViableCast/** - 4 working scripts for episode/season extraction
  - `v3UniversalSeasonExtractorBottomUpAllFormats.py` (currently running)
  - `v2UniversalSeasonExtractorMiddleDownAllFormats.py` (currently running)
  - `tmdb_final_extractor.py` (TMDb API alternative)
  - `append_viable_cast.py` (data management)

- **CastInfo/** - 2 scripts for cast data collection
  - `fetch_cast_info.py` (main cast builder)
  - `fetch_cast_info_simple.py` (simplified version)

- **UpdateInfo/** - 1 script for data aggregation
  - `build_update_info.py` (person-level aggregations)

- **WWHLinfo/** - 1 script for WWHL-specific data
  - `fetch_WWHL_info.py` (Watch What Happens Live data)

- **Person Details/** - 9 scripts for person data enhancement
  - `build_realitease_info.py` (creates RealiteaseInfo sheet)
  - 8 other person enhancement scripts (birthdays, IMDb, etc.)

#### **System Optimizations**
- **Parallel Processing**: v2 and v3 extractors running simultaneously
- **Sleep Prevention**: caffeinate activated for continuous operation
- **Enhanced Episode Detection**: Improved parsing for better data capture
- **Flexible Saving Logic**: Scripts now save partial data when possible

#### **Data Progress**
- **v2 Script**: Processing 348 rows from row 6346 downward
- **v3 Script**: Processing 348 rows from bottom upward  
- **Total Coverage**: Full ViableCast sheet being processed by both scripts

#### **Script Organization**
- **Archived**: 80+ experimental/debug scripts moved to `archived_scripts/`
- **Organized**: Production scripts grouped by Google Sheet they work with
- **Documented**: READMEs created for each folder explaining purpose and usage

---
