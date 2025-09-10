# Person Details Scripts

This folder contains scripts for enriching cast member data with additional biographical information, IMDb IDs, and other person-specific details.

## 🎯 Person Data Enhancement Scripts

### **Birthday & Biographical Data**

#### `fetch_famous_birthdays.py`
- **Purpose**: Extract birthday and biographical data from Famous Birthdays website
- **Input**: Cast names from CastInfo sheet
- **Output**: Enhanced cast data with birthdays, ages, biographical info
- **Use Case**: Primary source for birthday data and biographical details

#### `fetch_person_details.py`
- **Purpose**: General person details extraction from multiple sources
- **Features**: Comprehensive person data aggregation
- **Output**: Enriched person profiles

#### `fetch_person_details_wikidata.py`
- **Purpose**: Extract person data from Wikidata
- **Features**: Structured biographical data, relationships, career info
- **Advantage**: High-quality, structured data source

### **IMDb Integration**

#### `enhance_cast_info_imdb.py`
- **Purpose**: Enhance existing cast data with IMDb information
- **Features**: Add IMDb profiles, filmography, ratings
- **Input**: CastInfo sheet with names
- **Output**: IMDb-enhanced cast profiles

#### `add_cast_imdb_column.py`
- **Purpose**: Add IMDb ID column to existing cast data
- **Features**: Bulk IMDb ID assignment
- **Use Case**: Retroactively add IMDb IDs to cast sheets

### **Missing Data Detection & Filling**

#### `find_missing_cast_imdb.py`
- **Purpose**: Identify cast members without IMDb IDs
- **Features**: Gap analysis and reporting
- **Output**: List of cast members needing IMDb data

#### `find_missing_cast_selective.py`
- **Purpose**: Selective missing cast identification
- **Features**: Targeted gap analysis with filtering
- **Use Case**: Focus on specific shows or cast subsets

#### `fetch_missing_person_info.py`
- **Purpose**: Fill gaps in person data for identified missing records
- **Features**: Targeted data enhancement
- **Use Case**: Complete incomplete person profiles

### **Person-Level Data Aggregation**

#### `build_realitease_info.py`
- **Purpose**: Create comprehensive person-focused sheet from ViableCast data
- **Features**: Aggregates unique cast members with show associations
- **Input**: ViableCast sheet data
- **Output**: RealiteaseInfo sheet with person-level summaries
- **Use Case**: Final person-focused data product for the Realitease platform

## 🔄 Typical Usage Flow

### **For New Cast Data:**
```bash
# 1. Extract basic cast info (main pipeline)
python3 ../fetch_cast_info.py

# 2. Add birthday and biographical data
python3 "Person Details/fetch_famous_birthdays.py"

# 3. Enhance with IMDb data
python3 "Person Details/enhance_cast_info_imdb.py"

# 4. Fill any gaps
python3 "Person Details/fetch_missing_person_info.py"
```

### **For Existing Data Enhancement:**
```bash
# 1. Find what's missing
python3 "Person Details/find_missing_cast_imdb.py"

# 2. Add missing IMDb IDs
python3 "Person Details/add_cast_imdb_column.py"

# 3. Enhance selectively
python3 "Person Details/find_missing_cast_selective.py"
```

## 📊 Data Sources

- **Famous Birthdays**: Primary source for birthdays and biographical info
- **IMDb**: Film/TV career data, ratings, detailed profiles
- **Wikidata**: Structured biographical data, relationships, career milestones

## ⚡ Integration with Main Pipeline

These scripts typically run **after Step 2** (fetch_cast_info.py) and **before Step 3** (build_update_info.py) to ensure cast data is fully enriched before processing:

```
ShowInfo → CastInfo → [PERSON DETAILS] → UpdateInfo → ViableCast → RealiteaseInfo
```

## 🎭 Output Enhancement

After running person details scripts, your cast data will include:
- ✅ **Birthdays & Ages**
- ✅ **IMDb IDs & Profiles** 
- ✅ **Biographical Information**
- ✅ **Career Highlights**
- ✅ **Complete Person Profiles**

---

*These scripts ensure comprehensive person data for all cast members in the Realitease database.*
