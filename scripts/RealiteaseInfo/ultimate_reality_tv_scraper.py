#!/usr/bin/env python3

import os
import sys
import time
import re
import requests
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup
from datetime import datetime
import urllib.parse

# Load environment variables
load_env_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
load_dotenv(load_env_path)

def save_debug_html(html: str, person_name: str, wiki_domain: str, title: str):
    """Save HTML content for debugging purposes"""
    try:
        # Create debug directory if it doesn't exist
        debug_dir = os.path.join(os.path.dirname(__file__), 'debug_html')
        os.makedirs(debug_dir, exist_ok=True)
        
        # Create safe filename
        safe_name = re.sub(r'[^\w\-_]', '_', f"{person_name}_{wiki_domain}_{title}")
        filename = f"debug_{safe_name}.html"
        filepath = os.path.join(debug_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html)
        
        print(f"      🔍 Debug HTML saved: {filepath}")
    except Exception as e:
        print(f"      ⚠️ Could not save debug HTML: {e}")

class FamousBirthdaysEnhancer:
    """
    Comprehensive scraper that combines multiple sources for reality TV cast data
    """
    
    def __init__(self):
        # Session setup for web scraping
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
        })
        
        # Google Sheets setup
        self.gc = None
        self.worksheet = None
        
        # Batch update management
        self.batch_updates = []
        self.batch_size = 25
        
        # Processing counters
        self.processed_count = 0
        self.updated_count = 0
        self.skipped_count = 0
        self.failed_count = 0
        self.sources_used = {}
        
        # Franchise wiki mappings
        self.setup_wiki_mappings()
    
    def setup_wiki_mappings(self):
        """Setup all wiki mappings for reality shows"""
        self.franchise_wikis = {
            'bachelor-nation.fandom.com': [
                'the bachelor', 'the bachelorette', 'bachelor in paradise',
                'the golden bachelor', 'the golden bachelorette'
            ],
            'real-housewives.fandom.com': [
                'the real housewives of atlanta', 'the real housewives of new jersey',
                'the real housewives of new york city', 'the real housewives of beverly hills',
                'the real housewives of miami', 'the real housewives of orange county',
                'the real housewives of dubai', 'the real housewives ultimate girls trip',
                'the real housewives of salt lake city', 'the real housewives of dallas',
                'the real housewives of potomac', 'the real housewives of d.c.'
            ],
            'vanderpump-rules.fandom.com': ['vanderpump rules', 'vanderpump villa', 'the valley'],
            'thechallenge.fandom.com': ['the challenge', 'the challenge: all stars', 'the challenge: usa'],
            'survivor.fandom.com': ['survivor'],
            'bigbrother.fandom.com': ['big brother', 'celebrity big brother', 'big brother reindeer games'],
            'loveisland.fandom.com': ['love island', 'love island: all stars', 'love island games', 'love island: beyond the villa'],
            'rupaulsdragrace.fandom.com': ["rupaul's drag race", "rupaul's drag race all stars", "rupaul's drag race global all stars"],
            'belowdeck.fandom.com': ['below deck', 'below deck mediterranean', 'below deck sailing yacht', 'below deck adventure', 'below deck down under'],
            'jerseyshore.fandom.com': ['jersey shore', 'jersey shore: family vacation', 'snooki & jwoww'],
            'kardashians.fandom.com': ['keeping up with the kardashians', 'the kardashians', 'life of kylie'],
            'loveandhiphop.fandom.com': ['love & hip hop atlanta', 'love & hip hop new york'],
            'amazingrace.fandom.com': ['the amazing race'],
            'toohottohandle.fandom.com': ['too hot to handle', 'perfect match'],
            'thecircle.fandom.com': ['the circle'],
            'dancemoms.fandom.com': ['dance moms'],
            'americasnexttopmodel.fandom.com': ["america's next top model"],
            'badgirlsclub.fandom.com': ['bad girls club', 'baddies east reunion'],
            'thehills.fandom.com': ['the hills', 'the hills: new beginnings', 'laguna beach'],
            'realworld.fandom.com': ['the real world', 'the real world homecoming'],
            'teenmom.fandom.com': ['teen mom og']
        }
        
        # Create reverse mapping for quick lookup
        self.show_to_wiki = {}
        for wiki, shows in self.franchise_wikis.items():
            for show in shows:
                self.show_to_wiki[show.lower()] = wiki
    
    def calculate_zodiac(self, birthday_str):
        """Calculate zodiac sign from birthday string (YYYY-MM-DD)"""
        if not birthday_str:
            return None
            
        try:
            date_obj = datetime.strptime(birthday_str, '%Y-%m-%d')
            month = date_obj.month
            day = date_obj.day
            
            # Zodiac sign calculation
            if (month == 3 and day >= 21) or (month == 4 and day <= 19):
                return "Aries"
            elif (month == 4 and day >= 20) or (month == 5 and day <= 20):
                return "Taurus"
            elif (month == 5 and day >= 21) or (month == 6 and day <= 20):
                return "Gemini"
            elif (month == 6 and day >= 21) or (month == 7 and day <= 22):
                return "Cancer"
            elif (month == 7 and day >= 23) or (month == 8 and day <= 22):
                return "Leo"
            elif (month == 8 and day >= 23) or (month == 9 and day <= 22):
                return "Virgo"
            elif (month == 9 and day >= 23) or (month == 10 and day <= 22):
                return "Libra"
            elif (month == 10 and day >= 23) or (month == 11 and day <= 21):
                return "Scorpio"
            elif (month == 11 and day >= 22) or (month == 12 and day <= 21):
                return "Sagittarius"
            elif (month == 12 and day >= 22) or (month == 1 and day <= 19):
                return "Capricorn"
            elif (month == 1 and day >= 20) or (month == 2 and day <= 18):
                return "Aquarius"
            elif (month == 2 and day >= 19) or (month == 3 and day <= 20):
                return "Pisces"
                
        except Exception as e:
            print(f"   ⚠️ Error calculating zodiac for {birthday_str}: {e}")
            return None

    def setup_google_sheets(self):
        """Setup Google Sheets connection"""
        try:
            print("🔄 Setting up Google Sheets connection...")
            
            scope = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            
            key_file_path = '/Users/thomashulihan/Projects/TRR-Backend/keys/trr-backend-df2c438612e1.json'
            credentials = Credentials.from_service_account_file(key_file_path, scopes=scope)
            
            self.gc = gspread.authorize(credentials)
            workbook = self.gc.open("Realitease2025Data")
            self.worksheet = workbook.worksheet("RealiteaseInfo")
            
            print("✅ Google Sheets connection successful - Connected to RealiteaseInfo")
            return True
            
        except Exception as e:
            print(f"❌ Google Sheets setup failed: {str(e)}")
            return False
    
    def analyze_text_for_gender(self, text, cast_name=""):
        """Comprehensive gender detection from text using pronouns and context"""
        if not text:
            return None
        
        text_lower = text.lower()
        cast_name_lower = cast_name.lower() if cast_name else ""
        
        # Remove the person's name from text to avoid false matches
        if cast_name:
            text_lower = text_lower.replace(cast_name_lower, "PERSON")
        
        print(f"      🔍 Analyzing text for gender markers...")
        
        # 1. Check for explicit gender mentions
        explicit_male = ['gender: male', 'sex: male', 'gender: m', 'is a male', 'male actor', 
                         'male contestant', 'male model', 'male singer', 'male dancer']
        explicit_female = ['gender: female', 'sex: female', 'gender: f', 'is a female', 
                          'female actress', 'female contestant', 'female model', 'female singer', 
                          'female dancer', 'actress']
        
        for term in explicit_male:
            if term in text_lower:
                print(f"      ✅ Found explicit male gender: '{term}'")
                return 'M'
        
        for term in explicit_female:
            if term in text_lower:
                print(f"      ✅ Found explicit female gender: '{term}'")
                return 'F'
        
        # 2. Enhanced pronoun analysis with context
        # Look for pronouns that clearly refer to the person
        male_pronoun_patterns = [
            r'\bhe\s+(?:is|was|has|had|will|would|can|could|should|might)\b',
            r'\bhim\s+(?:to|from|with|by|for|as|in|on|at)\b',
            r'\bhis\s+(?:career|life|work|role|performance|appearance|family|wife|girlfriend)\b',
            r'(?:made|gave|brought|took|sent|showed|told)\s+him\b',
            r'\bhe\s+(?:appeared|starred|played|worked|lived|grew|born|died)\b'
        ]
        
        female_pronoun_patterns = [
            r'\bshe\s+(?:is|was|has|had|will|would|can|could|should|might)\b',
            r'\bher\s+(?:to|from|with|by|for|as|in|on|at)\b',
            r'\bher\s+(?:career|life|work|role|performance|appearance|family|husband|boyfriend)\b',
            r'(?:made|gave|brought|took|sent|showed|told)\s+her\b',
            r'\bshe\s+(?:appeared|starred|played|worked|lived|grew|born|died)\b'
        ]
        
        male_matches = sum(len(re.findall(pattern, text_lower)) for pattern in male_pronoun_patterns)
        female_matches = sum(len(re.findall(pattern, text_lower)) for pattern in female_pronoun_patterns)
        
        print(f"      📊 Contextual pronoun matches - Male: {male_matches}, Female: {female_matches}")
        
        # 3. Count general pronouns as fallback
        simple_male = len(re.findall(r'\b(he|him|his|himself)\b', text_lower))
        simple_female = len(re.findall(r'\b(she|her|hers|herself)\b', text_lower))
        
        print(f"      📊 Simple pronoun count - Male: {simple_male}, Female: {simple_female}")
        
        # 4. Check for gendered titles and relationships
        male_titles = ['mr', 'mr.', 'mister', 'sir', 'king', 'prince', 'duke', 'lord', 
                      'boyfriend', 'husband', 'father', 'dad', 'son', 'brother', 'uncle', 
                      'nephew', 'grandfather', 'grandson', 'widower', 'bachelor']
        
        female_titles = ['ms', 'ms.', 'mrs', 'mrs.', 'miss', 'madam', 'lady', 'queen', 
                        'princess', 'duchess', 'girlfriend', 'wife', 'mother', 'mom', 'mum',
                        'daughter', 'sister', 'aunt', 'niece', 'grandmother', 'granddaughter', 
                        'widow', 'bachelorette']
        
        male_title_count = sum(1 for title in male_titles if re.search(r'\b' + title + r'\b', text_lower))
        female_title_count = sum(1 for title in female_titles if re.search(r'\b' + title + r'\b', text_lower))
        
        if male_title_count > 0 or female_title_count > 0:
            print(f"      📊 Title/relationship count - Male: {male_title_count}, Female: {female_title_count}")
        
        # 5. Make determination based on all evidence
        total_male_evidence = male_matches * 2 + simple_male + male_title_count * 3
        total_female_evidence = female_matches * 2 + simple_female + female_title_count * 3
        
        print(f"      📊 Total evidence score - Male: {total_male_evidence}, Female: {total_female_evidence}")
        
        if total_male_evidence >= 3 and total_male_evidence > total_female_evidence * 1.5:
            print(f"      ✅ Determined MALE based on comprehensive analysis")
            return 'M'
        elif total_female_evidence >= 3 and total_female_evidence > total_male_evidence * 1.5:
            print(f"      ✅ Determined FEMALE based on comprehensive analysis")
            return 'F'
        elif total_male_evidence > 0 and total_female_evidence == 0:
            print(f"      ✅ Determined MALE (only male indicators found)")
            return 'M'
        elif total_female_evidence > 0 and total_male_evidence == 0:
            print(f"      ✅ Determined FEMALE (only female indicators found)")
            return 'F'
        else:
            print(f"      ⚠️ Could not determine gender conclusively")
            return None
    
    def process_cast_member(self, row_data, row_num):
        """Process a single cast member through all data sources"""
        cast_name = row_data.get('cast_name', '')
        show_names = row_data.get('show_names', '')
        
        print(f"\n{'='*60}")
        print(f"🎭 Row {row_num}: {cast_name} from {show_names or 'Unknown Shows'}")
        print(f"   📊 Current data - Gender: '{row_data.get('gender') or 'EMPTY'}', "
              f"Birthday: '{row_data.get('birthday') or 'EMPTY'}', "
              f"Zodiac: '{row_data.get('zodiac') or 'EMPTY'}'")
        
        # Check what we actually need to find
        needs_gender = not row_data.get('gender') or row_data.get('gender').strip() == ''
        needs_birthday = not row_data.get('birthday') or row_data.get('birthday').strip() == ''
        needs_zodiac = not row_data.get('zodiac') or row_data.get('zodiac').strip() == ''
        
        print(f"   🎯 Needs - Gender: {needs_gender}, Birthday: {needs_birthday}, Zodiac: {needs_zodiac}")
        
        updates_made = {}
        
        # Check if we need to calculate zodiac from existing birthday
        if row_data.get('birthday') and not row_data.get('zodiac'):
            zodiac = self.calculate_zodiac(row_data['birthday'])
            if zodiac:
                updates_made['zodiac'] = zodiac
                print(f"   ♈ Calculated zodiac from existing birthday: {zodiac}")
        
        # Only search if we're missing data
        if needs_gender or needs_birthday:
            print("🔍 Searching for missing data...")
            
            # Collect all text content for gender analysis
            all_text_content = ""
            
            # Try sources in order of reliability
            sources_to_try = [
                ('fandom_wiki', self.search_fandom_wiki),
                ('famous_birthdays', self.search_famous_birthdays),
                ('wikipedia', self.search_wikipedia),
                ('wikidata', self.search_wikidata),
                ('imdb', self.search_imdb),
                ('google_search', self.search_google)
            ]
            
            for source_name, source_func in sources_to_try:
                if (not needs_birthday or row_data.get('birthday')) and (not needs_gender or row_data.get('gender')):
                    print(f"   ✅ All needed data found, stopping search")
                    break  # We have what we need
                
                print(f"   🔍 Trying {source_name}...")
                try:
                    result = source_func(cast_name, show_names)
                except Exception as e:
                    print(f"      ❌ {source_name} failed with error: {str(e)}")
                    result = None
                
                if result:
                    # Collect any bio/text content for gender analysis
                    if result.get('bio'):
                        all_text_content += " " + result['bio']
                    
                    # Only update birthday if we actually need it and don't have one
                    if result.get('birthday') and needs_birthday and not row_data.get('birthday'):
                        updates_made['birthday'] = result['birthday']
                        row_data['birthday'] = result['birthday']
                        # Calculate zodiac from birthday
                        zodiac = self.calculate_zodiac(result['birthday'])
                        if zodiac and (needs_zodiac and not row_data.get('zodiac')):
                            updates_made['zodiac'] = zodiac
                            row_data['zodiac'] = zodiac
                            print(f"   ♈ Calculated zodiac: {zodiac}")
                        self.track_source(f"{source_name}_birthday")
                        print(f"   ✅ Added birthday: {result['birthday']}")
                    
                    # Only update gender if we actually need it and don't have one
                    if result.get('gender') and needs_gender and not row_data.get('gender'):
                        updates_made['gender'] = result['gender']
                        row_data['gender'] = result['gender']
                        self.track_source(f"{source_name}_gender")
                        print(f"   ✅ Added gender: {result['gender']}")
                    
                    # Log when we skip due to existing data
                    if result.get('birthday') and row_data.get('birthday') and result['birthday'] != row_data['birthday']:
                        print(f"   ℹ️ Preserving existing birthday: {row_data.get('birthday')} (found different: {result.get('birthday')})")
                    if result.get('gender') and row_data.get('gender') and result['gender'] != row_data['gender']:
                        print(f"   ℹ️ Preserving existing gender: {row_data.get('gender')} (found different: {result.get('gender')})")
            
            # If we still don't have gender but collected text, analyze it
            if needs_gender and not row_data.get('gender') and all_text_content:
                print("   🔍 Analyzing collected text for gender...")
                gender = self.analyze_text_for_gender(all_text_content, cast_name)
                if gender:
                    updates_made['gender'] = gender
                    row_data['gender'] = gender
                    self.track_source('text_analysis_gender')
                    print(f"   ✅ Gender from text analysis: {gender}")
            elif row_data.get('gender'):
                print(f"   ℹ️ Preserving existing gender: {row_data.get('gender')}")
        else:
            print("   ✅ All data already present, no search needed")
        
        # Return only the actual updates made (don't recalculate)
        if updates_made:
            print(f"   📝 Updates to apply: {list(updates_made.keys())}")
        else:
            print(f"   ℹ️ No new data to add")
        
        return updates_made
    
    def search_fandom_wiki(self, cast_name, show_names):
        """Search show-specific Fandom wikis"""
        try:
            if not show_names:
                print(f"      ⚠️ No show names provided for fandom search")
                return None
            
            # Try to find a matching wiki for any of the shows
            wiki_domain = None
            shows_list = [s.strip() for s in show_names.split(',')]
            
            for show in shows_list:
                wiki_domain = self.show_to_wiki.get(show.lower())
                if wiki_domain:
                    print(f"      🎯 Found wiki domain: {wiki_domain} for show: {show}")
                    break
            
            if not wiki_domain:
                print(f"      ⚠️ No matching fandom wiki found for shows: {shows_list}")
                return None
            
            # Format name for URL - try working pattern first (optimized order)
            # Fandom consistently uses Title_Case_With_Underscores format
            clean_name = cast_name.replace('.', '').replace("'", "")
            
            # Try the most successful pattern first
            urls_to_try = [
                # HIGHEST SUCCESS: Title_Case_With_Underscores (e.g., Teresa_Giudice)
                f"https://{wiki_domain}/wiki/{clean_name.title().replace(' ', '_')}",
                
                # Fallback patterns (in order of likelihood)
                f"https://{wiki_domain}/wiki/{clean_name.replace(' ', '_')}",  # Original_Case_With_Underscores
                f"https://{wiki_domain}/wiki/{clean_name.title().replace(' ', '-')}",  # Title-Case-With-Hyphens
                f"https://{wiki_domain}/wiki/{clean_name.replace(' ', '-')}",  # Original-Case-With-Hyphens
                f"https://{wiki_domain}/wiki/{clean_name.lower().replace(' ', '_')}",  # lowercase_with_underscores
                f"https://{wiki_domain}/wiki/{clean_name.lower().replace(' ', '-')}",  # lowercase-with-hyphens
            ]
            
            for url in urls_to_try:
                print(f"      🔗 Trying fandom URL: {url}")
                response = self.session.get(url, timeout=15)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Check if this is actually a person page (not a redirect or disambiguation)
                    page_text = soup.get_text().lower()
                    if 'this page is a redirect' in page_text or 'disambiguation' in page_text:
                        print(f"      ⚠️ Page is redirect/disambiguation, skipping")
                        continue
                    
                    result = {}
                    
                    # Extract birthday
                    birthday = self.extract_date_from_page(soup)
                    if birthday:
                        result['birthday'] = birthday
                        print(f"      ✅ Found birthday: {birthday}")
                    
                    # Extract gender with enhanced analysis
                    gender = self.analyze_text_for_gender(page_text, cast_name)
                    if gender:
                        result['gender'] = gender
                        print(f"      ✅ Found gender: {gender}")
                    
                    # Store bio text for further analysis
                    result['bio'] = soup.get_text()[:1000] if soup.get_text() else ""
                    
                    if result:
                        print(f"      ✅ Successfully scraped {wiki_domain}")
                        return result
                else:
                    print(f"      ❌ HTTP {response.status_code} for {url}")
            
            print(f"      ⚠️ No valid pages found on {wiki_domain}")
            return None
            
        except Exception as e:
            print(f"      ❌ Fandom wiki error: {str(e)}")
            return None
    
    def search_famous_birthdays(self, cast_name, show_name):
        """Search Famous Birthdays website"""
        try:
            name_slug = cast_name.lower().replace(' ', '-').replace('.', '').replace("'", "")
            
            urls_to_try = [
                f"https://www.famousbirthdays.com/people/{name_slug}.html",
            ]
            
            # Try with first-last and last-first
            name_parts = cast_name.split()
            if len(name_parts) == 2:
                urls_to_try.append(f"https://www.famousbirthdays.com/people/{name_parts[1].lower()}-{name_parts[0].lower()}.html")
            
            for url in urls_to_try:
                response = self.session.get(url, timeout=10)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    result = {}
                    
                    # Get full page text
                    page_text = soup.get_text()
                    
                    # Extract birthday - use Famous Birthdays specific extraction
                    birthday = self.extract_famous_birthdays_date(soup)
                    if birthday:
                        result['birthday'] = birthday
                    
                    # Extract gender with enhanced analysis
                    gender = self.analyze_text_for_gender(page_text, cast_name)
                    if gender:
                        result['gender'] = gender
                    
                    # Store text for further analysis
                    result['bio'] = page_text[:1000] if page_text else ""
                    
                    if result:
                        print(f"      ✅ Found on Famous Birthdays")
                        return result
            
            return None
            
        except Exception as e:
            return None
    
    def search_wikipedia(self, cast_name, show_name):
        """Search Wikipedia"""
        try:
            search_url = "https://en.wikipedia.org/w/api.php"
            params = {
                'action': 'query',
                'list': 'search',
                'srsearch': f"{cast_name} {show_name or ''}".strip(),
                'format': 'json',
                'srlimit': 3
            }
            
            response = self.session.get(search_url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                for result in data.get('query', {}).get('search', []):
                    page_title = result.get('title')
                    page_url = f"https://en.wikipedia.org/wiki/{page_title.replace(' ', '_')}"
                    
                    page_data = self.scrape_url_for_data(page_url, cast_name)
                    if page_data and (page_data.get('birthday') or page_data.get('gender')):
                        print(f"      ✅ Found on Wikipedia")
                        return page_data
            
            return None
            
        except Exception as e:
            return None
    
    def search_wikidata(self, cast_name, show_name):
        """Search Wikidata"""
        try:
            search_url = "https://www.wikidata.org/w/api.php"
            params = {
                'action': 'wbsearchentities',
                'search': cast_name,
                'language': 'en',
                'format': 'json',
                'limit': 5
            }
            
            response = self.session.get(search_url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                for item in data.get('search', []):
                    item_id = item.get('id')
                    
                    # Get detailed info
                    detail_params = {
                        'action': 'wbgetentities',
                        'ids': item_id,
                        'languages': 'en',
                        'format': 'json',
                        'props': 'claims|descriptions'
                    }
                    
                    detail_response = self.session.get(search_url, params=detail_params, timeout=10)
                    
                    if detail_response.status_code == 200:
                        detail_data = detail_response.json()
                        entity = detail_data.get('entities', {}).get(item_id, {})
                        claims = entity.get('claims', {})
                        
                        # Check if human
                        is_human = False
                        if 'P31' in claims:
                            for claim in claims['P31']:
                                if claim.get('mainsnak', {}).get('datavalue', {}).get('value', {}).get('id') == 'Q5':
                                    is_human = True
                                    break
                        
                        if not is_human:
                            continue
                        
                        result = {}
                        
                        # Get birthday (P569)
                        if 'P569' in claims:
                            for claim in claims['P569']:
                                date_value = claim.get('mainsnak', {}).get('datavalue', {}).get('value', {})
                                if date_value:
                                    time_str = date_value.get('time', '')
                                    match = re.match(r'\+?(\d{4})-(\d{2})-(\d{2})', time_str)
                                    if match:
                                        year, month, day = match.groups()
                                        result['birthday'] = f"{year}-{month}-{day}"
                        
                        # Get gender (P21)
                        if 'P21' in claims:
                            gender_claim = claims['P21'][0]
                            gender_id = gender_claim.get('mainsnak', {}).get('datavalue', {}).get('value', {}).get('id')
                            if gender_id == 'Q6581097':
                                result['gender'] = 'M'
                            elif gender_id == 'Q6581072':
                                result['gender'] = 'F'
                        
                        # Get description for bio
                        descriptions = entity.get('descriptions', {})
                        if 'en' in descriptions:
                            result['bio'] = descriptions['en'].get('value', '')
                        
                        if result:
                            print(f"      ✅ Found on Wikidata")
                            return result
            
            return None
            
        except Exception as e:
            return None
    
    def search_google(self, cast_name, show_names):
        """Search Google for cast member information"""
        try:
            # Build comprehensive search query
            search_terms = [cast_name]
            if show_names:
                # Add main show names
                shows = [s.strip() for s in show_names.split(',')]
                search_terms.extend(shows[:2])  # Limit to first 2 shows to avoid too long query
            
            # Add terms to help find biographical info
            search_terms.extend(['birthday', 'birth date', 'reality tv'])
            
            search_query = ' '.join(search_terms)
            print(f"      🔍 Google search query: {search_query}")
            
            # Use Google search (being respectful with requests)
            search_url = f"https://www.google.com/search?q={urllib.parse.quote(search_query)}"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            
            response = self.session.get(search_url, headers=headers, timeout=15)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Look for direct information in search results
                result = {}
                page_text = soup.get_text()
                
                # Try to extract birthday from search results directly
                birthday = self.extract_date_from_page(soup)
                if birthday:
                    result['birthday'] = birthday
                    print(f"      ✅ Found birthday in Google results: {birthday}")
                
                # Try to extract gender from search results
                gender = self.analyze_text_for_gender(page_text, cast_name)
                if gender:
                    result['gender'] = gender
                    print(f"      ✅ Found gender in Google results: {gender}")
                
                # Look for links to scrape
                if not result:
                    print(f"      🔗 Looking for scrapeable links in Google results...")
                    links = soup.find_all('a', href=True)
                    
                    # Priority domains to check
                    priority_domains = [
                        'wikipedia.org', 'imdb.com', 'famousbirthdays.com',
                        'fandom.com', 'wikia.com', 'tvguide.com', 'eonline.com'
                    ]
                    
                    for link in links[:10]:  # Check first 10 links
                        href = link.get('href', '')
                        if '/url?q=' in href:
                            # Extract actual URL from Google redirect
                            actual_url = href.split('/url?q=')[1].split('&')[0]
                            actual_url = urllib.parse.unquote(actual_url)
                            
                            # Check if it's a priority domain
                            for domain in priority_domains:
                                if domain in actual_url and actual_url.startswith('http'):
                                    print(f"      🔗 Trying priority link: {actual_url}")
                                    link_result = self.scrape_url_for_data(actual_url, cast_name)
                                    if link_result and (link_result.get('birthday') or link_result.get('gender')):
                                        print(f"      ✅ Found data from linked page")
                                        return link_result
                
                # Store any text for potential analysis
                if page_text:
                    result['bio'] = page_text[:1000]
                
                if result:
                    print(f"      ✅ Found data via Google search")
                    return result
                else:
                    print(f"      ⚠️ No biographical data found in Google results")
                    return None
            else:
                print(f"      ❌ Google search failed with status {response.status_code}")
                return None
            
        except Exception as e:
            print(f"      ❌ Google search error: {str(e)}")
            return None
    
    def search_imdb(self, cast_name, show_name):
        """Search IMDb"""
        try:
            search_query = urllib.parse.quote(cast_name)
            search_url = f"https://www.imdb.com/find?q={search_query}&s=nm"
            
            response = self.session.get(search_url, timeout=10)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find first person result
                first_result = soup.find('a', href=re.compile(r'/name/nm\d+'))
                if first_result:
                    person_url = f"https://www.imdb.com{first_result['href']}"
                    
                    person_response = self.session.get(person_url, timeout=10)
                    if person_response.status_code == 200:
                        person_soup = BeautifulSoup(person_response.text, 'html.parser')
                        result = {}
                        
                        # Get page text for analysis
                        page_text = person_soup.get_text()
                        
                        # Look for birthday
                        birth_info = person_soup.find('time', {'datetime': True})
                        if birth_info:
                            datetime_str = birth_info.get('datetime')
                            if datetime_str:
                                try:
                                    date_obj = datetime.strptime(datetime_str, '%Y-%m-%d')
                                    result['birthday'] = date_obj.strftime('%Y-%m-%d')
                                except:
                                    pass
                        
                        # Analyze for gender
                        gender = self.analyze_text_for_gender(page_text, cast_name)
                        if gender:
                            result['gender'] = gender
                        
                        # Store bio text
                        result['bio'] = page_text[:1000] if page_text else ""
                        
                        if result:
                            print(f"      ✅ Found on IMDb")
                            return result
            
            return None
            
        except Exception as e:
            return None
    
    def scrape_url_for_data(self, url, cast_name):
        """Generic function to scrape a URL for birthday/gender"""
        try:
            response = self.session.get(url, timeout=10)
            if response.status_code != 200:
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            result = {}
            
            # Get full page text
            page_text = soup.get_text()
            
            # Extract birthday
            birthday = self.extract_date_from_page(soup)
            if birthday:
                result['birthday'] = birthday
            
            # Extract gender with enhanced analysis
            gender = self.analyze_text_for_gender(page_text, cast_name)
            if gender:
                result['gender'] = gender
            
            # Store text for potential further analysis
            result['bio'] = page_text[:1000] if page_text else ""
            
            return result if result else None
            
        except Exception as e:
            return None
    
    def extract_date_from_page(self, soup):
        """Extract birthday from Fandom page using proven extraction methods"""
        try:
            # Define infobox label keys (from working Fandom script)
            infobox_label_keys = {
                "born","birth","birth date","birthdate","date of birth","dob","birthday","birth_date",
                "born on","birth_day","birthplace","date born","birth date:"
            }
            
            # Method 1: Check for hidden structured data first (most reliable)
            bday_span = soup.find("span", class_="bday")
            if bday_span:
                dt = bday_span.get_text(strip=True)
                parsed_date = self.parse_date_string(dt)
                if parsed_date:
                    return parsed_date

            # Method 2: <time itemprop="birthDate" datetime="YYYY-MM-DD">
            time_tag = soup.find("time", attrs={"itemprop": "birthDate"})
            if time_tag and time_tag.has_attr("datetime"):
                dt = time_tag["datetime"].strip()
                parsed_date = self.parse_date_string(dt)
                if parsed_date:
                    return parsed_date

            # Method 3: Portable infobox structure - check multiple selectors
            infobox_selectors = [
                ".portable-infobox .pi-item.pi-data",
                ".portable-infobox .pi-data",
                ".infobox .pi-item.pi-data",
                ".infobox .pi-data"
            ]
            
            for selector in infobox_selectors:
                nodes = soup.select(selector)
                for node in nodes:
                    # Check data-source (like data-source="Born" or data-source="Birthdate")
                    ds = (node.get("data-source") or "").strip().lower()
                    if ds in infobox_label_keys or ds == "born" or any(key in ds for key in ["birth", "born"]):
                        val = node.select_one(".pi-data-value")
                        if val:
                            val_text = val.get_text(" ", strip=True)
                            parsed_date = self.parse_date_string(val_text)
                            if parsed_date:
                                return parsed_date
                    
                    # Check label text
                    lab = node.select_one(".pi-data-label")
                    val = node.select_one(".pi-data-value")
                    if lab and val:
                        lab_txt = (lab.get_text(" ", strip=True) or "").lower()
                        if any(k in lab_txt for k in infobox_label_keys):
                            val_text = val.get_text(" ", strip=True)
                            parsed_date = self.parse_date_string(val_text)
                            if parsed_date:
                                return parsed_date

            # Method 4: Traditional table structure
            for td in soup.find_all("td"):
                if td.find("b") or td.find("strong"):
                    label_text = td.get_text(strip=True).lower()
                    if any(k in label_text for k in infobox_label_keys):
                        next_td = td.find_next_sibling("td")
                        if next_td:
                            val_text = next_td.get_text(" ", strip=True)
                            parsed_date = self.parse_date_string(val_text)
                            if parsed_date:
                                return parsed_date

            # Method 5: Look for table rows with "Born" or similar labels
            for tr in soup.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    label_text = tds[0].get_text(strip=True).lower()
                    if any(k in label_text for k in infobox_label_keys):
                        val_text = tds[1].get_text(" ", strip=True)
                        parsed_date = self.parse_date_string(val_text)
                        if parsed_date:
                            return parsed_date
            
            return None
            
        except Exception as e:
            return None
    
    def extract_famous_birthdays_date(self, soup):
        """Extract birthday from Famous Birthdays page using multiple methods"""
        try:
            # Method 1: Check for structured data (most reliable)
            scripts = soup.find_all('script', type='application/ld+json')
            for script in scripts:
                try:
                    import json
                    data = json.loads(script.get_text())
                    if 'birthDate' in data:
                        birth_date = data['birthDate']
                        # Parse ISO format like "1991-12-29T00:00:00-05:00"
                        if 'T' in birth_date:
                            birth_date = birth_date.split('T')[0]  # Extract just the date part
                        return birth_date
                except:
                    continue
            
            # Method 2: Look for bio-module section with birthday
            bio_section = soup.find('div', class_='bio-module')
            if bio_section:
                bio_text = bio_section.get_text()
                parsed_date = self.parse_date_string(bio_text)
                if parsed_date:
                    return parsed_date
            
            # Method 3: Look for specific birthday patterns in page text
            page_text = soup.get_text()
            
            # Famous Birthdays specific patterns
            patterns = [
                r'Birthday\s*[:]*\s*(\w+)\s+(\d{1,2})\s*,?\s*(\d{4})',  # Birthday: December 29, 1991
                r'Born\s*[:]*\s*(\w+)\s+(\d{1,2})\s*,?\s*(\d{4})',     # Born: December 29, 1991
                r'(\w+)\s+(\d{1,2})\s*,?\s*(\d{4})',                   # December 29, 1991
            ]
            
            for pattern in patterns:
                import re
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    groups = match.groups()
                    if len(groups) == 3:
                        month, day, year = groups
                        date_str = f"{month} {day} {year}"
                        parsed_date = self.parse_date_string(date_str)
                        if parsed_date:
                            return parsed_date
            
            return None
            
        except Exception as e:
            return None
    
    def parse_date_string(self, date_str):
        """Parse various date formats into YYYY-MM-DD using proven working logic"""
        if not date_str:
            return None
            
        # Define month mapping (from working Fandom script)
        DATE_MONTHS = {
            "january": "01","february": "02","march": "03","april": "04","may": "05","june": "06",
            "july": "07","august": "08","september": "09","october": "10","november": "11","december": "12",
            "jan": "01","feb": "02","mar": "03","apr": "04","may": "05","jun": "06","jul": "07","aug": "08",
            "sep": "09","sept": "09","oct": "10","nov": "11","dec": "12"
        }
        
        # Normalize text (remove extra whitespace)
        def normalize_text(s):
            return re.sub(r"\s+", " ", (s or "").strip())
        
        t = normalize_text(date_str).lower()

        # Clean up common extra text patterns (age, parentheses, etc.)
        t = re.sub(r'\s*\(age.*?\)', '', t)  # Remove "(age 39)" etc.
        t = re.sub(r'\s*\(.*?\)', '', t)     # Remove any other parentheses content
        
        # Remove common labels that might be concatenated with dates (no space)
        t = re.sub(r'^(birthdate|birthday|born|date of birth)', '', t, flags=re.IGNORECASE)
        
        t = normalize_text(t)

        # Try ISO first (YYYY-MM-DD)
        m = re.search(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b", t)
        if m:
            y, mo, d = m.groups()
            return f"{y}-{int(mo):02d}-{int(d):02d}"

        # "Month DD, YYYY" or "Month DDth, YYYY" (with ordinal suffixes)
        m = re.search(r"\b([a-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})\b", t)
        if m:
            mon, day, year = m.groups()
            mm = DATE_MONTHS.get(mon.lower())
            if mm:
                return f"{year}-{mm}-{int(day):02d}"

        # "DD Month YYYY" or "DDth Month YYYY" (with ordinal suffixes)
        m = re.search(r"\b(\d{1,2})(?:st|nd|rd|th)?\s+([a-z]+)\s+(\d{4})\b", t)
        if m:
            day, mon, year = m.groups()
            mm = DATE_MONTHS.get(mon.lower())
            if mm:
                return f"{year}-{mm}-{int(day):02d}"

        # "MM/DD/YYYY" or "MM-DD-YYYY"
        m = re.search(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})\b", t)
        if m:
            mo, d, y = m.groups()
            return f"{y}-{int(mo):02d}-{int(d):02d}"

        # "DD/MM/YYYY" (European format) - try if first attempt fails
        m = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", t)
        if m:
            first, second, year = m.groups()
            # Assume European format if first number > 12 (can't be month)
            if int(first) > 12:
                return f"{year}-{int(second):02d}-{int(first):02d}"
            # Otherwise assume American format
            return f"{year}-{int(first):02d}-{int(second):02d}"

        return None
    
    def track_source(self, source_name):
        """Track which sources provided data"""
        if source_name not in self.sources_used:
            self.sources_used[source_name] = 0
        self.sources_used[source_name] += 1
    
    def add_to_batch(self, row_num, updates):
        """Add updates to batch queue"""
        if updates:
            self.batch_updates.append((row_num, updates))
            
            # Process batch if it reaches the batch size
            if len(self.batch_updates) >= self.batch_size:
                self.process_batch()
    
    def process_batch(self):
        """Process all queued batch updates"""
        if not self.batch_updates:
            return
        
        print(f"\n📤 Processing batch of {len(self.batch_updates)} updates...")
        
        try:
            # Prepare batch update data
            requests = []
            
            for row_num, updates in self.batch_updates:
                for field, value in updates.items():
                    # Column mapping for RealiteaseInfo (1-indexed for gspread)
                    column_map = {
                        'gender': 8,    # Column H
                        'birthday': 9,  # Column I
                        'zodiac': 10,   # Column J
                    }
                    
                    if field in column_map:
                        # Use simple cell update for batch with sheet name
                        cell_range = f"RealiteaseInfo!{chr(64 + column_map[field])}{row_num}"
                        requests.append({
                            'range': cell_range,
                            'values': [[str(value)]]
                        })
            
            if requests:
                # Execute batch update using values_batch_update
                body = {
                    'valueInputOption': 'RAW',
                    'data': requests
                }
                self.worksheet.spreadsheet.values_batch_update(body)
                print(f"   ✅ Batch update successful: {len(self.batch_updates)} rows updated")
                
                # Update counter
                self.updated_count += len(self.batch_updates)
            
            # Clear the batch
            self.batch_updates = []
            
            # Add a small delay after batch processing
            time.sleep(1)
            
        except Exception as e:
            print(f"   ❌ Batch update failed: {e}")
            # Fall back to individual updates
            for row_num, updates in self.batch_updates:
                self.update_spreadsheet_single(row_num, updates)
            self.batch_updates = []
    
    def update_spreadsheet_single(self, row_num, updates):
        """Fallback single row update"""
        try:
            columns = {
                'gender': 8,    # Column H
                'birthday': 9,  # Column I
                'zodiac': 10,   # Column J
            }
            
            for field, value in updates.items():
                if field in columns:
                    self.worksheet.update_cell(row_num, columns[field], value)
                    time.sleep(0.3)
            
            return True
            
        except Exception as e:
            print(f"   ❌ Failed to update row {row_num}: {e}")
            return False
    
    def process_range(self):
        """Process cast members in specified row range"""
        if not self.setup_google_sheets():
            return False
        
        try:
            # Get all data from spreadsheet
            print("📊 Loading spreadsheet data...")
            all_data = self.worksheet.get_all_values()
            
            total_rows = len(all_data)
            print(f"📊 Total rows in spreadsheet: {total_rows}")
            
            # Get user input for range
            print("\n🎯 Select processing range:")
            print(f"1. Process all rows (2 to {total_rows})")
            print("2. Process specific range")
            print("3. Process from specific row to end")
            print("4. Process rows with missing data only")
            
            choice = input("\nEnter choice (1-4): ").strip()
            
            if choice == '1':
                start_row = 2
                end_row = total_rows
            elif choice == '2':
                start_row = int(input("Enter start row: "))
                end_row = int(input("Enter end row: "))
            elif choice == '3':
                start_row = int(input("Enter start row: "))
                end_row = total_rows
            elif choice == '4':
                start_row = 2
                end_row = total_rows
                print("Will process only rows with missing gender/birthday/zodiac data")
            else:
                print("❌ Invalid choice")
                return False
            
            # Validate range
            if start_row < 2:
                start_row = 2
            if end_row > total_rows:
                end_row = total_rows
            
            print(f"\n📊 Processing rows {start_row} to {end_row}")
            print(f"📊 Maximum rows to process: {end_row - start_row + 1}")
            print(f"📊 Batch updates every {self.batch_size} rows")
            
            # Show which data sources are available
            print("\n📚 Available data sources:")
            print("   ✅ Fandom Wikis (show-specific)")
            print("   ✅ Famous Birthdays")
            print("   ✅ Wikipedia")
            print("   ✅ Wikidata")
            print("   ✅ IMDb")
            print("   ✅ Google Search (comprehensive fallback)")
            print("   ✅ Enhanced text analysis for gender detection")
            
            # Confirm before proceeding
            confirm = input("\nProceed? (y/n): ").strip().lower()
            if confirm != 'y':
                print("❌ Cancelled by user")
                return False
            
            # Process each row
            for row_num in range(start_row, end_row + 1):
                row_index = row_num - 1  # Convert to 0-based index
                row = all_data[row_index]
                
                # Parse row data - RealiteaseInfo structure
                row_data = {
                    'cast_name': row[0] if len(row) > 0 else '',        # Column A: CastName
                    'cast_imdb_id': row[1] if len(row) > 1 else '',     # Column B: CastIMDbID
                    'cast_tmdb_id': row[2] if len(row) > 2 else '',     # Column C: CastTMDbID
                    'show_names': row[3] if len(row) > 3 else '',       # Column D: ShowNames
                    'show_imdb_ids': row[4] if len(row) > 4 else '',    # Column E: ShowIMDbIDs
                    'show_tmdb_ids': row[5] if len(row) > 5 else '',    # Column F: ShowTMDbIDs
                    'show_count': row[6] if len(row) > 6 else '',       # Column G: ShowCount
                    'gender': row[7] if len(row) > 7 else '',           # Column H: Gender
                    'birthday': row[8] if len(row) > 8 else '',         # Column I: Birthday
                    'zodiac': row[9] if len(row) > 9 else ''            # Column J: Zodiac
                }
                
                # Skip if no cast name
                if not row_data['cast_name']:
                    continue
                
                # For option 4, skip if all data is present
                if choice == '4':
                    has_all_data = all([
                        row_data['gender'],
                        row_data['birthday'],
                        row_data['zodiac']
                    ])
                    if has_all_data:
                        self.skipped_count += 1
                        continue
                
                self.processed_count += 1
                
                # Process the cast member
                updates = self.process_cast_member(row_data, row_num)
                
                # Add to batch if we found new data
                if updates:
                    self.add_to_batch(row_num, updates)
                else:
                    self.skipped_count += 1
                
                # Add delay to avoid rate limiting
                time.sleep(0.5)
                
                # Progress update every 10 rows
                if self.processed_count % 10 == 0:
                    self.print_progress()
            
            # Process any remaining batch updates
            if self.batch_updates:
                print("\n📤 Processing final batch...")
                self.process_batch()
            
            # Final summary
            print("\n" + "="*60)
            print("🎉 Processing complete!")
            self.print_final_summary()
            
            return True
            
        except Exception as e:
            print(f"❌ Error processing range: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def print_progress(self):
        """Print progress statistics"""
        success_rate = (self.updated_count / self.processed_count * 100) if self.processed_count > 0 else 0
        
        print(f"\n📊 Progress Report ({self.processed_count} rows processed):")
        print(f"   • Updated: {self.updated_count} ({success_rate:.1f}%)")
        print(f"   • Skipped: {self.skipped_count}")
        print(f"   • In batch queue: {len(self.batch_updates)}")
    
    def print_final_summary(self):
        """Print final summary with source breakdown"""
        success_rate = (self.updated_count / self.processed_count * 100) if self.processed_count > 0 else 0
        
        print(f"\n📊 Final Statistics:")
        print(f"   • Total processed: {self.processed_count}")
        print(f"   • Successfully updated: {self.updated_count}")
        print(f"   • Skipped (no new data): {self.skipped_count}")
        print(f"   • Success rate: {success_rate:.1f}%")
        
        if self.sources_used:
            print(f"\n📚 Data sources breakdown:")
            for source, count in sorted(self.sources_used.items(), key=lambda x: x[1], reverse=True):
                percentage = (count / sum(self.sources_used.values()) * 100)
                print(f"   • {source}: {count} ({percentage:.1f}%)")


def main():
    """Main function"""
    print("🚀 Starting Reality TV Data Scraper for RealiteaseInfo...")
    print("📝 This tool will enhance your cast data with:")
    print("   • Gender (Column H) - with enhanced pronoun analysis")
    print("   • Birthday (Column I)")
    print("   • Zodiac Sign (Column J) - auto-calculated from birthday")
    print("\n✨ Features:")
    print("   • Enhanced gender detection from bios and descriptions")
    print("   • Automatic zodiac calculation")
    print("   • Batch updates every 25 rows for efficiency")
    print("\n📚 Data sources:")
    print("   1. Fandom Wikis (show-specific)")
    print("   2. Famous Birthdays")
    print("   3. Wikipedia/Wikidata")
    print("   4. IMDb")
    print("   5. Google Search (with link following)")
    print("   6. Advanced text analysis for gender\n")
    
    scraper = FamousBirthdaysEnhancer()
    
    success = scraper.process_range()
    
    if success:
        print("\n✅ Data enhancement completed successfully!")
    else:
        print("\n❌ Data enhancement failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()