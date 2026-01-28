from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import threading
import re
import csv
import os
import time
from datetime import datetime


class UltimateFindchipsScraper:
    """
    Supply chain data scraper for Findchips.com electronic components.
    Extracts MPNs, pricing, inventory, and distributor information across categories.
    Supports parallel processing with thread-safe data collection and auto-save.
    """
    
    def __init__(self):
        self.all_parts = []
        self.visited_urls = set()
        self.lock = threading.Lock()
        
        # CSV column headers for supply chain data
        self.CSV_HEADERS = [
            'MPN', 'Price_Qty', 'Unit_Price', 'MFG_Name', 'Supplier_Name',
            'MFG_Lead_Time', 'On_Hand_Stock', 'Stock_Per_Price_Break',
            'Packaging_Type', 'Date_Code', 'COO', 'MOQ', 'Currency',
            'Main_Category', 'Distributor_Block', 'Disti_Part_Number', 'Region'
        ]
        self.save_filename = None
        self.save_running = False

    def add_parts_threadsafe(self, new_parts):
        """Append new parts to shared data structure in thread-safe manner."""
        with self.lock:
            for part in new_parts:
                if part['MPN'] and part['Supplier_Name']:
                    self.all_parts.append(part)

    def clean_price_text(self, price_text):
        """
        Normalize multi-line price text to standard currency-numeric format.
        
        Args:
            price_text (str): Raw price text from webpage
            
        Returns:
            str: Cleaned price in format "$3.25" or empty string
        """
        if not price_text or price_text.strip() == '':
            return ''
        
        # Remove whitespace and extract numeric portion
        cleaned = re.sub(r'\s+', '', price_text.strip())
        price_match = re.search(r'[\$€£¥₹¢]?([0-9,]+\.?[0-9]*)', cleaned)
        if price_match:
            numeric_price = price_match.group(1).replace(',', '')
            currency_symbol = re.search(r'[\$€£¥₹¢]', cleaned)
            if currency_symbol:
                return f"{currency_symbol.group(0)}{numeric_price}"
            return numeric_price
        
        return cleaned[:50]

    def setup_driver(self):
        """
        Configure Chrome WebDriver with stealth options to avoid detection.
        
        Returns:
            WebDriver: Configured Chrome driver instance
        """
        options = Options()
        options.add_argument('--start-maximized')
        options.add_argument('--disable-web-security')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-blink-features=AutomationControlled')
        
        driver = webdriver.Chrome(options=options)
        driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        return driver

    def clean_category_name(self, cat_text):
        """Remove numbers and parametric search text from category names."""
        clean = re.sub(r'[\d,]+', '', cat_text).strip()
        clean = re.sub(r'/Parametric Search.*', '', clean).strip()
        if '/' in clean:
            clean = clean.split('/')[0].strip()
        return clean[:50]

    def get_main_category_only(self, category_path):
        """
        Extract primary category from full hierarchical path.
        
        Args:
            category_path (str): Full path like "Components/Connectors/Headers"
            
        Returns:
            str: Main category name
        """
        parts = [p.strip() for p in category_path.split('/') if p.strip()]
        if not parts:
            return ''
        if parts[0].lower() == 'components' and len(parts) > 1:
            return parts[1]
        return parts[0]

    def get_all_main_categories(self, driver):
        """
        Discover all main parametric categories from Findchips parametric page.
        
        Args:
            driver (WebDriver): Selenium WebDriver instance
            
        Returns:
            list: List of (url, category_name) tuples
        """
        main_categories = []
        try:
            driver.get("https://www.findchips.com/parametric")
            time.sleep(8)

            cat_selectors = [
                "a[href*='/parametric/']", "[href*='/parametric/']",
                ".category a", ".cat-link", ".parametric-category a",
                "a[href*='findchips.com/parametric']",
                "[data-category] a", "nav a[href*='/parametric/']"
            ]

            all_cats = {}
            for selector in cat_selectors:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    for elem in elements:
                        href = elem.get_attribute('href')
                        text = elem.text.strip()
                        if href and '/parametric/' in href and len(text) > 1:
                            clean_name = self.clean_category_name(text)
                            if len(clean_name) > 1 and clean_name not in all_cats:
                                if not any(x in clean_name.lower() 
                                         for x in ['search', 'page', 'sub']):
                                    all_cats[clean_name] = href
                                    print(f"Category found: {clean_name}")
                except Exception:
                    continue

            main_categories = [(url, name) for name, url in all_cats.items()]
            print(f"Discovered {len(main_categories)} main categories")
            
        except Exception as e:
            print(f"Category discovery error: {str(e)}")

        return main_categories

    def parse_category_mfg(self, category_path):
        """
        Parse category path to extract category and potential manufacturer.
        
        Args:
            category_path (str): Path like "Connectors/TE Connectivity"
            
        Returns:
            tuple: (category_name, manufacturer_name)
        """
        if '/' in category_path and len(category_path.split('/')) > 1:
            parts = category_path.rsplit('/', 1)
            category = self.clean_category_name(parts[0])
            mfg_candidate = parts[1].strip()
            mfg_candidate = re.sub(r'[^\w\s&\-]', '', mfg_candidate)
            if len(mfg_candidate) > 2 and not mfg_candidate.startswith('Page'):
                return category, mfg_candidate
        return self.clean_category_name(category_path), ''

    def get_currency_from_price(self, price_text):
        """
        Detect currency symbol from price text.
        
        Args:
            price_text (str): Raw price text
            
        Returns:
            str: Currency code (USD, EUR, GBP, etc.)
        """
        if not price_text:
            return 'USD'
        if '¥' in price_text or 'CNY' in price_text:
            return 'CNY'
        if '€' in price_text:
            return 'EUR'
        if '£' in price_text or 'GBP' in price_text:
            return 'GBP'
        if '₹' in price_text or 'INR' in price_text:
            return 'INR'
        if '$' in price_text:
            return 'USD'
        return 'USD'

    def clean_mfg_name(self, mfg_text):
        """
        Validate and clean manufacturer names with strict filtering.
        
        Filters out UI elements, search terms, and invalid patterns.
        Ensures only legitimate manufacturer names are accepted.
        
        Args:
            mfg_text (str): Raw manufacturer text from page
            
        Returns:
            str: Cleaned manufacturer name or empty string
        """
        if not mfg_text or len(mfg_text.strip()) < 2:
            return ''
            
        # Block search/UI pollution keywords
        search_keywords = [
            'parametric', 'search', 'filter', 'category', 'browse',
            'results', 'view', 'find', 'parts', 'list', 'data',
            'select', 'sponsored', 'alert', 'loading', 'page'
        ]
        low_text = mfg_text.lower()
        if any(kw in low_text for kw in search_keywords):
            return ''
        
        # Clean and validate manufacturer pattern
        cleaned = re.sub(r'[^\w\s&\-]', '', mfg_text.strip())
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        # Strict manufacturer validation: Capitalized letters only, no numbers
        if (len(cleaned) >= 2 and 
            re.match(r'^[A-Z][A-Za-z\s&\-]{1,28}[A-Za-z]?$', cleaned) and 
            not re.search(r'\d', cleaned)):
            return cleaned[:30]
        
        return ''

    def _page_has_no_manufacturers_message(self, driver):
        """Check if page displays 'no manufacturers found' message."""
        try:
            body = driver.find_element(By.TAG_NAME, "body").text
        except Exception:
            return False
        patterns = [
            r'There are no manufacturers found for',
            r'No results found',
            r'No manufacturer.*found',
        ]
        return any(re.search(p, body, re.I) for p in patterns)

    def _extract_mpns_from_detail_links(self, driver):
        """
        Extract MPN candidates from detail page links.
        
        Prioritizes URL path extraction over link text.
        Handles bracketed MPNs and URL encoding.
        """
        mpns = set()
        try:
            links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/detail/']")
            for a in links:
                href = a.get_attribute("href") or ""
                text = a.text.strip()
                
                # Priority 1: Extract from URL path
                m = re.search(r'/detail/([^/?#]+)', href)
                if m:
                    candidate = m.group(1).strip()
                else:
                    candidate = text

                # Clean and validate MPN format
                candidate = re.sub(r'^\[|\]$', '', candidate)
                candidate = re.sub(r'[^\w\-/,:]', '', candidate).upper()
                
                if 6 <= len(candidate) <= 40 and re.search(r'[0-9]', candidate):
                    mpns.add(candidate)
        except Exception:
            pass
        return list(mpns)

    def _extract_mpns_from_text(self, driver):
        """
        Extract MPN candidates from page body text using multiple regex patterns.
        Preserves critical characters like / and , in part numbers.
        """
        mpns = []
        try:
            page_text = driver.find_element(By.TAG_NAME, "body").text
            
            mpn_patterns = [
                r'\b(?:\[)?([A-Z]{2,6}[A-Z0-9\-/,:]{3,30})(?:\])?\b',
                r'([A-Z]{3,}[A-Z0-9\-/,:]{4,30})\s+by[:\s]',
                r'([A-Z]{2,8}[A-Z0-9\-/,:]{4,35})',
            ]
            
            all_found = []
            for pattern in mpn_patterns:
                found = re.findall(pattern, page_text, re.I)
                all_found.extend(found)
            
            for mpn in all_found:
                clean_mpn = re.sub(r'[^\w\-/,:]', '', mpn).upper()
                if (6 <= len(clean_mpn) <= 40 and re.search(r'[0-9]', clean_mpn) and
                    not clean_mpn.isalpha() and clean_mpn not in mpns):
                    mpns.append(clean_mpn)
        except Exception:
            pass
        return mpns

    def find_real_mpns(self, driver):
        """
        Validate and extract legitimate MPNs from category pages.
        
        Multi-stage validation using detail links, page title, and page text.
        Eliminates false positives through cross-verification.
        """
        if self._page_has_no_manufacturers_message(driver):
            print("  No manufacturers - skipping MPN extraction")
            return []

        detail_mpns = self._extract_mpns_from_detail_links(driver)
        text_mpns = self._extract_mpns_from_text(driver)
        all_candidates = list(set(detail_mpns + text_mpns))
        
        valid_mpns = []
        print(f"  Validating {len(all_candidates)} MPN candidates")
        
        try:
            page_text = driver.find_element(By.TAG_NAME, "body").text
            page_title = driver.title
        except:
            page_text = ""
            page_title = ""

        # Cross-validate candidates against page elements
        for candidate in all_candidates:
            try:
                detail_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/detail/']")
                for link in detail_links:
                    href = link.get_attribute('href') or ''
                    encoded_candidate = candidate.replace('/', '%2F')
                    if (candidate in href or 
                        encoded_candidate in href or
                        encoded_candidate.upper() in href.upper()):
                        valid_mpns.append(candidate)
                        print(f"  DETAIL-LINK: {candidate}")
                        break
                else:
                    if candidate.upper() in page_title.upper():
                        valid_mpns.append(candidate)
                        print(f"   TITLE: {candidate}")
                    elif candidate.upper() in page_text.upper():
                        valid_mpns.append(candidate)
                        print(f"   PAGE-TEXT: {candidate}")
                        
            except Exception:
                continue
        
        valid_mpns = list(set(valid_mpns))
        print(f"  Validated {len(valid_mpns)} MPNS")
        return valid_mpns

    def _extract_real_manufacturer(self, driver, mpn):
        """
        Multi-layer manufacturer extraction with strict validation.
        
        Layer 1: Detail link table rows
        Layer 2: H1 title patterns  
        Layer 3: Manufacturer-specific CSS selectors
        """
        try:
            # Layer 1: Detail links (most reliable)
            detail_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/detail/']")
            for a in detail_links:
                if mpn.upper() in (a.text or '').upper():
                    parent = a.find_element(By.XPATH, "./ancestor::tr[1]")
                    mfg_cand = self.clean_mfg_name(parent.text)
                    if mfg_cand:
                        print(f"      MFG(Detail): {mfg_cand}")
                        return mfg_cand
            
            # Layer 2: H1 title patterns
            try:
                h1 = driver.find_element(By.TAG_NAME, "h1").text
                patterns = [
                    r'([A-Z][A-Za-z\s&\-]{2,30})(?:\s+by|\s+from|\s*[-|])',
                    r'by[:\s]*([A-Z][A-Za-z\s&\-]{2,30})',
                ]
                for pat in patterns:
                    m = re.search(pat, h1, re.I)
                    if m:
                        cleaned = self.clean_mfg_name(m.group(1))
                        if cleaned:
                            print(f"      MFG(H1): {cleaned}")
                            return cleaned
            except:
                pass
            
        except Exception:
            pass
        
        return ''

    def _clean_country(self, text):
        """Clean and validate country of origin text."""
        if not text:
            return ''

        txt = text.strip()
        # Remove trailing punctuation and supply chain keywords
        txt = re.sub(r'[,.;:]+$', '', txt).strip()
        txt = re.sub(r'\b(MIN\s*QTY|MOQ|ROHS?|LF|LEAD\s*FREE|STD|STOCK|QTY|PCS?)\b.*$', 
                    '', txt, flags=re.I)
        
        # Block UI pollution
        garbage_keywords = ['cookies', 'cookie', 'tracking', 'terms', 'policy']
        if any(g in txt.lower() for g in garbage_keywords):
            return ''

        # Standardize common countries
        low = txt.lower()
        if low in ['us', 'usa', 'u.s.a', 'u.s.']:
            return 'USA'
        if low in ['uk', 'united kingdom']:
            return 'United Kingdom'

        # Extract final country name and clean
        parts = re.split(r'[/|(),]+', txt)
        parts = [p.strip() for p in parts if p.strip()]
        if parts:
            txt = parts[-1].strip()

        txt = re.sub(r'[^a-zA-Z\s]', '', txt).strip()
        txt = re.sub(r'\s+', ' ', txt).strip()

        return txt if 2 <= len(txt) <= 40 and not txt.isnumeric() else ''

    def _get_enhanced_packaging_type(self, row_text):
        """
        Extract packaging type with priority matching for specific formats.
        Prioritizes specific packaging over generic 'Container'.
        """
        specific_pack_patterns = [
            r'Bulk', r'Tray', r'Reel', r'Tape', r'Cut Tape', r'Each',
            r'Bag', r'Box', r'Tube', r'Rail', r'Ammo Pack|Ammo',
            r'Carrier Tape', r'Digi-Reel|MouseReel', r'Cut Strip',
            r'Digi-Stake', r'Loose', r'Plastic Box', r'Anti-Static Bag|ESD'
        ]
        
        for pattern in specific_pack_patterns:
            match = re.search(pattern, row_text, re.I)
            if match:
                return match.group(0).strip()
        
        if 'Container' in row_text and not any(re.search(p, row_text, re.I) 
                                             for p in specific_pack_patterns):
            return 'Container'
        
        return ''

    def extract_perfect_stock(self, tr, supplier_clean):
        """
        Extract stock quantity using multiple HTML attribute fallbacks.
        
        Priority: data-stock → td.td-stock → data-instock
        """
        stock_value = ''
        
        # Method 1: data-stock attribute
        try:
            data_stock = tr.get_attribute('data-stock')
            if data_stock and data_stock.strip().isdigit():
                stock_value = data_stock.strip()
                print(f"      Stock(data): {stock_value}")
                return stock_value
        except:
            pass
        
        # Method 2: td.td-stock class
        try:
            stock_td = tr.find_element(By.CSS_SELECTOR, "td.td-stock")
            stock_text = stock_td.text.strip()
            if stock_text and stock_text.isdigit():
                stock_value = stock_text
                print(f"      Stock(td): {stock_value}")
                return stock_value
        except:
            pass
        
        # Method 3: data-instock attribute
        try:
            data_instock = tr.get_attribute('data-instock')
            if data_instock and data_instock.strip().isdigit():
                stock_value = data_instock.strip()
                print(f"      Stock(instk): {stock_value}")
                return stock_value
        except:
            pass
        
        print(f"      No stock from {supplier_clean}")
        return ''

    def extract_rows_from_search_page(self, driver, mpn, category_path):
        """
        Extract all distributor pricing and inventory rows for specific MPN.
        
        Parses distributor blocks, price breaks, stock levels, and metadata.
        Applies manufacturer validation at multiple safety checkpoints.
        """
        rows_data = []

        # Parse category hierarchy
        full_category, mfg_from_category = self.parse_category_mfg(category_path)
        main_cat = self.get_main_category_only(full_category)

        # Extract manufacturer with validation
        page_mfg = self._extract_real_manufacturer(driver, mpn)
        mfg_name = page_mfg or mfg_from_category or ''
        
        if any(pollute in mfg_name.lower() for pollute in ['parametric', 'search']):
            print(f"     Blocked MFG pollution for {mpn}")
            mfg_name = ''

        try:
            rows = driver.find_elements(By.CSS_SELECTOR, "tr.row[data-distributor_name]")
            print(f"     Found {len(rows)} distributor rows")
        except Exception:
            return rows_data

        # Process each distributor row
        for tr_idx, tr in enumerate(rows):
            try:
                distributor_name = tr.get_attribute('data-distributor_name') or ''
                if not distributor_name:
                    continue

                supplier_clean = self.clean_mfg_name(distributor_name)
                if len(supplier_clean) < 2:
                    continue

                # Initialize base record
                base_part = {field: '' for field in self.CSV_HEADERS}
                base_part['MPN'] = mpn
                base_part['MFG_Name'] = mfg_name
                base_part['Main_Category'] = main_cat
                base_part['Supplier_Name'] = supplier_clean
                base_part['Distributor_Block'] = distributor_name

                # Extract stock and metadata
                stock_value = self.extract_perfect_stock(tr, supplier_clean)
                base_part['On_Hand_Stock'] = stock_value
                base_part['Stock_Per_Price_Break'] = stock_value

                # Distributor-specific fields
                disti_match = re.search(r'DISTI\s*#?\s*([A-Za-z0-9\-\:_]+)', tr.text)
                if disti_match:
                    base_part['Disti_Part_Number'] = disti_match.group(1)

                region_match = re.search(r'(Americas|Europe|Asia|Global)\s*[-—]\s*\d+', tr.text, re.I)
                if region_match:
                    base_part['Region'] = region_match.group(1)

                row_text = tr.text
                base_part['Packaging_Type'] = self._get_enhanced_packaging_type(row_text)

                # Additional metadata extraction
                lead_match = re.search(r'(\d+)\s*(?:weeks?|days?)\b', row_text, re.I)
                if lead_match:
                    base_part['MFG_Lead_Time'] = f"{lead_match.group(1)} weeks"

                date_match = re.search(r'(?:Date Code|DC|Lot)[:\s]*(\d{4}|\d{2}\d{2})', row_text, re.I)
                if date_match:
                    base_part['Date_Code'] = date_match.group(1)

                moq_match = re.search(r'(?:MOQ|Min\s+Qty?)[:\s]*(\d{1,5}(?:,\d{3})?)', row_text, re.I)
                if moq_match:
                    base_part['MOQ'] = moq_match.group(1)

                coo_match = re.search(r'COO[:\s]*([A-Za-z\s]{2,40})', row_text, re.I)
                if coo_match:
                    base_part['COO'] = self._clean_country(coo_match.group(1))

                # Extract price breaks
                price_lis = tr.find_elements(By.CSS_SELECTOR, "td.td-price ul.price-list li")
                if price_lis:
                    for li in price_lis:
                        try:
                            qty_el = li.find_element(By.CSS_SELECTOR, ".label")
                            price_el = li.find_element(By.CSS_SELECTOR, ".value")
                            qty_text = qty_el.text.strip()
                            raw_price_text = price_el.text.strip()
                            
                            clean_price = self.clean_price_text(raw_price_text)
                            qty_num = re.sub(r'[^\d]', '', qty_text)
                            
                            if qty_num and clean_price:
                                rcp = base_part.copy()
                                rcp['Price_Qty'] = qty_num
                                rcp['Unit_Price'] = clean_price
                                rcp['Currency'] = self.get_currency_from_price(raw_price_text)
                                
                                # Final validation
                                if any(pollute in rcp['MFG_Name'].lower() 
                                     for pollute in ['parametric', 'search']):
                                    print(f"     Final block: {mpn}")
                                else:
                                    rows_data.append(rcp)
                        except:
                            continue
                    continue

                # Fallback single row without price breaks
                if base_part['Supplier_Name'] and stock_value:
                    rows_data.append(base_part)

            except Exception as e:
                print(f"     Row {tr_idx} error: {str(e)[:50]}")
                continue

        print(f"     Extracted {len(rows_data)} rows for {mpn}")
        return rows_data

    def scrape_category_tree(self, driver, url, category_path="Components"):
        """
        Recursively scrape category tree, extracting MPNs and distributor data.
        
        Visits subcategories and individual MPN search pages.
        Implements visited URL tracking to prevent cycles.
        """
        if url in self.visited_urls or len(self.all_parts) > 500000:
            return

        self.visited_urls.add(url)
        print(f"\nScraping [{len(self.visited_urls)}] {category_path}")

        try:
            driver.get(url)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(5)

            # Extract MPNs from current category page
            mpns = self.find_real_mpns(driver)
            print(f"  Processing {len(mpns)} MPNS")

            # Process each MPN
            for i, mpn in enumerate(mpns):
                print(f"  [{i+1}/{len(mpns)}] {mpn}")
                try:
                    search_url = f"https://www.findchips.com/search/{mpn}"
                    driver.get(search_url)
                    time.sleep(6)

                    if self._page_has_no_manufacturers_message(driver):
                        print(f"     {mpn}: no manufacturers, skipping")
                        time.sleep(1)
                        continue

                    rows = self.extract_rows_from_search_page(driver, mpn, category_path)
                    self.add_parts_threadsafe(rows)
                    print(f"     {mpn}: added {len(rows)} rows")
                    time.sleep(2)

                except Exception as e:
                    print(f"     {mpn}: {str(e)[:40]}")
                    time.sleep(1)
                    continue

            # Recurse into subcategories
            subcats = driver.find_elements(By.CSS_SELECTOR, "a[href*='/parametric/']")
            print(f"  Found {len(subcats)} subcategories")

            for i, subcat in enumerate(subcats):
                try:
                    href = subcat.get_attribute('href')
                    text = subcat.text.strip()
                    if (href and '/parametric/' in href and len(text) > 2
                        and href not in self.visited_urls):
                        new_path = f"{category_path}/{self.clean_category_name(text)}"
                        print(f"   → [{i+1}/{len(subcats)}] {self.clean_category_name(text)}")
                        self.scrape_category_tree(driver, href, new_path)
                except:
                    continue

        except Exception as e:
            print(f"Error in {category_path}: {str(e)[:40]}")

    def setup_auto_save(self):
        """Initialize background auto-save thread (every 5 minutes)."""
        self.save_filename = f"output/stock_{datetime.now().strftime('%Y%m%d')}.csv"
        os.makedirs("output", exist_ok=True)
        self.save_running = True
        self.save_thread = threading.Thread(target=self.auto_save_worker, daemon=True)
        self.save_thread.start()
        print(f"Auto-save enabled: {self.save_filename}")

    def auto_save_worker(self):
        """Background thread for periodic CSV saves."""
        while self.save_running:
            time.sleep(300)  # 5 minutes
            if self.all_parts and self.save_filename and self.save_running:
                self.save_csv()
                print(f"Saved {len(self.all_parts):,} parts")

    def save_csv(self):
        """
        Save collected data to CSV with thread-safe deduplication and cleaning.
        Appends new records only, maintains column alignment.
        """
        if not self.all_parts or not self.save_filename:
            return

        with self.lock:
            # Filter unsaved records
            new_parts = [p for p in self.all_parts if 'scrape_time' not in p]
            if not new_parts:
                return

            # Clean and standardize data
            for part in new_parts:
                for field in self.CSV_HEADERS + ['scrape_time']:
                    if field not in part:
                        part[field] = ''
                    value = str(part[field])
                    value = re.sub(r'[\r\n\t]+', ' ', value)
                    value = re.sub(r'\s+', ' ', value).strip()
                    part[field] = value[:100]
                part['scrape_time'] = datetime.now().strftime('%Y-%m-%d %H:%M')

            # Append to CSV
            file_exists = os.path.exists(self.save_filename)
            try:
                with open(self.save_filename, 'a', newline='', encoding='utf-8-sig') as f:
                    writer = csv.DictWriter(
                        f,
                        fieldnames=self.CSV_HEADERS + ['scrape_time'],
                        quoting=csv.QUOTE_MINIMAL,
                        lineterminator='\n'
                    )
                    if not file_exists:
                        writer.writeheader()
                    writer.writerows(new_parts)
                print(f"Saved {len(new_parts)} new records → Total: {len(self.all_parts):,}")
            except Exception as e:
                print(f"Save error: {str(e)[:50]}")

    def worker_thread(self, thread_id, categories_subset):
        """
        Individual worker thread for parallel category processing.
        
        Each thread maintains independent WebDriver instance.
        """
        print(f"Thread #{thread_id} started - {len(categories_subset)} categories")
        
        driver = self.setup_driver()
        try:
            for i, (href, cat_name) in enumerate(categories_subset, 1):
                print(f"[T{thread_id}-{i}/{len(categories_subset)}] {cat_name}")
                self.scrape_category_tree(driver, href, cat_name)
                with self.lock:
                    print(f"  Thread #{thread_id}: {len(self.all_parts):,} total parts")
                    
        except Exception as e:
            print(f"Thread #{thread_id} error: {str(e)}")
        finally:
            driver.quit()
            print(f"Thread #{thread_id} completed")

    def run_parallel(self):
        """
        Execute parallel scraping across 6 threads.
        Discovers categories → divides workload → coordinates threads → final save.
        """
        print("Findchips Supply Chain Scraper - Starting parallel execution")
        self.setup_auto_save()
        
        # Discover categories with single driver
        driver = self.setup_driver()
        main_cats = self.get_all_main_categories(driver)
        driver.quit()
        
        if len(main_cats) == 0:
            print("No categories found!")
            return
        
        # Divide categories across 6 threads
        categories_per_thread = len(main_cats) // 6
        threads_categories = [
            main_cats[:categories_per_thread],
            main_cats[categories_per_thread:2*categories_per_thread],
            main_cats[2*categories_per_thread:3*categories_per_thread],
            main_cats[3*categories_per_thread:4*categories_per_thread],
            main_cats[4*categories_per_thread:5*categories_per_thread],
            main_cats[5*categories_per_thread:]
        ]
        
        print(f"Workload divided (6 threads): {[len(x) for x in threads_categories]} categories")
        
        # Launch parallel threads
        threads = []
        for i, subset in enumerate(threads_categories, 1):
            t = threading.Thread(target=self.worker_thread, args=(i, subset))
            t.daemon = True
            t.start()
            threads.append(t)
        
        # Wait for completion
        for t in threads:
            t.join()
        
        time.sleep(2)
        self.save_running = False
        self.save_csv()
        print(f"Complete! Total parts: {len(self.all_parts):,}")
        print(f"Data saved to: {self.save_filename}")


if __name__ == "__main__":
    scraper = UltimateFindchipsScraper()
    scraper.run_parallel()
