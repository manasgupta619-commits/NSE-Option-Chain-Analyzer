import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
from scipy.stats import norm
import numpy as np
import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from io import StringIO 

# --- CONFIGURATION ---
SHEET_NAME = 'Live Option Chain'
JSON_KEYFILE = 'credentials.json'
SYMBOL = 'NIFTY' 
REFRESH_SECONDS = 60
HEADLESS_MODE = False 

# --- GOOGLE SHEETS AUTHENTICATION ---
def connect_to_sheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEYFILE, scope)
    client = gspread.authorize(creds)
    sheet = client.open(SHEET_NAME).worksheet("RawData") # Ensuring we write to RawData tab
    return sheet

# --- BLACK-SCHOLES DELTA CALCULATION ---
def calculate_delta(S, K, T, r, sigma, option_type):
    if T <= 0 or sigma == 0:
        return 0
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    if option_type == 'CE':
        return norm.cdf(d1)
    return norm.cdf(d1) - 1

# --- BROWSER CYCLE (SUPER STEALTH MODE) ---
def fetch_live_data():
    print("   -> Launching fresh Chrome session (Stealth Mode)...")
    options = Options()
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    if HEADLESS_MODE:
        options.add_argument("--headless")
    
    # 1. REMOVE AUTOMATION FLAGS (Crucial for bypassing blocks)
    options.add_argument("--disable-blink-features=AutomationControlled") 
    options.add_experimental_option("excludeSwitches", ["enable-automation"]) 
    options.add_experimental_option("useAutomationExtension", False) 
    
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    # 2. OVERWRITE NAVIGATOR PROPERTY
    # This executes JavaScript to hide the "webdriver" property that tells websites you are a bot.
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {
              get: () => undefined
            })
        """
    })
    
    try:
        url = f"https://www.nseindia.com/option-chain"
        print(f"   -> Loading {url}...")
        driver.get(url)

        try:
            # Increased timeout to 30s and added a retry logic message
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.ID, "optionChainTable-indices"))
            )
            print("   -> Table detected!")
        except:
            print("   [Error] Table load timeout. NSE might be slow or blocking.")
            # Debug: Screenshot or check page source could go here
            return None, None

        try:
            spot_element = driver.find_element(By.ID, "equity_underlyingVal")
            spot_text = spot_element.text.replace("Underlying Index: ", "").replace("NIFTY", "").strip()
            spot_price = float(spot_text.split(" ")[0].replace(",", ""))
        except:
            spot_price = 0

        table_element = driver.find_element(By.ID, "optionChainTable-indices")
        table_html = table_element.get_attribute('outerHTML')
        
        dfs = pd.read_html(StringIO(table_html))
        raw_df = dfs[0]
        
        return raw_df, spot_price

    except Exception as e:
        print(f"   [Browser Error] {e}")
        return None, None

    finally:
        driver.quit()

# --- MAIN LOGIC ---
def update_dashboard():
    print(f"\n[{datetime.datetime.now().strftime('%H:%M:%S')}] Starting Update Cycle...")
    
    df, spot_price = fetch_live_data()
    
    if df is None:
        print("Retrying next cycle...")
        return
    
    print(f"   -> Data Fetched! Spot Price: {spot_price}")
    print(f"   -> Processing {len(df)} rows...")
    
    try:
        clean_data = []
        T = 4.0 / 365.0 
        
        for index, row in df.iterrows():
            if index < 2: continue 
            
            try:
                def parse(val):
                    if str(val).strip() == '-' or str(val).strip() == '': return 0
                    return float(str(val).replace(',', ''))

                # EXTRACT ALL COLUMNS
                c_oi = parse(row[1])
                c_chng_oi = parse(row[2])
                c_vol = parse(row[3])
                c_iv = parse(row[4])
                c_ltp = parse(row[5])
                c_chng = parse(row[6])
                c_bid_qty = parse(row[7])
                c_bid = parse(row[8])
                c_ask = parse(row[9])
                c_ask_qty = parse(row[10])
                
                strike = parse(row[11])
                
                p_bid_qty = parse(row[12])
                p_bid = parse(row[13])
                p_ask = parse(row[14])
                p_ask_qty = parse(row[15])
                p_chng = parse(row[16])
                p_ltp = parse(row[17])
                p_iv = parse(row[18])
                p_vol = parse(row[19])
                p_chng_oi = parse(row[20])
                p_oi = parse(row[21])

                # Greeks
                c_delta = calculate_delta(spot_price, strike, T, 0.10, c_iv/100, 'CE')
                p_delta = calculate_delta(spot_price, strike, T, 0.10, p_iv/100, 'PE')
                
                clean_data.append([
                    c_oi, c_chng_oi, c_vol, c_iv, round(c_delta, 2), c_ltp, c_chng, c_bid_qty, c_bid, c_ask, c_ask_qty,
                    strike,
                    p_bid_qty, p_bid, p_ask, p_ask_qty, p_chng, p_ltp, p_iv, round(p_delta, 2), p_vol, p_chng_oi, p_oi
                ])
            except Exception:
                continue 

        headers = [
            'Call OI', 'Call Chng OI', 'Call Vol', 'Call IV', 'Call Delta', 'Call LTP', 'Call Chng', 'Call Bid Qty', 'Call Bid', 'Call Ask', 'Call Ask Qty',
            'Strike Price',
            'Put Bid Qty', 'Put Bid', 'Put Ask', 'Put Ask Qty', 'Put Chng', 'Put LTP', 'Put IV', 'Put Delta', 'Put Vol', 'Put Chng OI', 'Put OI'
        ]

        final_df = pd.DataFrame(clean_data, columns=headers)
        
        # Push to Google Sheet (RawData Tab)
        sheet = connect_to_sheet()
        sheet.clear()
        
        sheet.append_row([
            f"Symbol: {SYMBOL}", 
            f"Mode: Stealth Scraper", 
            f"Spot: {spot_price}", 
            f"Last Updated: {datetime.datetime.now().strftime('%H:%M:%S')}"
        ])
        
        sheet.append_row(final_df.columns.tolist())
        sheet.update('A3', final_df.values.tolist())
        print(f"Dashboard Updated Successfully with {len(final_df)} rows!")
        
    except Exception as e:
        print(f"Processing Error: {e}")

if __name__ == "__main__":
    print(f"Script Started. Press Ctrl+C to stop.")
    try:
        while True:
            update_dashboard()
            print(f"Waiting {REFRESH_SECONDS} seconds...")
            time.sleep(REFRESH_SECONDS)
    except KeyboardInterrupt:
        print("Script Stopped.")