import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
import re
import json
import os
from io import BytesIO
import zipfile
import io as io_module
from pathlib import Path
import pickle
import tempfile
import hashlib
from datetime import datetime, timedelta
# -------------------------------
# PAGE CONFIG
# -------------------------------
st.set_page_config(page_title="CFTC COT Data Analyzer", layout="wide")
st.title("📊 CFTC Commitments of Traders (COT) - Institutional Positioning")
st.markdown("---")

# -------------------------------
# DATA STORAGE SETUP
# -------------------------------
DATA_DIR = Path("cftc_data_store")
DATA_DIR.mkdir(exist_ok=True)

EXCEL_STORE_PATH = DATA_DIR / "cot_master_store.xlsx"
JSON_STORE_PATH = DATA_DIR / "cot_historical_data.json"
BACKUP_EXCEL_PATH = DATA_DIR / "cot_backup_data.xlsx"
# PERSISTENT STORAGE LOCATIONS - Add these!
TEMP_STORE_PATH = Path(tempfile.gettempdir()) / "cftc_data_store"
TEMP_STORE_PATH.mkdir(exist_ok=True)

TEMP_JSON_PATH = TEMP_STORE_PATH / "cot_historical_data.json"
TEMP_PICKLE_PATH = TEMP_STORE_PATH / "cot_data.pkl"

# Session ID for tracking instances
if 'instance_id' not in st.session_state:
    st.session_state.instance_id = hashlib.md5(str(datetime.now()).encode()).hexdigest()[:8]
def init_session_state():
    """Initialize all session state variables"""
    if 'markets_df' not in st.session_state:
        st.session_state.markets_df = {}
    if 'last_fetch_date' not in st.session_state:
        st.session_state.last_fetch_date = None
    if 'extracted_data_count' not in st.session_state:
        st.session_state.extracted_data_count = 0
    if 'fetch_history' not in st.session_state:
        st.session_state.fetch_history = []
    if 'edit_mode' not in st.session_state:
        st.session_state.edit_mode = False
    if 'editable_df' not in st.session_state:
        st.session_state.editable_df = None
    if 'current_editing_market' not in st.session_state:
        st.session_state.current_editing_market = None
    # Toggle states for analysis sections
    if 'show_positioning' not in st.session_state:
        st.session_state.show_positioning = False
    if 'show_peak' not in st.session_state:
        st.session_state.show_peak = False
    if 'show_comparison' not in st.session_state:
        st.session_state.show_comparison = False
    if 'show_zones' not in st.session_state:
        st.session_state.show_zones = False
    if 'show_rsi' not in st.session_state:
        st.session_state.show_rsi = False
    if 'show_myfxbook' not in st.session_state:
        st.session_state.show_myfxbook = False
    if 'show_news' not in st.session_state:
        st.session_state.show_news = False
    if 'show_plan' not in st.session_state:
        st.session_state.show_plan = False

init_session_state()

# -------------------------------
# PEAK VOLUME VALUES FROM YOUR EXCEL FILES (HARDCODED)
# -------------------------------

PEAK_VOLUME_VALUES = {
    # ===== CURRENCIES =====
    'USD/CAD': {
        'peak_longs': 219989,
        'peak_shorts': 105403,
        'min_longs': 5203,
        'min_shorts': 1641,
        'has_peaks': True
    },
    'EUR/USD': {
        'peak_longs': 266078,
        'peak_shorts': 271608,
        'min_longs': 17040,
        'min_shorts': 8524,
        'has_peaks': True
    },
    'GBP/USD': {
        'peak_longs': 188489,
        'peak_shorts': 154332,
        'min_longs': None,
        'min_shorts': None,
        'has_peaks': True
    },
    'USD/JPY': {
        'peak_longs': 237488,
        'peak_shorts': 204008,
        'min_longs': None,
        'min_shorts': None,
        'has_peaks': True
    },
    'AUD/USD': {
        'peak_longs': 144966,
        'peak_shorts': 145745,
        'min_longs': 5922,
        'min_shorts': 0,
        'has_peaks': True
    },
    'USD/ZAR': {
        'peak_longs': None,
        'peak_shorts': None,
        'min_longs': None,
        'min_shorts': None,
        'has_peaks': False
    },
    'USD/MXN': {
        'peak_longs': None,
        'peak_shorts': None,
        'min_longs': None,
        'min_shorts': None,
        'has_peaks': False
    },
    'NZD/USD': {
        'peak_longs': None,
        'peak_shorts': None,
        'min_longs': None,
        'min_shorts': None,
        'has_peaks': False
    },
    'USD/BRL': {
        'peak_longs': None,
        'peak_shorts': None,
        'min_longs': None,
        'min_shorts': None,
        'has_peaks': False
    },
    'USD/CHF': {
        'peak_longs': None,
        'peak_shorts': None,
        'min_longs': None,
        'min_shorts': None,
        'has_peaks': False
    },
    
    # ===== METALS =====
    'XAU/USD': {
        'peak_longs': 408349,
        'peak_shorts': 222210,
        'min_longs': 97630,
        'min_shorts': 18549,
        'has_peaks': True
    },
    'XAG/USD': {
        'peak_longs': 131969,
        'peak_shorts': 152035,
        'min_longs': 41325,
        'min_shorts': 16172,
        'has_peaks': True
    },
    'COPPER/USD': {
        'peak_longs': None,
        'peak_shorts': None,
        'min_longs': None,
        'min_shorts': None,
        'has_peaks': False
    },
    'STEEL-HRC/USD': {
        'peak_longs': None,
        'peak_shorts': None,
        'min_longs': None,
        'min_shorts': None,
        'has_peaks': False
    },
    'LITHIUM/USD': {
        'peak_longs': None,
        'peak_shorts': None,
        'min_longs': None,
        'min_shorts': None,
        'has_peaks': False
    },
    
    # ===== ENERGIES =====
    'CRUDE OIL/USD': {
        'peak_longs': None,
        'peak_shorts': None,
        'min_longs': None,
        'min_shorts': None,
        'has_peaks': False
    },
    'NAT GAS/USD': {
        'peak_longs': None,
        'peak_shorts': None,
        'min_longs': None,
        'min_shorts': None,
        'has_peaks': False
    },
    
    # ===== AGRICULTURE =====
    'COFFEE/USD': {
        'peak_longs': None,
        'peak_shorts': None,
        'min_longs': None,
        'min_shorts': None,
        'has_peaks': False
    },
    'WHEAT SRW/USD': {
        'peak_longs': None,
        'peak_shorts': None,
        'min_longs': None,
        'min_shorts': None,
        'has_peaks': False
    },
    'WHEAT HRW/USD': {
        'peak_longs': None,
        'peak_shorts': None,
        'min_longs': None,
        'min_shorts': None,
        'has_peaks': False
    },
    
    # ===== CRYPTO =====
    'MICRO-BTC/USD': {
        'peak_longs': None,
        'peak_shorts': None,
        'min_longs': None,
        'min_shorts': None,
        'has_peaks': False
    }
}

# ============================================
# ENHANCED DATA EDITING & ROW MANAGEMENT
# ============================================

def add_new_row(market, new_date, new_longs, new_shorts):
    """
    Add a new row of data to a specific market
    Returns (success, message)
    """
    try:
        # Validate inputs
        if market not in st.session_state.markets_df:
            return False, f"Market {market} not found"
        
        # Convert date
        try:
            date_obj = pd.to_datetime(new_date)
        except:
            return False, "Invalid date format. Use YYYY-MM-DD"
        
        # Validate numbers
        try:
            longs = float(new_longs)
            shorts = float(new_shorts)
            if longs < 0 or shorts < 0:
                return False, "Longs and Shorts must be positive numbers"
        except:
            return False, "Longs and Shorts must be valid numbers"
        
        # Check for duplicate date
        df = st.session_state.markets_df[market]
        if date_obj in df['Date'].values:
            return False, f"Data for {new_date} already exists. Use edit instead."
        
        # Calculate derived values
        total = longs + shorts
        if total > 0:
            long_pct = (longs / total * 100)
            short_pct = (shorts / total * 100)
        else:
            long_pct = 0
            short_pct = 0
        net = longs - shorts
        
        # Create new row
        new_row = pd.DataFrame([{
            'Date': date_obj,
            'Longs': longs,
            'Shorts': shorts,
            'Total': total,
            'Long %': round(long_pct, 1),
            'Short %': round(short_pct, 1),
            'Net': net
        }])
        
        # Add to dataframe
        updated_df = pd.concat([df, new_row], ignore_index=True)
        updated_df = updated_df.sort_values('Date', ascending=True).reset_index(drop=True)
        
        # Update session state
        st.session_state.markets_df[market] = updated_df
        
        # Auto-save
        save_to_json()
        
        return True, f"✅ Added data for {new_date}"
        
    except Exception as e:
        return False, f"Error adding row: {str(e)}"

def edit_row(market, row_index, new_longs, new_shorts):
    """
    Edit an existing row of data
    Returns (success, message)
    """
    try:
        if market not in st.session_state.markets_df:
            return False, f"Market {market} not found"
        
        df = st.session_state.markets_df[market].copy()
        
        if row_index < 0 or row_index >= len(df):
            return False, f"Row index {row_index} out of range"
        
        # Validate numbers
        try:
            longs = float(new_longs)
            shorts = float(new_shorts)
            if longs < 0 or shorts < 0:
                return False, "Longs and Shorts must be positive numbers"
        except:
            return False, "Longs and Shorts must be valid numbers"
        
        # Update values
        df.loc[row_index, 'Longs'] = longs
        df.loc[row_index, 'Shorts'] = shorts
        
        # Recalculate derived columns
        total = longs + shorts
        df.loc[row_index, 'Total'] = total
        if total > 0:
            df.loc[row_index, 'Long %'] = round(longs / total * 100, 1)
            df.loc[row_index, 'Short %'] = round(shorts / total * 100, 1)
        else:
            df.loc[row_index, 'Long %'] = 0
            df.loc[row_index, 'Short %'] = 0
        df.loc[row_index, 'Net'] = longs - shorts
        
        # Update session state
        st.session_state.markets_df[market] = df
        
        # Auto-save
        save_to_json()
        
        return True, f"✅ Updated row {row_index + 1}"
        
    except Exception as e:
        return False, f"Error editing row: {str(e)}"

def delete_row(market, row_index):
    """
    Delete a row from a market
    Returns (success, message)
    """
    try:
        if market not in st.session_state.markets_df:
            return False, f"Market {market} not found"
        
        df = st.session_state.markets_df[market]
        
        if row_index < 0 or row_index >= len(df):
            return False, f"Row index {row_index} out of range"
        
        # Get date for message
        deleted_date = df.iloc[row_index]['Date'].strftime('%Y-%m-%d')
        
        # Delete row
        updated_df = df.drop(df.index[row_index]).reset_index(drop=True)
        
        # Update session state
        st.session_state.markets_df[market] = updated_df
        
        # Auto-save
        save_to_json()
        
        return True, f"✅ Deleted data for {deleted_date}"
        
    except Exception as e:
        return False, f"Error deleting row: {str(e)}"

def insert_missing_week(market, target_date):
    """
    Intelligently insert a missing week by interpolating between adjacent weeks
    """
    try:
        if market not in st.session_state.markets_df:
            return False, f"Market {market} not found"
        
        df = st.session_state.markets_df[market]
        date_obj = pd.to_datetime(target_date)
        
        # Check if date already exists
        if date_obj in df['Date'].values:
            return False, f"Data for {target_date} already exists"
        
        # Find surrounding dates
        all_dates = df['Date'].tolist()
        all_dates.sort()
        
        # Find closest dates before and after
        before_date = None
        after_date = None
        
        for d in all_dates:
            if d < date_obj:
                before_date = d
            if d > date_obj and after_date is None:
                after_date = d
        
        if before_date is None or after_date is None:
            return False, "Need data before AND after the missing week to interpolate"
        
        # Get data for surrounding weeks
        before_row = df[df['Date'] == before_date].iloc[0]
        after_row = df[df['Date'] == after_date].iloc[0]
        
        # Calculate days difference for interpolation
        total_days = (after_date - before_date).days
        days_from_before = (date_obj - before_date).days
        weight = days_from_before / total_days if total_days > 0 else 0.5
        
        # Interpolate values
        interpolated_longs = before_row['Longs'] + weight * (after_row['Longs'] - before_row['Longs'])
        interpolated_shorts = before_row['Shorts'] + weight * (after_row['Shorts'] - before_row['Shorts'])
        
        # Round to integers
        longs = round(interpolated_longs)
        shorts = round(interpolated_shorts)
        
        # Add the interpolated row
        success, message = add_new_row(market, target_date, longs, shorts)
        
        if success:
            return True, f"✅ Inserted interpolated data for {target_date} (based on {before_date.strftime('%Y-%m-%d')} and {after_date.strftime('%Y-%m-%d')})"
        else:
            return False, message
            
    except Exception as e:
        return False, f"Error interpolating week: {str(e)}"

def bulk_edit_mode(market):
    """
    Display bulk editing interface for a market
    """
    st.subheader(f"✏️ BULK EDIT: {market}")
    
    df = st.session_state.markets_df[market].copy()
    
    # Display current data
    st.write("Current Data (showing last 20 rows):")
    display_df = df.tail(20).copy()
    display_df['Date'] = display_df['Date'].dt.strftime('%Y-%m-%d')
    
    edited_df = st.data_editor(
        display_df[['Date', 'Longs', 'Shorts']],
        use_container_width=True,
        num_rows="dynamic",
        key=f"bulk_editor_{market}",
        column_config={
            "Date": st.column_config.DateColumn("Date", format="YYYY-MM-DD"),
            "Longs": st.column_config.NumberColumn("Longs", min_value=0, format="%d"),
            "Shorts": st.column_config.NumberColumn("Shorts", min_value=0, format="%d"),
        }
    )
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("💾 Save All Changes", key=f"bulk_save_{market}"):
            try:
                # Convert back to proper format
                edited_df['Date'] = pd.to_datetime(edited_df['Date'])
                
                # Merge with full dataset
                full_df = df.copy()
                for _, row in edited_df.iterrows():
                    mask = full_df['Date'] == row['Date']
                    if mask.any():
                        # Update existing
                        full_df.loc[mask, 'Longs'] = row['Longs']
                        full_df.loc[mask, 'Shorts'] = row['Shorts']
                    else:
                        # Add new
                        total = row['Longs'] + row['Shorts']
                        new_row = pd.DataFrame([{
                            'Date': row['Date'],
                            'Longs': row['Longs'],
                            'Shorts': row['Shorts'],
                            'Total': total,
                            'Long %': round(row['Longs']/total*100, 1) if total > 0 else 0,
                            'Short %': round(row['Shorts']/total*100, 1) if total > 0 else 0,
                            'Net': row['Longs'] - row['Shorts']
                        }])
                        full_df = pd.concat([full_df, new_row], ignore_index=True)
                
                full_df = full_df.sort_values('Date').drop_duplicates('Date', keep='last').reset_index(drop=True)
                st.session_state.markets_df[market] = full_df
                save_to_json()
                st.success("✅ All changes saved!")
                st.rerun()
                
            except Exception as e:
                st.error(f"Error saving: {str(e)}")
    
    with col2:
        if st.button("➕ Add Empty Row", key=f"add_empty_{market}"):
            # Add a blank row with today's date
            today = datetime.now().strftime('%Y-%m-%d')
            success, msg = add_new_row(market, today, 0, 0)
            if success:
                st.success(msg)
                st.rerun()
            else:
                st.error(msg)
    
    with col3:
        if st.button("❌ Cancel", key=f"cancel_bulk_{market}"):
            st.session_state.edit_mode = False
            st.rerun()

# -------------------------------
# DATA PERSISTENCE FUNCTIONS
# -------------------------------

def save_to_json():
    """Save all market data to MULTIPLE text-based formats (NO EXCEL NEEDED)"""
    data_to_save = {}
    for market, df in st.session_state.markets_df.items():
        data_to_save[market] = {
            'Date': df['Date'].dt.strftime('%Y-%m-%d').tolist(),
            'Longs': df['Longs'].tolist(),
            'Shorts': df['Shorts'].tolist(),
            'Total': df['Total'].tolist(),
            'Long %': df['Long %'].tolist(),
            'Short %': df['Short %'].tolist(),
            'Net': df['Net'].tolist()
        }
    
    # ===== FORMAT 1: JSON (Primary storage) =====
    with open(JSON_STORE_PATH, 'w') as f:
        json.dump(data_to_save, f, indent=2)
    
    # ===== FORMAT 2: Compressed JSON (smaller size) =====
    import gzip
    with gzip.open(JSON_STORE_PATH.with_suffix('.json.gz'), 'wt') as f:
        json.dump(data_to_save, f)
    
    # ===== FORMAT 3: CSV files (one per market) =====
    csv_dir = DATA_DIR / "csv_backup"
    csv_dir.mkdir(exist_ok=True)
    
    for market, df in st.session_state.markets_df.items():
        csv_path = csv_dir / f"{market.replace('/', '_')}.csv"
        df.to_csv(csv_path, index=False)
    
    # ===== FORMAT 4: Single combined CSV =====
    all_data = []
    for market, df in st.session_state.markets_df.items():
        temp_df = df.copy()
        temp_df['Market'] = market
        all_data.append(temp_df)
    
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df.to_csv(DATA_DIR / "all_markets_combined.csv", index=False)
    
    # ===== FORMAT 5: Text summary =====
    with open(DATA_DIR / "summary.txt", 'w') as f:
        f.write(f"CFTC COT Data Summary\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Markets: {len(st.session_state.markets_df)}\n")
        f.write("="*50 + "\n\n")
        
        for market, df in st.session_state.markets_df.items():
            f.write(f"\n{market}:\n")
            f.write(f"  Records: {len(df)}\n")
            f.write(f"  Latest: {df['Date'].max().strftime('%Y-%m-%d')}\n")
            f.write(f"  Oldest: {df['Date'].min().strftime('%Y-%m-%d')}\n")

   def load_from_json():
    """Load market data from JSON (primary source)"""
    
    # Try multiple formats in order of preference
    load_paths = [
        JSON_STORE_PATH,  # Regular JSON
        JSON_STORE_PATH.with_suffix('.json.gz'),  # Compressed JSON
        DATA_DIR / "all_markets_combined.csv",  # Combined CSV
    ]
    
    for path in load_paths:
        try:
            if path.suffix == '.csv':
                # Load from CSV
                df = pd.read_csv(path)
                # Convert back to market-specific dataframes
                markets_df = {}
                for market in df['Market'].unique():
                    market_df = df[df['Market'] == market].drop('Market', axis=1)
                    market_df['Date'] = pd.to_datetime(market_df['Date'])
                    markets_df[market] = market_df
                return markets_df
                
            elif path.suffix == '.gz':
                # Load compressed JSON
                import gzip
                with gzip.open(path, 'rt') as f:
                    data = json.load(f)
            else:
                # Load regular JSON
                with open(path, 'r') as f:
                    data = json.load(f)
            
            # Convert JSON to dataframes
            if isinstance(data, dict):
                markets_df = {}
                for market, market_data in data.items():
                    df = pd.DataFrame({
                        'Date': pd.to_datetime(market_data['Date']),
                        'Longs': market_data['Longs'],
                        'Shorts': market_data['Shorts'],
                        'Total': market_data['Total'],
                        'Long %': market_data['Long %'],
                        'Short %': market_data['Short %'],
                        'Net': market_data['Net']
                    })
                    markets_df[market] = df
                return markets_df
                
        except Exception as e:
            continue
    
    return None         
            """
    """Save all market data to JSON for permanent storage"""
    data_to_save = {}
    for market, df in st.session_state.markets_df.items():
        data_to_save[market] = {
            'Date': df['Date'].dt.strftime('%Y-%m-%d').tolist(),
            'Longs': df['Longs'].tolist(),
            'Shorts': df['Shorts'].tolist(),
            'Total': df['Total'].tolist(),
            'Long %': df['Long %'].tolist(),
            'Short %': df['Short %'].tolist(),
            'Net': df['Net'].tolist()
        }
    
    with open(JSON_STORE_PATH, 'w') as f:
        json.dump(data_to_save, f, indent=2)
    
    # Save to Excel (primary)
    with pd.ExcelWriter(EXCEL_STORE_PATH, engine='openpyxl') as writer:
        for market, df in st.session_state.markets_df.items():
            sheet_name = market.replace('/', '_').replace(' ', '_')[:31]
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    # Save backup Excel with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = DATA_DIR / f"cot_backup_{timestamp}.xlsx"
    with pd.ExcelWriter(backup_path, engine='openpyxl') as writer:
        for market, df in st.session_state.markets_df.items():
            sheet_name = market.replace('/', '_').replace(' ', '_')[:31]
            df.to_excel(writer, sheet_name=sheet_name, index=False)




    
def load_from_json():
    """Load market data from JSON if it exists"""
    if JSON_STORE_PATH.exists():
        try:
            with open(JSON_STORE_PATH, 'r') as f:
                data = json.load(f)
            
            markets_df = {}
            for market, market_data in data.items():
                df = pd.DataFrame({
                    'Date': pd.to_datetime(market_data['Date']),
                    'Longs': market_data['Longs'],
                    'Shorts': market_data['Shorts'],
                    'Total': market_data['Total'],
                    'Long %': market_data['Long %'],
                    'Short %': market_data['Short %'],
                    'Net': market_data['Net']
                })
                markets_df[market] = df
            return markets_df
        except:
            return None
    return None
"""
# -------------------------------
# HISTORICAL DATA ARRAYS - 16+ WEEKS FROM YOUR EXCEL FILES
# -------------------------------

def load_historical_data():
    """Load COMPLETE historical data for ALL markets from your Excel files"""
    
    markets_df = {}
    
    # ============= CURRENCIES - 16+ WEEKS HISTORICAL DATA =============
    
    # ----- USD/CAD -----
    markets_df['USD/CAD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-02-03','2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
        ]),
        'Longs': [77397,77169, 59456, 62705, 56931, 52787, 41739, 25653, 15794, 
                  19047, 21438, 24252, 23151],
        'Shorts': [75267,93215, 101241, 104955, 97516, 93298, 97532, 112293, 146394, 
                 169094, 171852, 173351, 180786],
    })
    
    # ----- EUR/USD -----
    markets_df['EUR/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-02-03','2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
            
        ]),
        'Longs': [302300,290336, 275235, 283592, 298253, 294738, 293179, 277002, 268118,
                 249672, 244392, 243961, 235920],
        'Shorts': [138339,158202, 163540, 150936, 135441, 137273, 133288, 132099, 129330,
                  141219, 150321, 144954, 162331],
    })
    
    # ----- GBP/USD -----
    markets_df['GBP/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-02-03','2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
        ]),
        'Longs': [94893,87786, 81332, 79003, 76486, 196003, 198132, 282031, 332405,
                 299768, 315550, 279341, 272612],
        'Shorts': [108804,103948, 103312, 104273, 107024, 69492, 63540, 61968, 60319,
                  52252, 45257, 53189, 52423],
    })
    
    # ----- USD/JPY -----
    markets_df['USD/JPY'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-02-03','2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
        ]),
        'Longs': [114428,104460, 107139, 111743, 140441, 144596, 141133, 146275, 184488,
                  184958, 169218, 169890, 172349],
        'Shorts': [133650,138393, 151968, 156907, 131626, 130528, 139910, 149217, 167040,
                 148540, 142701, 138733, 123873],
    })
    
    # ----- USD/ZAR -----
    markets_df['USD/ZAR'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-02-03','2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
        ]),
        'Longs': [15993,17299, 16757, 16425, 16197, 15516, 14908, 13163, 12772,
                  12935, 12834, 14185, 13573],
        'Shorts': [6313,8217, 8301, 9315, 10291, 10395, 10060, 10326, 7034,
                 6198, 5395, 6664, 6053],
    })
    
    # ----- USD/MXN -----
    markets_df['USD/MXN'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-02-03','2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
        ]),
        'Longs': [132392,149094, 153398, 153670, 161616, 162240, 164985, 158447, 153728,
                  149102, 126551, 132165, 118511],
        'Shorts': [41800,45980, 46245, 50112, 52315, 55874, 63809, 71335, 46752,
                 50162, 31364, 36337, 33330],
    })
    
    # ----- NZD/USD -----
    markets_df['NZD/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-02-03','2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
        ]),
        'Longs': [11883,12074, 13670, 9613, 12971, 8706, 9596, 8129, 14333,
                 18394, 23477, 24211, 28677],
        'Shorts': [46177,59819, 63280, 58464, 56334, 51960, 53630, 56172, 71114,
                  71510, 75548, 73468, 72746],
    })
    
    # ----- AUD/USD -----
    markets_df['AUD/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-02-03','2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
        ]),
        'Longs': [118751,109806, 85759, 83955, 80491, 77497, 70657, 67640, 57569,
                 45868, 43918, 45721, 43114],
        'Shorts': [92633,102660, 99770, 102801, 99451, 98713, 92255, 89535, 120516,
                  129261, 128094, 121577, 121741],
    })
    
    # ----- USD/BRL -----
    markets_df['USD/BRL'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-02-03','2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
        ]),
        'Longs': [57232,56027, 53730, 52400, 48051, 60132, 61596, 66955, 74505,
                  74586, 71274, 74494, 74487],
        'Shorts': [26270,37182, 36089, 34526, 30434, 18023, 13921, 18949, 17071,
                 13740, 14493, 20675, 17082],
    })
    
    # ----- USD/CHF -----
    markets_df['USD/CHF'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-02-03','2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
        ]),
        'Longs': [9687,9724, 12257, 13395, 11077, 8910, 8780, 9448, 8456,
                  6894, 7571, 8746, 7403],
        'Shorts': [50404,52617, 55464, 56787, 51434, 53108, 52769, 48355, 47059,
                 42679, 42931, 40931, 43452],
    })
    
    # ============= METALS - 16+ WEEKS HISTORICAL DATA =============
    
    # ----- XAU/USD (GOLD) -----
    markets_df['XAU/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-02-03','2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
        ]),
        'Longs': [214508,252100, 295772, 296183, 274435, 275592, 290161, 280920, 268485,
                 261331, 253266, 269556, 265916],
        'Shorts': [48904,46704, 51002, 44945, 46803, 44419, 49461, 46942, 44599,
                  43771, 48678, 59217, 58847],
    })
    
    # ----- XAG/USD (SILVER) -----
    markets_df['XAG/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-02-03','2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
        ]),
        'Longs': [38883,43475, 42965, 47337, 47384, 50506, 55243, 56034, 65958,
                 59575, 52002, 54535, 55038],
        'Shorts': [13006,19772, 17751, 15277, 18113, 20443, 19359, 19682, 21249,
                  21056, 19814, 20519, 22052],
    })
    
    # ----- COPPER/USD -----
    markets_df['COPPER/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-02-03','2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
        ]),
        'Longs': [97407,188489, 183287, 135316, 106753, 102547, 105920, 110300, 102118,
                 93041, 61538, 48674, 51777],
        'Shorts': [49593,46306, 50358, 50626, 44712, 58499, 58299, 58179, 58908,
                  67639, 67485, 68749, 73590],
    })
    
    # ----- STEEL-HRC/USD -----
    markets_df['STEEL-HRC/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-02-03','2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
        ]),
        'Longs': [13849,14856, 14235, 13437, 12020, 12852, 12180, 11332, 10402,
                 8682, 9326, 8866, 7583],
        'Shorts': [2362,2516, 2564, 2415, 2543, 2791, 2236, 2403, 2366,
                  2669, 3400, 2867, 2807],
    })
    
    # ----- LITHIUM/USD -----
    markets_df['LITHIUM/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-02-03','2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
        ]),
        'Longs': [2881,3263, 3499, 3631, 4046, 4198, 4126, 4293, 4406,
                 4376, 4919, 5074, 5252],
        'Shorts': [10839,11352, 11348, 10525, 9888, 12155, 11710, 12482, 13046,
                  13071, 14818, 14889, 14923],
    })
    
    # ============= ENERGIES - 16+ WEEKS HISTORICAL DATA =============
    
    # ----- CRUDE OIL/USD -----
    markets_df['CRUDE OIL/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-02-03','2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
        ]),
        'Longs': [151103,145710, 142097, 146531, 141251, 130251, 122362, 136298, 141636,
                 109644, 107447, 120683, 125340],
        'Shorts': [73241,76266, 76986, 75225, 71878, 67713, 69985, 72507, 67101,
                  72858, 80769, 77700, 74886],
    })
    
    # ----- NAT GAS/USD -----
    markets_df['NAT GAS/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-02-03','2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
        ]),
        'Longs': [203843, 240024, 273463, 298303, 323975, 320954, 351162, 314412,
                 296553, 242178, 196658, 190434,189999],
        'Shorts': [82032, 82245, 80573, 82198, 78703, 81141, 97072, 86286,
                  79129, 84985, 119042, 144269,145554],
    })
    
    # ============= AGRICULTURE - 16+ WEEKS HISTORICAL DATA =============
    
    # ----- COFFEE/USD -----
    markets_df['COFFEE/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-02-03','2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
        ]),
        'Longs': [49342,57118, 56009, 57888, 56382, 53068, 53105, 58241, 60286,
                 60948, 61210, 63537, 69746],
        'Shorts': [30978,24384, 26246, 25136, 25846, 28525, 29432, 28337, 25539,
                  25598, 25223, 25007, 26449],
    })
    
    # ----- WHEAT SRW/USD -----
    markets_df['WHEAT SRW/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-02-03','2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
        ]),
        'Longs': [119821, 124615, 128167, 127991, 132115, 134314, 133339, 123773,
                 116502, 105562, 110378, 118609, 125795],
        'Shorts': [199211, 218345, 214192, 216082, 203106, 206146, 183505, 152845,
                  142933, 140928, 139665, 154507, 174981],
    })
    
    # ----- WHEAT HRW/USD -----
    markets_df['WHEAT HRW/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-02-03','2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
        ]),
        'Longs': [79923, 82290, 86843, 89618, 84515, 83434, 85510, 78437,
                 70860, 71772, 75561, 79903, 77673],
        'Shorts': [83089, 87020, 87183, 87988, 86315, 92219, 96568, 82422,
                  79037, 81882, 79300, 95434, 100281],
    })
    
    # ============= CRYPTO - 16+ WEEKS HISTORICAL DATA =============
    
    # ----- MICRO-BTC/USD -----
    markets_df['MICRO-BTC/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-02-03','2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
        ]),
        'Longs': [17822,18878, 23797, 21863, 19367, 14385, 21221, 24217, 23124,
                 20378, 34029, 28971, 25475],
        'Shorts': [24081,26154, 29254, 26851, 24303, 18343, 26469, 29513, 27994,
                  25413, 40626, 34110, 30669],
    })
    
    # Calculate derived columns for ALL markets
    for market in markets_df:
        df = markets_df[market]
        df['Total'] = df['Longs'] + df['Shorts']
        df['Net'] = df['Longs'] - df['Shorts']
        df['Long %'] = (df['Longs'] / df['Total'] * 100).round(1)
        df['Short %'] = (df['Shorts'] / df['Total'] * 100).round(1)
        df = df.sort_values('Date', ascending=True).reset_index(drop=True)
        markets_df[market] = df
    
    return markets_df

# -------------------------------
# APPLY SWITCH LOGIC FOR USD-BASED PAIRS
# -------------------------------

def apply_switch_logic(markets_df):
    """Apply long/short switching for currencies with USD as base"""
    switch_markets = ['USD/CAD', 'USD/CHF', 'USD/JPY', 'USD/MXN', 'USD/BRL', 'USD/ZAR']
    
    for market in switch_markets:
        if market in markets_df:
            df = markets_df[market].copy()
            # Swap longs and shorts
            df['Longs'], df['Shorts'] = df['Shorts'], df['Longs']
            df['Net'] = -df['Net']
            df['Long %'], df['Short %'] = df['Short %'], df['Long %']
            df['Total'] = df['Longs'] + df['Shorts']
            markets_df[market] = df
    
    return markets_df

# -------------------------------
# YOUR EXACT CFTC EXTRACTOR
# -------------------------------
class CombinedCFTCExtractor:
    def __init__(self):
        self.commodity_data = {}
        self.report_date = ""

    def parse_report_text(self, text, source):
        date_match = re.search(r'FUTURES ONLY POSITIONS AS OF (\d{2}/\d{2}/\d{2})', text, re.IGNORECASE)
        if date_match:
            report_date_str = date_match.group(1)
            month, day, year = report_date_str.split('/')
            self.report_date = f"20{year}-{month}-{day}"

        commodity_blocks = re.split(r'NUMBER OF TRADERS IN EACH CATEGORY', text, flags=re.IGNORECASE)

        data = {}
        for block in commodity_blocks:
            name_match = re.search(r'([A-Z][A-Z0-9#\s,\-\.]+)\s*-\s*(CHICAGO MERCANTILE EXCHANGE|COMMODITY EXCHANGE INC\.|ICE FUTURES EUROPE|ICE FUTURES U\.S\.)', block, re.IGNORECASE)
            if name_match:
                commodity_name = name_match.group(1).strip()
                
                commitments_match = re.search(r'COMMITMENTS\s+([\d,\s-]+)', block, re.IGNORECASE)
                if commitments_match:
                    numbers_str = commitments_match.group(1)
                    numbers = re.findall(r'[-]?\d+', numbers_str.replace(',', ''))
                    if len(numbers) >= 8:
                        noncomm_long = int(numbers[0])
                        noncomm_short = int(numbers[1])
                        net_position = noncomm_long - noncomm_short
                        total_positions = noncomm_long + noncomm_short
                        long_percent = (noncomm_long / total_positions * 100) if total_positions > 0 else 0
                        short_percent = (noncomm_short / total_positions * 100) if total_positions > 0 else 0

                        data[commodity_name] = {
                            'longs': noncomm_long,
                            'shorts': noncomm_short,
                            'net': net_position,
                            'long_percent': round(long_percent, 2),
                            'short_percent': round(short_percent, 2),
                            'total': total_positions
                        }
        return data

    def fetch_current_reports(self):
        urls = {
            'CME': "https://www.cftc.gov/dea/futures/deacmesf.htm",
            'COMEX': "https://www.cftc.gov/dea/futures/deacmxsf.htm",
            'ICE_US': "https://www.cftc.gov/dea/futures/deanybtsf.htm",
            'ICE_EU': "https://www.cftc.gov/dea/futures/deaiceusf.htm",
        }

        all_current = {}
        for source, url in urls.items():
            try:
                response = requests.get(url, timeout=30)
                text = response.text
                data = self.parse_report_text(text, source)
                all_current.update(data)
            except Exception:
                pass

        self.commodity_data = all_current
        return all_current

    def extract_all(self):
        self.fetch_current_reports()
        return self.get_grouped_data()

    def get_grouped_data(self):
        currency_mapping = {
            'EURO FX': 'EUR/USD',
            'BRITISH POUND': 'GBP/USD',
            'AUSTRALIAN DOLLAR': 'AUD/USD',
            'NZ DOLLAR': 'NZD/USD',
            'CANADIAN DOLLAR': 'USD/CAD',
            'SWISS FRANC': 'USD/CHF',
            'MEXICAN PESO': 'USD/MXN',
            'BRAZILIAN REAL': 'USD/BRL',
            'SO AFRICAN RAND': 'USD/ZAR',
            'JAPANESE YEN': 'USD/JPY',
        }

        groups = {
            'Currencies': {},
            'Metals': {},
            'Energies': {},
            'Agriculture': {},
            'Crypto': {}
        }

        # Currencies
        for cme_name, user_name in currency_mapping.items():
            if cme_name in self.commodity_data:
                data = self.commodity_data[cme_name].copy()
                groups['Currencies'][user_name] = data

        # Metals
        metal_names = {
            'GOLD': 'XAU/USD',
            'SILVER': 'XAG/USD',
            'COPPER- #1': 'COPPER/USD',
            'STEEL-HRC': 'STEEL-HRC/USD',
            'LITHIUM HYDROXIDE': 'LITHIUM/USD',
        }
        for orig_name, display_name in metal_names.items():
            for key in self.commodity_data:
                if orig_name in key:
                    groups['Metals'][display_name] = self.commodity_data[key]
                    break

        # Energies
        for key in self.commodity_data:
            if 'CRUDE OIL' in key.upper():
                groups['Energies']['CRUDE OIL/USD'] = self.commodity_data[key]
            if 'NATURAL GAS' in key.upper():
                groups['Energies']['NAT GAS/USD'] = self.commodity_data[key]

        # Agriculture
        for key in self.commodity_data:
            if 'COFFEE' in key.upper():
                groups['Agriculture']['COFFEE/USD'] = self.commodity_data[key]
            if 'WHEAT-SRW' in key.upper():
                groups['Agriculture']['WHEAT SRW/USD'] = self.commodity_data[key]
            if 'WHEAT-HRW' in key.upper():
                groups['Agriculture']['WHEAT HRW/USD'] = self.commodity_data[key]

        # Crypto
        for key in self.commodity_data:
            if 'MICRO BITCOIN' in key.upper():
                groups['Crypto']['MICRO-BTC/USD'] = self.commodity_data[key]
                break

        return groups

# -------------------------------
# FUNCTION TO ADD NEW DATA
# -------------------------------

def add_new_data(markets_df, display_name, new_date, new_data):
    """Add new data and maintain rolling 13 weeks"""
    
    switch_markets = ['USD/CAD', 'USD/CHF', 'USD/JPY', 'USD/MXN', 'USD/BRL', 'USD/ZAR']
    
    # Apply switch logic for USD-based pairs
    if display_name in switch_markets:
        processed_data = {
            'longs': new_data['shorts'],
            'shorts': new_data['longs'],
            'net': -new_data['net'],
            'long_percent': new_data['short_percent'],
            'short_percent': new_data['long_percent'],
            'total': new_data['total']
        }
    else:
        processed_data = new_data
    
    new_row = pd.DataFrame([{
        'Date': new_date,
        'Longs': processed_data['longs'],
        'Shorts': processed_data['shorts'],
        'Total': processed_data['total'],
        'Long %': processed_data['long_percent'],
        'Short %': processed_data['short_percent'],
        'Net': processed_data['net']
    }])
    
    if display_name in markets_df:
        updated_df = pd.concat([markets_df[display_name], new_row], ignore_index=True)
    else:
        updated_df = new_row
    
    updated_df = updated_df.sort_values('Date', ascending=True).reset_index(drop=True)
    updated_df = updated_df.drop_duplicates(subset=['Date'], keep='last')
    markets_df[display_name] = updated_df
    
    return markets_df
"""

# -------------------------------
# DATA EDITING FUNCTIONS
# -------------------------------
# Edit button for this market
if st.session_state.edit_mode and st.session_state.current_editing_market is None:
    col_e1, col_e2, col_e3 = st.columns(3)
    with col_e1:
        if st.button(f"✏️ Quick Edit {market}", key=f"quick_edit_{market}"):
            st.session_state.current_editing_market = market
            st.session_state.edit_submode = 'quick'
            st.rerun()
    with col_e2:
        if st.button(f"📝 Bulk Edit {market}", key=f"bulk_edit_{market}"):
            st.session_state.current_editing_market = market
            st.session_state.edit_submode = 'bulk'
            st.rerun()
    with col_e3:
        if st.button(f"🔍 Insert Missing Week", key=f"insert_{market}"):
            st.session_state.current_editing_market = market
            st.session_state.edit_submode = 'insert'
            st.rerun()

# Handle different edit modes
if st.session_state.edit_mode and st.session_state.current_editing_market == market:
    if st.session_state.get('edit_submode') == 'quick':
        # Your existing quick edit code
        st.subheader("✏️ QUICK EDIT MODE")
        # ... (keep your existing quick edit code)
        
    elif st.session_state.get('edit_submode') == 'bulk':
        # New bulk edit mode
        bulk_edit_mode(market)
        
    elif st.session_state.get('edit_submode') == 'insert':
        # Insert missing week mode
        st.subheader(f"🔍 INSERT MISSING WEEK - {market}")
        
        # Show available dates
        df = st.session_state.markets_df[market]
        st.write("Current data range:")
        st.write(f"From: {df['Date'].min().strftime('%Y-%m-%d')}")
        st.write(f"To: {df['Date'].max().strftime('%Y-%m-%d')}")
        
        # Input for missing date
        missing_date = st.date_input(
            "Select date to insert",
            value=df['Date'].max() + timedelta(days=7),
            min_value=df['Date'].min() + timedelta(days=7),
            max_value=df['Date'].max() - timedelta(days=7)
        )
        
        col_i1, col_i2, col_i3 = st.columns(3)
        with col_i1:
            if st.button("📊 Auto-Interpolate", key=f"interpolate_{market}"):
                success, msg = insert_missing_week(market, missing_date.strftime('%Y-%m-%d'))
                if success:
                    st.success(msg)
                    st.session_state.edit_mode = False
                    st.rerun()
                else:
                    st.error(msg)
        
        with col_i2:
            if st.button("➕ Add Manual", key=f"manual_{market}"):
                # Switch to quick edit with this date pre-filled
                st.session_state.edit_prefill_date = missing_date
                st.session_state.edit_submode = 'quick'
                st.rerun()
        
        with col_i3:
            if st.button("❌ Cancel", key=f"cancel_insert_{market}"):
                st.session_state.edit_mode = False
                st.rerun()

"""
def save_edited_data(market, edited_df):
    """Save edited data back to session state"""
    st.session_state.markets_df[market] = edited_df
    save_to_json()  # Auto-save after edits
    st.session_state.edit_mode = False
    st.session_state.editable_df = None
    st.session_state.current_editing_market = None
    st.success(f"✅ Data for {market} updated successfully!")

def cancel_edit():
    """Cancel editing mode"""
    st.session_state.edit_mode = False
    st.session_state.editable_df = None
    st.session_state.current_editing_market = None
"""
# -------------------------------
# ENHANCED MARKET ANALYSIS WITH PEAK VALUES AND TOGGLE SECTIONS
# -------------------------------

def analyze_market_with_peaks(df, market_name):
    """Comprehensive market analysis including peak/min values with toggle sections"""
    
    recent_13 = df.tail(13).copy()
    latest = recent_13.iloc[-1]
    
    avg_longs = recent_13['Longs'].mean()
    avg_shorts = recent_13['Shorts'].mean()
    avg_net = recent_13['Net'].mean()
    
    # Calculate bias shift indicators
    longs_vs_avg = ((latest['Longs'] - avg_longs) / avg_longs * 100)
    shorts_vs_avg = ((latest['Shorts'] - avg_shorts) / avg_shorts * 100)
    
    # Get peak values for this market
    peaks = PEAK_VOLUME_VALUES.get(market_name, {'has_peaks': False})
    
    analysis = []
    
    # ==================== HEADER ====================
    analysis.append(f"## 📊 COMPLETE ANALYSIS: {market_name}")
    analysis.append("---")
    
    # ==================== BIAS SHIFT WARNING ====================
    if abs(longs_vs_avg) > 15 or abs(shorts_vs_avg) > 15:
        analysis.append("### ⚠️ **BIAS SHIFT DETECTED!**")
        if longs_vs_avg > 15:
            analysis.append(f"🔥 **LONGS shifting BULLISH** - {longs_vs_avg:+.1f}% above 13-week average")
            analysis.append("🎯 **Watch DEMAND ZONES carefully as price approaches**")
        elif longs_vs_avg < -15:
            analysis.append(f"📉 **LONGS shifting BEARISH** - {longs_vs_avg:+.1f}% below 13-week average")
            analysis.append("🎯 **Watch SUPPLY ZONES carefully as price approaches**")
        
        if shorts_vs_avg > 15:
            analysis.append(f"🔥 **SHORTS shifting BEARISH** - {shorts_vs_avg:+.1f}% above 13-week average")
            analysis.append("🎯 **Watch SUPPLY ZONES carefully as price approaches**")
        elif shorts_vs_avg < -15:
            analysis.append(f"📈 **SHORTS shifting BULLISH** - {shorts_vs_avg:+.1f}% below 13-week average")
            analysis.append("🎯 **Watch DEMAND ZONES carefully as price approaches**")
        analysis.append("---")
    
    # ==================== SECTION 1: CURRENT POSITIONING ====================
    if st.session_state.show_positioning:
        analysis.append("### 🎯 CURRENT INSTITUTIONAL POSITIONING")
        analysis.append(f"- **Longs:** {latest['Longs']:,.0f} ({latest['Long %']:.1f}%)")
        analysis.append(f"- **Shorts:** {latest['Shorts']:,.0f} ({latest['Short %']:.1f}%)")
        analysis.append(f"- **Net Position:** {latest['Net']:+,.0f}")
        
        if latest['Long %'] >= 70:
            analysis.append("🔥 **EXTREME BULLISH** - 70%+ long concentration")
        elif latest['Short %'] >= 70:
            analysis.append("🔥 **EXTREME BEARISH** - 70%+ short concentration")
        analysis.append("")
    
    # ==================== SECTION 2: PEAK VOLUME ANALYSIS ====================
    if st.session_state.show_peak:
        analysis.append("### 📈 PEAK VOLUME ANALYSIS")
        
        if peaks['has_peaks']:
            if peaks['peak_longs']:
                longs_pct_of_peak = (latest['Longs'] / peaks['peak_longs'] * 100)
                analysis.append(f"\n**Longs:** {latest['Longs']:,.0f} vs Peak {peaks['peak_longs']:,.0f} ({longs_pct_of_peak:.1f}%)")
                
                if longs_pct_of_peak >= 98:
                    analysis.append("🔴 **CRITICAL: AT ALL-TIME HIGH** - Swift reversal expected!")
                    analysis.append("   → Look for **SUPPLY ZONES** above current price")
                elif longs_pct_of_peak >= 95:
                    analysis.append("⚠️ **CRITICAL: APPROACHING ALL-TIME HIGH**")
                    analysis.append("   → Prepare for potential reversal at supply zones")
                elif longs_pct_of_peak >= 90:
                    analysis.append("📊 **Near peak levels** - Monitor for exhaustion at supply")
            
            if peaks['peak_shorts']:
                shorts_pct_of_peak = (latest['Shorts'] / peaks['peak_shorts'] * 100)
                analysis.append(f"\n**Shorts:** {latest['Shorts']:,.0f} vs Peak {peaks['peak_shorts']:,.0f} ({shorts_pct_of_peak:.1f}%)")
                
                if shorts_pct_of_peak >= 98:
                    analysis.append("🔴 **CRITICAL: SHORTS AT ALL-TIME HIGH** - Short squeeze imminent!")
                    analysis.append("   → Look for **DEMAND ZONES** below current price")
                elif shorts_pct_of_peak >= 95:
                    analysis.append("⚠️ **CRITICAL: SHORTS APPROACHING ALL-TIME HIGH**")
                    analysis.append("   → Prepare for potential short squeeze at demand zones")
                elif shorts_pct_of_peak >= 90:
                    analysis.append("📊 **Shorts near peak** - Monitor for covering at demand")
            
            if peaks['min_longs'] and latest['Longs'] <= peaks['min_longs'] * 1.1:
                analysis.append(f"\n🟢 **Longs at historic lows** - Potential **DEMAND ZONE** forming")
            
            if peaks['min_shorts'] and latest['Shorts'] <= peaks['min_shorts'] * 1.1:
                analysis.append(f"\n🔴 **Shorts at historic lows** - Potential **SUPPLY ZONE** forming")
        else:
            analysis.append("📊 No peak volume data available for this market")
        analysis.append("")
    
    # ==================== SECTION 3: 13-WEEK COMPARISON ====================
    if st.session_state.show_comparison:
        analysis.append("### 📊 13-WEEK AVERAGE COMPARISON")
        analysis.append(f"- **Longs:** {latest['Longs']:,.0f} vs 13wk avg {avg_longs:,.0f} ({longs_vs_avg:+.1f}%)")
        analysis.append(f"- **Shorts:** {latest['Shorts']:,.0f} vs 13wk avg {avg_shorts:,.0f} ({shorts_vs_avg:+.1f}%)")
        analysis.append(f"- **Net:** {latest['Net']:+,.0f} vs 13wk avg {avg_net:+,.0f}")
        
        if abs(longs_vs_avg) > 20:
            analysis.append(f"\n{'📈' if longs_vs_avg > 0 else '📉'} **Significant deviation** in long positioning")
        if abs(shorts_vs_avg) > 20:
            analysis.append(f"{'📉' if shorts_vs_avg > 0 else '📈'} **Significant deviation** in short positioning")
        analysis.append("")
    
    # ==================== SECTION 4: SUPPLY/DEMAND ZONES ====================
    if st.session_state.show_zones:
        analysis.append("### 🎯 KEY SUPPLY/DEMAND ZONES")
        
        if latest['Long %'] >= 70:
            analysis.append("**📈 DEMAND ZONE** (Institutional Buying)")
            analysis.append("- **Location:** Recent swing lows")
            analysis.append("- **Strategy:** Buy on pullbacks to demand zone")
            analysis.append("- **Stop Loss:** Below the demand zone low")
            analysis.append("- **RSI Confirmation:** Look for RSI > 40 to confirm demand zone holding")
        elif latest['Short %'] >= 70:
            analysis.append("**📉 SUPPLY ZONE** (Institutional Selling)")
            analysis.append("- **Location:** Recent swing highs")
            analysis.append("- **Strategy:** Sell on rallies to supply zone")
            analysis.append("- **Stop Loss:** Above the supply zone high")
            analysis.append("- **RSI Confirmation:** Look for RSI < 60 to confirm supply zone holding")
        else:
            analysis.append("**📊 RANGE BOUNDARIES**")
            analysis.append("- **Demand Zone:** Recent swing lows")
            analysis.append("- **Supply Zone:** Recent swing highs")
            analysis.append("- **Strategy:** Buy at demand, sell at supply")
            analysis.append("- **RSI Confirmation:** RSI < 30 at demand, RSI > 70 at supply")
        analysis.append("")
    
    # ==================== SECTION 5: RSI CONFIRMATION ====================
    if st.session_state.show_rsi:
        analysis.append("### 📊 RSI CONFIRMATION LEVELS")
        analysis.append("**RSI (Relative Strength Index) Rules:**")
        analysis.append("- **RSI > 40** suggests DEMAND ZONE will likely hold (bullish)")
        analysis.append("- **RSI < 60** suggests SUPPLY ZONE will likely hold (bearish)")
        analysis.append("- **RSI < 30** at demand zone = oversold bounce potential")
        analysis.append("- **RSI > 70** at supply zone = overbought reversal potential")
        analysis.append("\n*Note: Check your chart for actual RSI values*")
        analysis.append("")
    
    # ==================== SECTION 6: MYFXBOOK SENTIMENT ====================
    if st.session_state.show_myfxbook:
        analysis.append("### 👥 MYFXBOOK RETAIL SENTIMENT")
        analysis.append("**Contrarian Trading Signals:**")
        
        if latest['Long %'] >= 70:
            analysis.append("- **🔥 Institutional long extreme** → Check MyFxBook for retail long crowd")
            analysis.append("- **CONTRARIAN:** If retail is also long, consider fading the move")
            analysis.append("- **Confirmation:** Wait for retail sentiment to peak before trading against")
        elif latest['Short %'] >= 70:
            analysis.append("- **🔥 Institutional short extreme** → Check MyFxBook for retail short crowd")
            analysis.append("- **CONTRARIAN:** If retail is also short, prepare for reversal")
            analysis.append("- **Confirmation:** Look for retail capitulation")
        else:
            analysis.append("- Monitor MyFxBook for extreme retail positioning (80%+ in one direction)")
            analysis.append("- Use as additional confluence with COT data")
        
        analysis.append("\n**Trading Against Sentiment Rules:**")
        analysis.append("1. Identify extreme retail positioning (70-80%+ on MyFxBook)")
        analysis.append("2. Confirm with COT institutional extreme (70%+ longs/shorts)")
        analysis.append("3. Wait for price to reach key supply/demand zone")
        analysis.append("4. Look for reversal candlestick patterns")
        analysis.append("5. Execute trade in opposite direction of retail crowd")
        analysis.append("")
    
    # ==================== SECTION 7: NEWS & FUNDAMENTAL CONTEXT ====================
    if st.session_state.show_news:
        analysis.append("### 📰 NEWS & FUNDAMENTAL CONTEXT")
        analysis.append("**Check MyFxBook News Section for:**")
        analysis.append("- Central bank decisions (Fed, ECB, BOE, etc.)")
        analysis.append("- Economic data releases (CPI, NFP, GDP, etc.)")
        analysis.append("- Geopolitical events")
        analysis.append("- Market sentiment shifts")
        analysis.append("\n**Integration with COT Data:**")
        analysis.append("- Strong COT positioning + major news event = increased volatility")
        analysis.append("- News can trigger the reversal at extreme COT levels")
        analysis.append("- Use news as confluence for supply/demand zone trades")
        analysis.append("")
    
    # ==================== SECTION 8: ACTIONABLE TRADING PLAN ====================
    if st.session_state.show_plan:
        analysis.append("### 📋 COMPLETE TRADING PLAN")
        
        # Determine bias
        if latest['Long %'] >= 70:
            bias = "BULLISH (but watch for reversal at supply)"
        elif latest['Short %'] >= 70:
            bias = "BEARISH (but watch for reversal at demand)"
        else:
            bias = "NEUTRAL - range trading"
        
        analysis.append(f"**Primary Bias:** {bias}")
        
        analysis.append("\n**✅ ENTRY CONDITIONS (ALL must be met):**")
        analysis.append("1. **COT Confirmation:** Institutional positioning aligns with bias")
        analysis.append("2. **Price Action:** Price reaches key supply/demand zone")
        analysis.append("3. **RSI Confirmation:**")
        analysis.append("   - For DEMAND zone longs: RSI > 40 (zone likely to hold)")
        analysis.append("   - For SUPPLY zone shorts: RSI < 60 (zone likely to hold)")
        analysis.append("4. **MyFxBook Sentiment:** Retail crowd is on opposite side (contrarian)")
        analysis.append("5. **Candlestick Pattern:** Reversal signal at the zone")
        
        analysis.append("\n**🛑 STOP LOSS PLACEMENT:**")
        if "BULLISH" in bias:
            analysis.append("- Below the DEMAND ZONE low")
            analysis.append("- Add 1.5x ATR buffer for volatility")
        elif "BEARISH" in bias:
            analysis.append("- Above the SUPPLY ZONE high")
            analysis.append("- Add 1.5x ATR buffer for volatility")
        else:
            analysis.append("- Beyond range boundaries (below demand or above supply)")
        
        analysis.append("\n**🎯 TAKE PROFIT TARGETS:**")
        analysis.append("- **Target 1:** Nearest opposite zone (1:2 risk/reward)")
        analysis.append("- **Target 2:** Next major supply/demand level")
        analysis.append("- **Target 3:** Trail stop after 1:1 achieved")
        
        analysis.append("\n**⚖️ RISK MANAGEMENT:**")
        analysis.append("- Maximum risk: 1-2% of account per trade")
        analysis.append("- Avoid trading 30 minutes before/after major news")
        analysis.append("- Correlated markets should confirm (e.g., EUR/USD and GBP/USD)")
        
        # Final warning for peak levels
        if peaks['has_peaks']:
            if (peaks['peak_longs'] and latest['Longs'] >= peaks['peak_longs'] * 0.95) or \
               (peaks['peak_shorts'] and latest['Shorts'] >= peaks['peak_shorts'] * 0.95):
                analysis.append("\n⚠️ **⚠️ CRITICAL WARNING: NEAR HISTORICAL EXTREMES! ⚠️**")
                analysis.append("**Action:** Prepare for swift reversal at nearest supply/demand zone")
                analysis.append("**Confirmation:** Wait for RSI divergence and MyFxBook retail extreme")
        
        analysis.append("")
    
    analysis.append("---")
    analysis.append(f"*Analysis based on last {len(recent_13)} weeks of COT data*")
    analysis.append(f"*Combine with technical analysis on your charts*")
    
    return "\n".join(analysis)

# -------------------------------
# LOAD OR INITIALIZE DATA
# -------------------------------
loaded_data = load_from_json()
if loaded_data:
    st.session_state.markets_df = loaded_data
    st.session_state.historical_data_loaded = True
else:
    st.session_state.markets_df = load_historical_data()
    st.session_state.markets_df = apply_switch_logic(st.session_state.markets_df)
    st.session_state.historical_data_loaded = True
    save_to_json()

# -------------------------------
# AUTO-FETCH ON FRIDAYS
# -------------------------------
def check_and_auto_fetch():
    today = datetime.now().date()
    is_friday = datetime.now().weekday() == 4
    
    if 'last_auto_fetch' not in st.session_state:
        st.session_state.last_auto_fetch = None
    
    if is_friday and st.session_state.last_auto_fetch != today:
        with st.spinner("📡 Auto-fetching weekly CFTC data..."):
            extractor = CombinedCFTCExtractor()
            grouped_data = extractor.extract_all()
            
            if extractor.report_date:
                report_date = datetime.strptime(extractor.report_date, '%Y-%m-%d')
                
                # Check if data already exists
                data_already_exists = False
                for group_name, markets in grouped_data.items():
                    for display_name, data in markets.items():
                        if display_name in st.session_state.markets_df:
                            df = st.session_state.markets_df[display_name]
                            if report_date in df['Date'].values:
                                data_already_exists = True
                                break
                
                if data_already_exists:
                    st.info(f"ℹ️ Data for {extractor.report_date} already exists in database")
                    return
                
                st.session_state.last_fetch_date = extractor.report_date
                st.session_state.last_auto_fetch = today
                st.session_state.fetch_history.append(extractor.report_date)
                
                added_count = 0
                for group_name, markets in grouped_data.items():
                    for display_name, data in markets.items():
                        if display_name not in st.session_state.markets_df:
                            switch_markets = ['USD/CAD', 'USD/CHF', 'USD/JPY', 'USD/MXN', 'USD/BRL', 'USD/ZAR']
                            if display_name in switch_markets:
                                processed_data = {
                                    'longs': data['shorts'],
                                    'shorts': data['longs'],
                                    'net': -data['net'],
                                    'long_percent': data['short_percent'],
                                    'short_percent': data['long_percent'],
                                    'total': data['total']
                                }
                            else:
                                processed_data = data
                            
                            st.session_state.markets_df[display_name] = pd.DataFrame([{
                                'Date': report_date,
                                'Longs': processed_data['longs'],
                                'Shorts': processed_data['shorts'],
                                'Total': processed_data['total'],
                                'Long %': processed_data['long_percent'],
                                'Short %': processed_data['short_percent'],
                                'Net': processed_data['net']
                            }])
                            added_count += 1
                        else:
                            df = st.session_state.markets_df[display_name]
                            if report_date not in df['Date'].values:
                                st.session_state.markets_df = add_new_data(
                                    st.session_state.markets_df, display_name, report_date, data
                                )
                                added_count += 1
                
                save_to_json()
                st.success(f"✅ Auto-fetched {added_count} new data points for {extractor.report_date}")

check_and_auto_fetch()

# -------------------------------
# STREAMLIT UI
# -------------------------------

st.sidebar.header("📁 Data Management")

# Show data stats
total_markets = len(st.session_state.markets_df)
total_records = sum(len(df) for df in st.session_state.markets_df.values())
st.sidebar.success(f"✅ LOADED: {total_markets} markets")
st.sidebar.info(f"📊 Total records: {total_records}")

if st.session_state.last_fetch_date:
    st.sidebar.info(f"📡 Latest: {st.session_state.last_fetch_date}")

# Analysis Section Toggles
st.sidebar.divider()
st.sidebar.header("🔘 Analysis Toggles")

col1, col2 = st.sidebar.columns(2)
with col1:
    st.session_state.show_positioning = st.checkbox("🎯 Positioning", value=st.session_state.show_positioning)
    st.session_state.show_peak = st.checkbox("📈 Peak Volume", value=st.session_state.show_peak)
    st.session_state.show_comparison = st.checkbox("📊 13-Week Comp", value=st.session_state.show_comparison)
    st.session_state.show_zones = st.checkbox("🎯 Supply/Demand", value=st.session_state.show_zones)

with col2:
    st.session_state.show_rsi = st.checkbox("📊 RSI", value=st.session_state.show_rsi)
    st.session_state.show_myfxbook = st.checkbox("👥 MyFxBook", value=st.session_state.show_myfxbook)
    st.session_state.show_news = st.checkbox("📰 News", value=st.session_state.show_news)
    st.session_state.show_plan = st.checkbox("📋 Trading Plan", value=st.session_state.show_plan)

# Manual fetch button with duplicate check
st.sidebar.divider()
if st.sidebar.button("🚀 FETCH LATEST CFTC DATA", type="primary", use_container_width=True):
    with st.spinner("📡 Fetching data from CFTC.gov..."):
        extractor = CombinedCFTCExtractor()
        grouped_data = extractor.extract_all()
        
        if extractor.report_date:
            report_date = datetime.strptime(extractor.report_date, '%Y-%m-%d')
            
            # Check if data already exists
            data_already_exists = False
            for group_name, markets in grouped_data.items():
                for display_name, data in markets.items():
                    if display_name in st.session_state.markets_df:
                        df = st.session_state.markets_df[display_name]
                        if report_date in df['Date'].values:
                            data_already_exists = True
                            break
                    if data_already_exists:
                        break
                if data_already_exists:
                    break
            
            if data_already_exists:
                st.sidebar.warning(f"⚠️ Data for {extractor.report_date} has already been extracted!")
            else:
                st.session_state.last_fetch_date = extractor.report_date
                st.session_state.fetch_history.append(extractor.report_date)
                
                added_count = 0
                for group_name, markets in grouped_data.items():
                    for display_name, data in markets.items():
                        if display_name not in st.session_state.markets_df:
                            switch_markets = ['USD/CAD', 'USD/CHF', 'USD/JPY', 'USD/MXN', 'USD/BRL', 'USD/ZAR']
                            if display_name in switch_markets:
                                processed_data = {
                                    'longs': data['shorts'],
                                    'shorts': data['longs'],
                                    'net': -data['net'],
                                    'long_percent': data['short_percent'],
                                    'short_percent': data['long_percent'],
                                    'total': data['total']
                                }
                            else:
                                processed_data = data
                            
                            st.session_state.markets_df[display_name] = pd.DataFrame([{
                                'Date': report_date,
                                'Longs': processed_data['longs'],
                                'Shorts': processed_data['shorts'],
                                'Total': processed_data['total'],
                                'Long %': processed_data['long_percent'],
                                'Short %': processed_data['short_percent'],
                                'Net': processed_data['net']
                            }])
                            added_count += 1
                        else:
                            df = st.session_state.markets_df[display_name]
                            if report_date not in df['Date'].values:
                                st.session_state.markets_df = add_new_data(
                                    st.session_state.markets_df, display_name, report_date, data
                                )
                                added_count += 1
                
                save_to_json()
                st.sidebar.success(f"✅ Added {added_count} new data points for {extractor.report_date}")
                st.rerun()
        else:
            st.sidebar.error("❌ Failed to fetch data")

# Edit mode toggle
st.sidebar.divider()
if not st.session_state.edit_mode:
    if st.sidebar.button("✏️ Enable Data Editing Mode", use_container_width=True):
        st.session_state.edit_mode = True
        st.rerun()
else:
    st.sidebar.warning("⚠️ Editing Mode Active")
    if st.sidebar.button("❌ Cancel Editing", use_container_width=True):
        cancel_edit()
        st.rerun()

# Clear data button
if st.sidebar.button("🗑️ Clear All Data", use_container_width=True):
    if st.sidebar.checkbox("Confirm delete? This cannot be undone"):
        st.session_state.markets_df = {}
        st.session_state.last_fetch_date = None
        st.session_state.extracted_data_count = 0
        st.session_state.fetch_history = []
        if JSON_STORE_PATH.exists():
            os.remove(JSON_STORE_PATH)
        if EXCEL_STORE_PATH.exists():
            os.remove(EXCEL_STORE_PATH)
        st.sidebar.success("✅ All data cleared")
        st.rerun()

# -------------------------------
# DISPLAY MARKET DATA
# -------------------------------

group_markets = {
    'Currencies': ['EUR/USD', 'GBP/USD', 'AUD/USD', 'NZD/USD', 'USD/CAD', 'USD/CHF', 'USD/JPY', 'USD/MXN', 'USD/BRL', 'USD/ZAR'],
    'Metals': ['XAU/USD', 'XAG/USD', 'COPPER/USD', 'STEEL-HRC/USD', 'LITHIUM/USD'],
    'Energies': ['CRUDE OIL/USD', 'NAT GAS/USD'],
    'Agriculture': ['COFFEE/USD', 'WHEAT SRW/USD', 'WHEAT HRW/USD'],
    'Crypto': ['MICRO-BTC/USD']
}

for group, markets in group_markets.items():
    available_markets = [m for m in markets if m in st.session_state.markets_df]
    
    if available_markets:
        st.header(f"💰 {group}")
        tabs = st.tabs(available_markets)
        
        for idx, market in enumerate(available_markets):
            with tabs[idx]:
                df = st.session_state.markets_df[market].copy()
                
                # Check if we're editing this market
                if st.session_state.edit_mode and st.session_state.current_editing_market == market:
                    st.subheader("✏️ EDITING DATA")
                    st.caption("Edit the values below. Changes will auto-save.")
                    
                    # Create editable dataframe
                    edit_df = df.copy()
                    edit_df['Date'] = edit_df['Date'].dt.strftime('%Y-%m-%d')
                    
                    # Use data_editor for inline editing
                    edited_df = st.data_editor(
                        edit_df[['Date', 'Longs', 'Shorts']],
                        use_container_width=True,
                        num_rows="fixed",
                        key=f"editor_{market}"
                    )
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        if st.button("💾 Save Changes", key=f"save_{market}"):
                            # Convert back to datetime
                            edited_df['Date'] = pd.to_datetime(edited_df['Date'])
                            edited_df['Total'] = edited_df['Longs'] + edited_df['Shorts']
                            edited_df['Net'] = edited_df['Longs'] - edited_df['Shorts']
                            edited_df['Long %'] = (edited_df['Longs'] / edited_df['Total'] * 100).round(1)
                            edited_df['Short %'] = (edited_df['Shorts'] / edited_df['Total'] * 100).round(1)
                            
                            # Sort by date
                            edited_df = edited_df.sort_values('Date', ascending=True).reset_index(drop=True)
                            
                            st.session_state.markets_df[market] = edited_df
                            save_to_json()
                            st.session_state.edit_mode = False
                            st.session_state.current_editing_market = None
                            st.success(f"✅ Data saved for {market}")
                            st.rerun()
                    
                    with col2:
                        if st.button("❌ Cancel", key=f"cancel_{market}"):
                            cancel_edit()
                            st.rerun()
                    
                    with col3:
                        st.button("➕ Add New Row", key=f"add_{market}", disabled=True, 
                                 help="Add new row feature coming soon")
                    
                    st.divider()
                
                # Show 13 weeks (most recent at top)
                display_df = df.sort_values('Date', ascending=False).head(13).copy()
                total_weeks = len(df)
                
                # Calculate averages
                avg_longs = display_df['Longs'].mean()
                avg_shorts = display_df['Shorts'].mean()
                avg_net = display_df['Net'].mean()
                
                latest = display_df.iloc[0]
                
                # Metrics
                col1, col2, col3, col4, col5 = st.columns(5)
                with col1:
                    st.metric("Latest Longs", f"{latest['Longs']:,.0f}", 
                             delta=f"{latest['Longs'] - avg_longs:,.0f}")
                with col2:
                    st.metric("Latest Shorts", f"{latest['Shorts']:,.0f}",
                             delta=f"{latest['Shorts'] - avg_shorts:,.0f}")
                with col3:
                    st.metric("Latest Net", f"{latest['Net']:+,.0f}",
                             delta=f"{latest['Net'] - avg_net:+,.0f}")
                with col4:
                    st.metric("Long %", f"{latest['Long %']:.1f}%")
                with col5:
                    st.metric("Short %", f"{latest['Short %']:.1f}%")
                
                # Switch info
                if market in ['USD/CAD', 'USD/CHF', 'USD/JPY', 'USD/MXN', 'USD/BRL', 'USD/ZAR']:
                    st.caption("🔄 **SWITCHED**: Longs/Shorts swapped for USD-based pair")
                
                # Edit button for this market
                if st.session_state.edit_mode and st.session_state.current_editing_market is None:
                    if st.button(f"✏️ Edit {market}", key=f"edit_btn_{market}"):
                        st.session_state.current_editing_market = market
                        st.rerun()
                
                # Display table
                st.subheader("📅 Last 13 Weeks (Most Recent at Top)")
                
                display_table = display_df.copy()
                display_table['Date'] = display_table['Date'].dt.strftime('%Y-%m-%d')
                display_table['Longs'] = display_table['Longs'].map('{:,.0f}'.format)
                display_table['Shorts'] = display_table['Shorts'].map('{:,.0f}'.format)
                display_table['Net'] = display_table['Net'].map('{:+,.0f}'.format)
                display_table['Long %'] = display_table['Long %'].map('{:.1f}%'.format)
                display_table['Short %'] = display_table['Short %'].map('{:.1f}%'.format)
                
                # Highlight latest
                def highlight_live(row):
                    if st.session_state.last_fetch_date and row['Date'] == st.session_state.last_fetch_date:
                        return ['background-color: #90EE90'] * len(row)
                    return [''] * len(row)
                
                styled_table = display_table[['Date', 'Longs', 'Shorts', 'Net', 'Long %', 'Short %']].style.apply(highlight_live, axis=1)
                st.dataframe(styled_table, use_container_width=True, hide_index=True)
                
                st.caption(f"📈 Total records: {total_weeks} weeks")
                
                # ==================== COMPREHENSIVE ANALYSIS ====================
                st.subheader("🔍 COMPREHENSIVE MARKET ANALYSIS")
                analysis_text = analyze_market_with_peaks(df, market)
                st.markdown(analysis_text)
                
                st.divider()

# -------------------------------
# EXPORT DATA
# -------------------------------
st.sidebar.divider()
st.sidebar.header("💾 Export Data")

if st.session_state.markets_df:
    if st.sidebar.button("💾 Save to Master Excel", use_container_width=True):
        save_to_json()
        st.sidebar.success(f"✅ Saved {len(st.session_state.markets_df)} markets")
    
    st.sidebar.subheader("📋 Download CSV")
    selected_market = st.sidebar.selectbox(
        "Select market",
        sorted(st.session_state.markets_df.keys())
    )
    
    if selected_market:
        df_download = st.session_state.markets_df[selected_market].copy()
        df_download['Date'] = df_download['Date'].dt.strftime('%Y-%m-%d')
        csv = df_download.to_csv(index=False)
        st.sidebar.download_button(
            label=f"📥 Download {selected_market} CSV",
            data=csv,
            file_name=f"{selected_market.replace('/', '_')}_cot_data.csv",
            mime="text/csv",
            use_container_width=True
        )
    
    # Fetch history
    if st.session_state.fetch_history:
        st.sidebar.divider()
        st.sidebar.caption("📅 Fetch History:")
        for date in st.session_state.fetch_history[-5:]:
            st.sidebar.caption(f"  • {date}")

# -------------------------------
# FOOTER
# -------------------------------
st.markdown("---")
st.caption("Data source: U.S. Commodity Futures Trading Commission (CFTC)")
st.caption("✅ **PEAK VOLUME VALUES**: Hardcoded from your Excel files")
st.caption("✅ **SWITCH LOGIC**: Applied to USD/CAD, USD/CHF, USD/JPY, USD/MXN, USD/BRL, USD/ZAR")
st.caption("✅ **13 WEEKS DISPLAY**: Most recent at top")
st.caption("✅ **AUTO-FETCH**: Runs automatically on Fridays")
st.caption("✅ **DUPLICATE CHECK**: Won't fetch same data twice")
st.caption("✅ **EDIT MODE**: Manually add/edit missing data")
st.caption("✅ **TOGGLE SECTIONS**: Each analysis section can be hidden/shown")
st.caption("✅ **BIAS SHIFT ALERTS**: Warns when positioning shifts >15% from 13-week average")






