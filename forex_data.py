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

# -------------------------------
# DATA PERSISTENCE FUNCTIONS
# -------------------------------

def save_to_json():
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
    
    with pd.ExcelWriter(EXCEL_STORE_PATH, engine='openpyxl') as writer:
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
            '2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
            '2025-11-04', '2025-10-28', '2025-10-21', '2025-10-14'
        ]),
        'Longs': [77169, 59456, 62705, 56931, 52787, 41739, 25653, 15794, 
                  19047, 21438, 24252, 23151, 27186, 28366, 28098, 27162],
        'Shorts': [93215, 101241, 104955, 97516, 93298, 97532, 112293, 146394, 
                 169094, 171852, 173351, 180786, 186698, 178410, 173206, 164755],
    })
    
    # ----- EUR/USD -----
    markets_df['EUR/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
            '2025-11-04', '2025-10-28', '2025-10-21', '2025-10-14'
        ]),
        'Longs': [290336, 275235, 283592, 298253, 294738, 293179, 277002, 268118,
                 249672, 244392, 243961, 235920, 252542, 250400, 244507, 243010],
        'Shorts': [158202, 163540, 150936, 135441, 137273, 133288, 132099, 129330,
                  141219, 150321, 144954, 162331, 160747, 143067, 132755, 134685],
    })
    
    # ----- GBP/USD -----
    markets_df['GBP/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
            '2025-11-04', '2025-10-28', '2025-10-21', '2025-10-14'
        ]),
        'Longs': [87786, 81332, 79003, 76486, 196003, 198132, 282031, 332405,
                 299768, 315550, 279341, 272612, 270917, 248118, 233484, 79515],
        'Shorts': [103948, 103312, 104273, 107024, 69492, 63540, 61968, 60319,
                  52252, 45257, 53189, 52423, 63117, 82471, 75419, 91144],
    })
    
    # ----- USD/JPY -----
    markets_df['USD/JPY'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
            '2025-11-04', '2025-10-28', '2025-10-21', '2025-10-14'
        ]),
        'Longs': [104460, 107139, 111743, 140441, 144596, 141133, 146275, 184488,
                  184958, 169218, 169890, 172349, 170180, 178745, 175724, 160639],
        'Shorts': [138393, 151968, 156907, 131626, 130528, 139910, 149217, 167040,
                 148540, 142701, 138733, 123873, 118915, 110630, 105310, 123473],
    })
    
    # ----- USD/ZAR -----
    markets_df['USD/ZAR'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
            '2025-11-04', '2025-10-28', '2025-10-21', '2025-10-14'
        ]),
        'Longs': [17299, 16757, 16425, 16197, 15516, 14908, 13163, 12772,
                  12935, 12834, 14185, 13573, 14314, 15488, 15475, 15080],
        'Shorts': [8217, 8301, 9315, 10291, 10395, 10060, 10326, 7034,
                 6198, 5395, 6664, 6053, 5821, 6839, 6500, 7024],
    })
    
    # ----- USD/MXN -----
    markets_df['USD/MXN'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
            '2025-11-04', '2025-10-28', '2025-10-21', '2025-10-14'
        ]),
        'Longs': [149094, 153398, 153670, 161616, 162240, 164985, 158447, 153728,
                  149102, 126551, 132165, 118511, 113164, 116489, 99658, 95674],
        'Shorts': [45980, 46245, 50112, 52315, 55874, 63809, 71335, 46752,
                 50162, 31364, 36337, 33330, 26988, 34985, 26316, 28113],
    })
    
    # ----- NZD/USD -----
    markets_df['NZD/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
            '2025-11-04', '2025-10-28', '2025-10-21', '2025-10-14'
        ]),
        'Longs': [12074, 13670, 9613, 12971, 8706, 9596, 8129, 14333,
                 18394, 23477, 24211, 28677, 25314, 24330, 25478, 25599],
        'Shorts': [59819, 63280, 58464, 56334, 51960, 53630, 56172, 71114,
                  71510, 75548, 73468, 72746, 62726, 59296, 61753, 55340],
    })
    
    # ----- AUD/USD -----
    markets_df['AUD/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
            '2025-11-04', '2025-10-28', '2025-10-21', '2025-10-14'
        ]),
        'Longs': [109806, 85759, 83955, 80491, 77497, 70657, 67640, 57569,
                 45868, 43918, 45721, 43114, 50360, 41715, 51794, 50324],
        'Shorts': [102660, 99770, 102801, 99451, 98713, 92255, 89535, 120516,
                  129261, 128094, 121577, 121741, 121936, 115649, 117558, 109902],
    })
    
    # ----- USD/BRL -----
    markets_df['USD/BRL'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
            '2025-11-04', '2025-10-28', '2025-10-21', '2025-10-14'
        ]),
        'Longs': [56027, 53730, 52400, 48051, 60132, 61596, 66955, 74505,
                  74586, 71274, 74494, 74487, 67193, 75220, 75904, 75781],
        'Shorts': [37182, 36089, 34526, 30434, 18023, 13921, 18949, 17071,
                 13740, 14493, 20675, 17082, 15980, 21390, 20752, 19672],
    })
    
    # ----- USD/CHF -----
    markets_df['USD/CHF'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
            '2025-11-04', '2025-10-28', '2025-10-21', '2025-10-14'
        ]),
        'Longs': [9724, 12257, 13395, 11077, 8910, 8780, 9448, 8456,
                  6894, 7571, 8746, 7403, 8809, 9149, 7366, 8407],
        'Shorts': [52617, 55464, 56787, 51434, 53108, 52769, 48355, 47059,
                 42679, 42931, 40931, 43452, 40696, 37007, 35210, 36613],
    })
    
    # ============= METALS - 16+ WEEKS HISTORICAL DATA =============
    
    # ----- XAU/USD (GOLD) -----
    markets_df['XAU/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
            '2025-11-04', '2025-10-28', '2025-10-21', '2025-10-14'
        ]),
        'Longs': [252100, 295772, 296183, 274435, 275592, 290161, 280920, 268485,
                 261331, 253266, 269556, 265916, 256572, 266308, 253851, 278405],
        'Shorts': [46704, 51002, 44945, 46803, 44419, 49461, 46942, 44599,
                  43771, 48678, 59217, 58847, 54265, 61644, 77242, 74489],
    })
    
    # ----- XAG/USD (SILVER) -----
    markets_df['XAG/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
            '2025-11-04', '2025-10-28', '2025-10-21', '2025-10-14'
        ]),
        'Longs': [43475, 42965, 47337, 47384, 50506, 55243, 56034, 65958,
                 59575, 52002, 54535, 55038, 54166, 55959, 60904, 67041],
        'Shorts': [19772, 17751, 15277, 18113, 20443, 19359, 19682, 21249,
                  21056, 19814, 20519, 22052, 20945, 18840, 23645, 23860],
    })
    
    # ----- COPPER/USD -----
    markets_df['COPPER/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2024-07-23', '2024-07-16', '2024-07-09', '2024-07-02',
            '2024-06-25', '2024-06-18', '2024-06-11', '2024-06-04',
            '2024-05-28', '2024-05-21', '2024-05-14', '2024-05-07',
            '2024-04-30', '2024-04-23', '2024-04-16', '2024-04-09'
        ]),
        'Longs': [188489, 183287, 135316, 106753, 102547, 105920, 110300, 102118,
                 93041, 61538, 48674, 51777, 43668, 48459, 71800, 80000],
        'Shorts': [46306, 50358, 50626, 44712, 58499, 58299, 58179, 58908,
                  67639, 67485, 68749, 73590, 72658, 74692, 63181, 51748],
    })
    
    # ----- STEEL-HRC/USD -----
    markets_df['STEEL-HRC/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
            '2025-11-04', '2025-10-28', '2025-10-21', '2025-10-14'
        ]),
        'Longs': [14856, 14235, 13437, 12020, 12852, 12180, 11332, 10402,
                 8682, 9326, 8866, 7583, 7527, 8696, 6182, 6348],
        'Shorts': [2516, 2564, 2415, 2543, 2791, 2236, 2403, 2366,
                  2669, 3400, 2867, 2807, 3030, 4756, 4087, 4189],
    })
    
    # ----- LITHIUM/USD -----
    markets_df['LITHIUM/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
            '2025-11-04', '2025-10-28', '2025-10-21', '2025-10-14'
        ]),
        'Longs': [3263, 3499, 3631, 4046, 4198, 4126, 4293, 4406,
                 4376, 4919, 5074, 5252, 5168, 5223, 5731, 5661],
        'Shorts': [11352, 11348, 10525, 9888, 12155, 11710, 12482, 13046,
                  13071, 14818, 14889, 14923, 14789, 16006, 16506, 16234],
    })
    
    # ============= ENERGIES - 16+ WEEKS HISTORICAL DATA =============
    
    # ----- CRUDE OIL/USD -----
    markets_df['CRUDE OIL/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
            '2025-11-04', '2025-10-28', '2025-10-21', '2025-10-14'
        ]),
        'Longs': [145710, 142097, 146531, 141251, 130251, 122362, 136298, 141636,
                 109644, 107447, 120683, 125340, 138046, 115809, 121537, 119311],
        'Shorts': [76266, 76986, 75225, 71878, 67713, 69985, 72507, 67101,
                  72858, 80769, 77700, 74886, 75696, 74607, 77712, 77893],
    })
    
    # ----- NAT GAS/USD -----
    markets_df['NAT GAS/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
            '2025-11-04', '2025-10-28', '2025-10-21', '2025-10-14',
            '2025-10-07', '2025-09-30', '2025-09-23', '2025-09-16'
        ]),
        'Longs': [203843, 240024, 273463, 298303, 323975, 320954, 351162, 314412,
                 296553, 242178, 196658, 190434, 186808, 161500, 165944, 167351],
        'Shorts': [82032, 82245, 80573, 82198, 78703, 81141, 97072, 86286,
                  79129, 84985, 119042, 144269, 146934, 169833, 160196, 146164],
    })
    
    # ============= AGRICULTURE - 16+ WEEKS HISTORICAL DATA =============
    
    # ----- COFFEE/USD -----
    markets_df['COFFEE/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
            '2025-11-04', '2025-10-28', '2025-10-21', '2025-10-14'
        ]),
        'Longs': [57118, 56009, 57888, 56382, 53068, 53105, 58241, 60286,
                 60948, 61210, 63537, 69746, 68887, 69173, 69913, 67699],
        'Shorts': [24384, 26246, 25136, 25846, 28525, 29432, 28337, 25539,
                  25598, 25223, 25007, 26449, 24629, 25028, 25570, 25401],
    })
    
    # ----- WHEAT SRW/USD -----
    markets_df['WHEAT SRW/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
            '2025-11-04', '2025-10-28', '2025-10-21', '2025-10-14'
        ]),
        'Longs': [119821, 124615, 128167, 127991, 132115, 134314, 133339, 123773,
                 116502, 105562, 110378, 118609, 125795, 148755, 149938, 151616],
        'Shorts': [199211, 218345, 214192, 216082, 203106, 206146, 183505, 152845,
                  142933, 140928, 139665, 154507, 174981, 198584, 230170, 236295],
    })
    
    # ----- WHEAT HRW/USD -----
    markets_df['WHEAT HRW/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
            '2025-11-04', '2025-10-28', '2025-10-21', '2025-10-14'
        ]),
        'Longs': [79923, 82290, 86843, 89618, 84515, 83434, 85510, 78437,
                 70860, 71772, 75561, 79903, 77673, 85613, 91300, 92053],
        'Shorts': [83089, 87020, 87183, 87988, 86315, 92219, 96568, 82422,
                  79037, 81882, 79300, 95434, 100281, 115136, 133873, 136416],
    })
    
    # ============= CRYPTO - 16+ WEEKS HISTORICAL DATA =============
    
    # ----- MICRO-BTC/USD -----
    markets_df['MICRO-BTC/USD'] = pd.DataFrame({
        'Date': pd.to_datetime([
            '2026-01-27', '2026-01-20', '2026-01-13', '2026-01-06',
            '2025-12-30', '2025-12-23', '2025-12-16', '2025-12-09',
            '2025-12-02', '2025-11-25', '2025-11-18', '2025-11-11',
            '2025-11-04', '2025-10-28', '2025-10-21', '2025-10-14'
        ]),
        'Longs': [18878, 23797, 21863, 19367, 14385, 21221, 24217, 23124,
                 20378, 34029, 28971, 25475, 22032, 32695, 32448, 26134],
        'Shorts': [26154, 29254, 26851, 24303, 18343, 26469, 29513, 27994,
                  25413, 40626, 34110, 30669, 27077, 37814, 38100, 31553],
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

# -------------------------------
# ENHANCED MARKET ANALYSIS WITH PEAK VALUES
# -------------------------------

def analyze_market_with_peaks(df, market_name):
    """Comprehensive market analysis including peak/min values"""
    
    recent_13 = df.tail(13).copy()
    latest = recent_13.iloc[-1]
    
    avg_longs = recent_13['Longs'].mean()
    avg_shorts = recent_13['Shorts'].mean()
    avg_net = recent_13['Net'].mean()
    
    # Get peak values for this market
    peaks = PEAK_VOLUME_VALUES.get(market_name, {'has_peaks': False})
    
    analysis = []
    
    # ==================== HEADER ====================
    analysis.append(f"## 📊 COMPLETE ANALYSIS: {market_name}")
    analysis.append("---")
    
    # ==================== SECTION 1: CURRENT POSITIONING ====================
    analysis.append("### 🎯 CURRENT INSTITUTIONAL POSITIONING")
    analysis.append(f"- **Longs:** {latest['Longs']:,.0f} ({latest['Long %']:.1f}%)")
    analysis.append(f"- **Shorts:** {latest['Shorts']:,.0f} ({latest['Short %']:.1f}%)")
    analysis.append(f"- **Net Position:** {latest['Net']:+,.0f}")
    
    if latest['Long %'] >= 70:
        analysis.append("🔥 **EXTREME BULLISH** - 70%+ long concentration")
    elif latest['Short %'] >= 70:
        analysis.append("🔥 **EXTREME BEARISH** - 70%+ short concentration")
    
    # ==================== SECTION 2: PEAK VOLUME ANALYSIS ====================
    analysis.append("\n### 📈 PEAK VOLUME ANALYSIS")
    
    if peaks['has_peaks']:
        if peaks['peak_longs']:
            longs_pct_of_peak = (latest['Longs'] / peaks['peak_longs'] * 100)
            analysis.append(f"\n**Longs:** {latest['Longs']:,.0f} vs Peak {peaks['peak_longs']:,.0f} ({longs_pct_of_peak:.1f}%)")
            
            if longs_pct_of_peak >= 98:
                analysis.append("🔴 **CRITICAL: AT ALL-TIME HIGH** - Swift reversal expected!")
                analysis.append("   → Look for supply zones above current price")
            elif longs_pct_of_peak >= 95:
                analysis.append("⚠️ **CRITICAL: APPROACHING ALL-TIME HIGH**")
                analysis.append("   → Prepare for potential reversal")
            elif longs_pct_of_peak >= 90:
                analysis.append("📊 **Near peak levels** - Monitor for exhaustion")
        
        if peaks['peak_shorts']:
            shorts_pct_of_peak = (latest['Shorts'] / peaks['peak_shorts'] * 100)
            analysis.append(f"\n**Shorts:** {latest['Shorts']:,.0f} vs Peak {peaks['peak_shorts']:,.0f} ({shorts_pct_of_peak:.1f}%)")
            
            if shorts_pct_of_peak >= 98:
                analysis.append("🔴 **CRITICAL: SHORTS AT ALL-TIME HIGH** - Short squeeze imminent!")
                analysis.append("   → Look for demand zones below current price")
            elif shorts_pct_of_peak >= 95:
                analysis.append("⚠️ **CRITICAL: SHORTS APPROACHING ALL-TIME HIGH**")
                analysis.append("   → Prepare for potential short squeeze")
            elif shorts_pct_of_peak >= 90:
                analysis.append("📊 **Shorts near peak** - Monitor for covering")
        
        if peaks['min_longs'] and latest['Longs'] <= peaks['min_longs'] * 1.1:
            analysis.append(f"\n🟢 **Longs at historic lows** - Potential market bottom")
        
        if peaks['min_shorts'] and latest['Shorts'] <= peaks['min_shorts'] * 1.1:
            analysis.append(f"\n🔴 **Shorts at historic lows** - Potential market top")
    else:
        analysis.append("📊 No peak volume data available for this market")
    
    # ==================== SECTION 3: 13-WEEK COMPARISON ====================
    analysis.append("\n### 📊 13-WEEK AVERAGE COMPARISON")
    
    longs_vs_avg = ((latest['Longs'] - avg_longs) / avg_longs * 100)
    shorts_vs_avg = ((latest['Shorts'] - avg_shorts) / avg_shorts * 100)
    
    analysis.append(f"- **Longs:** {latest['Longs']:,.0f} vs 13wk avg {avg_longs:,.0f} ({longs_vs_avg:+.1f}%)")
    analysis.append(f"- **Shorts:** {latest['Shorts']:,.0f} vs 13wk avg {avg_shorts:,.0f} ({shorts_vs_avg:+.1f}%)")
    analysis.append(f"- **Net:** {latest['Net']:+,.0f} vs 13wk avg {avg_net:+,.0f}")
    
    if abs(longs_vs_avg) > 20:
        analysis.append(f"\n{'📈' if longs_vs_avg > 0 else '📉'} **Significant deviation** in long positioning")
    if abs(shorts_vs_avg) > 20:
        analysis.append(f"{'📉' if shorts_vs_avg > 0 else '📈'} **Significant deviation** in short positioning")
    
    # ==================== SECTION 4: SUPPLY/DEMAND ZONES ====================
    analysis.append("\n### 🎯 KEY SUPPLY/DEMAND ZONES")
    
    if latest['Long %'] >= 70:
        analysis.append("**DEMAND ZONE** (Institutional Buying)")
        analysis.append("- Location: Recent swing lows")
        analysis.append("- Strategy: Buy on pullbacks")
        analysis.append("- Stop: Below zone low")
    elif latest['Short %'] >= 70:
        analysis.append("**SUPPLY ZONE** (Institutional Selling)")
        analysis.append("- Location: Recent swing highs")
        analysis.append("- Strategy: Sell on rallies")
        analysis.append("- Stop: Above zone high")
    else:
        analysis.append("**RANGE BOUNDARIES**")
        analysis.append("- Support: Recent swing lows")
        analysis.append("- Resistance: Recent swing highs")
        analysis.append("- Strategy: Buy support, sell resistance")
    
    # ==================== SECTION 5: ACTIONABLE TRADING PLAN ====================
    analysis.append("\n### 📋 TRADING PLAN")
    
    # Determine bias
    if latest['Long %'] >= 70:
        bias = "BULLISH"
    elif latest['Short %'] >= 70:
        bias = "BEARISH"
    else:
        bias = "NEUTRAL"
    
    analysis.append(f"**Primary Bias:** {bias}")
    
    analysis.append("\n**Entry Conditions:**")
    if bias == "BULLISH":
        analysis.append("1. Wait for pullback to demand zone")
        analysis.append("2. Look for bullish reversal candlestick")
        analysis.append("3. Enter on confirmation")
    elif bias == "BEARISH":
        analysis.append("1. Wait for rally to supply zone")
        analysis.append("2. Look for bearish reversal candlestick")
        analysis.append("3. Enter on confirmation")
    else:
        analysis.append("1. Buy at support, sell at resistance")
        analysis.append("2. Wait for breakout for trend")
    
    analysis.append("\n**Stop Loss:**")
    if bias == "BULLISH":
        analysis.append("- Below demand zone low")
    elif bias == "BEARISH":
        analysis.append("- Above supply zone high")
    else:
        analysis.append("- Beyond range boundaries")
    
    analysis.append("\n**Take Profit:**")
    analysis.append("- Target 1: Nearest swing high/low (1:2 R/R)")
    analysis.append("- Target 2: Next major zone")
    analysis.append("- Target 3: Trail stop after 1:1")
    
    # Final warning for peak levels
    if peaks['has_peaks']:
        if (peaks['peak_longs'] and latest['Longs'] >= peaks['peak_longs'] * 0.95) or \
           (peaks['peak_shorts'] and latest['Shorts'] >= peaks['peak_shorts'] * 0.95):
            analysis.append("\n⚠️ **CRITICAL WARNING: Near historical extremes!**")
    
    analysis.append("\n---")
    analysis.append(f"*Analysis based on last {len(recent_13)} weeks of data*")
    
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

# Manual fetch button
if st.sidebar.button("🚀 FETCH LATEST CFTC DATA", type="primary", use_container_width=True):
    with st.spinner("📡 Fetching data from CFTC.gov..."):
        extractor = CombinedCFTCExtractor()
        grouped_data = extractor.extract_all()
        
        if extractor.report_date:
            report_date = datetime.strptime(extractor.report_date, '%Y-%m-%d')
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
            st.sidebar.success(f"✅ Added {added_count} new data points")
            st.rerun()
        else:
            st.sidebar.error("❌ Failed to fetch data")

# Clear data button
if st.sidebar.button("🗑️ Clear All Data", use_container_width=True):
    if st.sidebar.checkbox("Confirm delete?"):
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
                    st.caption("🔄 **SWITCHED**: Longs/Shorts swapped")
                
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