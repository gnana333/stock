# KiteConnect Configuration
# Reads from environment variables for security
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Your KiteConnect API Key
API_KEY = os.getenv("API_KEY", "tmp23p1tsmywqb5s")

# Your KiteConnect API Secret
API_SECRET = os.getenv("API_SECRET", "d1lkd7orpowxrdm4ff6l4fnctp0cjmh9")

# Your Access Token (obtained after login)
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN", "pcqbBPPDAKvx3lGYeh3sYtsacdwQVWzg")

# Optional: Request Token (for generating new access token)
REQUEST_TOKEN = os.getenv("REQUEST_TOKEN", "JPL5T3D0gZ4gB4dkeBPwfok3AAiB06aM")

# MongoDB Atlas Configuration
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb+srv://kondareddy:konda095156@equity.oxb46dw.mongodb.net/?appName=Equity")
DB_NAME = os.getenv("DB_NAME", "stock_volumes_db")

# Stock symbols to monitor
STOCK_SYMBOLS = [
    # Major Indices (First 5)
    "NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX",
    
    # All F&O Stocks in alphabetical order
    "ABB", "ADANIENSOL", "ADANIENT", "ADANIGREEN", "ADANIPORTS", 
    "ALKEM", "AMBER", "AMBUJACEM", "ANGELONE", "APLAPOLLO", "APOLLOHOSP", "ASIANPAINT", "ASTRAL",    
    "AUBANK", "AUROPHARMA", "AXISBANK", "BAJAJ-AUTO", "BAJAJFINSV", "BAJFINANCE",
    "BDL", "BEL", "BHARATFORG", "BHARTIARTL", "BLUESTARCO", "BPCL", "BRITANNIA",
    "CAMS", "CDSL", "CGPOWER", "CHOLAFIN", "CIPLA", "COALINDIA",
    "COFORGE", "COLPAL", "CONCOR", "CUMMINSIND", "CYIENT", "DABUR", "DALBHARAT", "DELHIVERY", "DIVISLAB", "DLF", "DMART", "DRREDDY",
    "EICHERMOT", "EXIDEIND", "FORTIS", "GLENMARK", "GODREJCP", "GODREJPROP", "GRASIM", "HAL", "HAVELLS",
    "HCLTECH", "HDFCAMC", "HDFCBANK", "HDFCLIFE", "HEROMOTOCO", "HINDALCO", "HINDPETRO", "HINDUNILVR", "HINDZINC", "ICICIBANK", "ICICIGI", "ICICIPRULI",
    "IIFL", "INDHOTEL", "INDIANB", "INDIGO", "INDUSINDBK", "INDUSTOWER", "INFY", "IRCTC", "ITC",
    "JINDALSTEL", "JIOFIN", "JSWSTEEL", "JSWENERGY", "JUBLFOOD", "KALYANKJIL", "KAYNES", "KEI", "KFINTECH", "KOTAKBANK", "KPITTECH", "LAURUSLABS", "LICHSGFIN", "LICI", "LODHA", "LT",
    "LTIM", "LUPIN", "M&M", "MANKIND", "MARICO", "MAXHEALTH", "MAZDOCK",
    "MFSL", "MPHASIS", "MUTHOOTFIN",
    "NAUKRI", "NESTLEIND", "NTPC", "OBEROIRLTY",
    "OIL", "PATANJALI", "PAYTM", "PERSISTENT", "PFC", "PGEL", "PHOENIXLTD", "PIDILITIND",
    "PIIND", "PNBHOUSING", "POLICYBZR", "POLYCAB", "PRESTIGE", "RECLTD",
    "RELIANCE", "RVNL", "SBICARD", "SBILIFE", "SBIN", "SHRIRAMFIN", "SIEMENS", "SONACOMS",
    "SRF", "SUNPHARMA", "SUPREMEIND", "SYNGENE", "TATACHEM", "TATAELXSI",
    "TATAMOTORS", "TATAPOWER", "TATATECH", "TCS", "TECHM", "TIINDIA", "TITAGARH", "TITAN", "TORNTPHARM", "TORNTPOWER",
    "TRENT", "TVSMOTOR", "UNITDSPR", "UNOMINDA", "UPL", "VBL", "VEDL", "VOLTAS",
    "ZYDUSLIFE"
]
