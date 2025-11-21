from flask import Flask, render_template, jsonify, Response, request
import json
from bson import ObjectId
from kiteconnect import KiteConnect
from datetime import datetime, timedelta, date
import pytz
from pymongo import MongoClient
import config
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Custom JSON encoder for MongoDB ObjectId and datetime
class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        return super().default(obj)

# Custom jsonify function that uses our encoder
def mongo_jsonify(*args, **kwargs):
    return Response(
        json.dumps(dict(*args, **kwargs), cls=MongoJSONEncoder),
        mimetype='application/json'
    )

# Helper function for error responses
def error_response(message, status_code=500):
    response = mongo_jsonify(success=False, error=message)
    response.status_code = status_code
    return response

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Initialize KiteConnect
kite = KiteConnect(api_key=config.API_KEY)
kite.set_access_token(config.ACCESS_TOKEN)

# Indian timezone
IST = pytz.timezone('Asia/Kolkata')

# MongoDB connection
mongo_client = MongoClient(config.MONGODB_URI)
db = mongo_client[config.DB_NAME]
volume_collection = db['previous_day_volumes']
alerts_collection = db['volume_alerts']
oi_collection = db['previous_day_oi']
oi_alerts_collection = db['oi_alerts']
oi_change_collection = db['previous_cycle_oi_change']  # Stores previous cycle's Total Change in OI
oi_change_alerts_collection = db['oi_change_alerts']  # Alerts for Total Change in OI changes
oi_strikes_collection = db['previous_day_oi_strikes']  # Stores previous day OI for individual strikes (spot ±2)
oi_strike_alerts_collection = db['oi_strike_change_alerts']  # Alerts when strike-level OI change <= -100
oi_strike_daily_alerts_collection = db['oi_strike_daily_alerts']  # New collection for daily OI change alerts
previous_cycle_volume_collection = db['previous_cycle_volume']  # Stores previous cycle's volume data
volume_drop_alerts_collection = db['volume_drop_alerts']  # Alerts for volume drops
oi_near_atm_collection = db['previous_day_oi_near_atm']  # Stores previous day OI for near ATM strikes (spot ±10, 21 strikes total)
oi_near_atm_change_collection = db['previous_cycle_oi_near_atm_change']  # Stores previous cycle's near ATM OI change
prev_day_eod_total_oi_collection = db['prev_day_eod_total_oi']  # Stores previous day EOD total OI at 3:30pm spot price (21 strikes)

# Cache for instruments and lot sizes
instruments_cache = {}
lot_size_cache = {}

# Rate limiting for API calls (optimized for faster processing)
import threading
api_call_lock = threading.Lock()
last_api_call_time = 0
MIN_API_CALL_INTERVAL = 0.05  # 50ms between calls = 20 calls/sec (safe limit)


def rate_limited_api_call(api_function, *args, **kwargs):
    """
    Wrapper to enforce rate limiting on KiteConnect API calls
    Ensures minimum interval between calls globally across all threads
    """
    global last_api_call_time
    
    with api_call_lock:
        current_time = time.time()
        time_since_last_call = current_time - last_api_call_time
        
        if time_since_last_call < MIN_API_CALL_INTERVAL:
            sleep_time = MIN_API_CALL_INTERVAL - time_since_last_call
            time.sleep(sleep_time)
        
        try:
            result = api_function(*args, **kwargs)
            last_api_call_time = time.time()
            return result
        except Exception as e:
            last_api_call_time = time.time()
            raise e


def cleanup_old_alerts():
    """
    Delete alerts from previous days to save storage
    Keeps only today's alerts for display
    """
    try:
        current_time = datetime.now(IST)
        today_start = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
        
        logger.info("="*80)
        logger.info("🗑️ CLEANING UP OLD ALERTS")
        logger.info("="*80)
        
        # Delete old volume alerts (left table - 2x alerts)
        old_volume_alerts = alerts_collection.delete_many({
            'date': {'$lt': today_start}
        })
        logger.info(f"🗑️ Deleted {old_volume_alerts.deleted_count} old volume alerts (2x previous day)")
        
        # Delete old volume drop alerts (right table - cycle-to-cycle drops)
        old_volume_drop_alerts = volume_drop_alerts_collection.delete_many({
            'date': {'$lt': today_start}
        })
        logger.info(f"🗑️ Deleted {old_volume_drop_alerts.deleted_count} old volume drop alerts (cycle drops)")
        
        logger.info(f"✅ Cleanup complete - Keeping only alerts from {today_start.date()}")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"❌ Error during alert cleanup: {e}")


def get_previous_trading_day():
    """Get the previous trading day (excluding weekends and holidays)"""
    today = datetime.now(IST).replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Try to fetch historical data for NIFTY 50 index to find the last trading day
    # This will automatically skip weekends and holidays
    try:
        # Start checking from yesterday
        check_date = today - timedelta(days=1)
        max_days_back = 10  # Check up to 10 days back to handle long holiday periods
        
        for i in range(max_days_back):
            try:
                # Skip weekends first (optimization)
                if check_date.weekday() in [5, 6]:  # Saturday or Sunday
                    check_date = check_date - timedelta(days=1)
                    continue
                
                # Try to fetch data for this specific date
                # Using NIFTY 50 index (instrument_token: 256265)
                data = kite.historical_data(
                    instrument_token=256265,  # NIFTY 50
                    from_date=check_date,
                    to_date=check_date,  # Same date for exact match
                    interval="day"
                )
                
                # Check if we got data AND the date matches exactly
                if data and len(data) > 0:
                    # Verify the returned date matches what we requested
                    returned_date = data[0]['date'] if isinstance(data[0], dict) else data[0][0]
                    if isinstance(returned_date, datetime):
                        returned_date = returned_date.date()
                    
                    # If dates match, this was a trading day
                    if returned_date == check_date.date():
                        logger.info(f"Previous trading day found: {check_date.strftime('%Y-%m-%d')} (verified with market data)")
                        return check_date
                    else:
                        logger.info(f"Date mismatch: requested {check_date.date()}, got {returned_date}. Trying previous day...")
                
                # No data for this date or date mismatch, try previous day
                check_date = check_date - timedelta(days=1)
                
            except Exception as e:
                # If error, try previous day
                logger.debug(f"Error checking {check_date.strftime('%Y-%m-%d')}: {e}")
                check_date = check_date - timedelta(days=1)
                continue
        
        # Fallback: if API fails, use simple weekend logic
        logger.warning("Could not fetch previous trading day from API, using fallback logic")
        if today.weekday() == 0:  # Monday
            previous_day = today - timedelta(days=3)
        elif today.weekday() == 6:  # Sunday
            previous_day = today - timedelta(days=2)
        else:
            previous_day = today - timedelta(days=1)
        return previous_day
        
    except Exception as e:
        logger.error(f"Error in get_previous_trading_day: {e}")
        # Fallback to simple logic
        if today.weekday() == 0:  # Monday
            previous_day = today - timedelta(days=3)
        elif today.weekday() == 6:  # Sunday
            previous_day = today - timedelta(days=2)
        else:
            previous_day = today - timedelta(days=1)
        return previous_day


def load_instruments_cache():
    """Load all NFO instruments into cache"""
    global instruments_cache, lot_size_cache
    if not instruments_cache:
        logger.info("Loading NFO instruments...")
        instruments = kite.instruments("NFO")
        for instrument in instruments:
            instruments_cache[instrument['tradingsymbol']] = instrument
            if instrument['lot_size']:
                lot_size_cache[instrument['tradingsymbol']] = instrument['lot_size']
        logger.info(f"Loaded {len(instruments_cache)} instruments")


def get_lot_size_for_symbol(symbol):
    """Get lot size for a stock symbol"""
    for trading_symbol, lot_size in lot_size_cache.items():
        if symbol in trading_symbol:
            return lot_size
    return None


def get_nearest_expiry(symbol):
    """Get the nearest expiry date for options"""
    expiry_dates = set()
    for trading_symbol, instrument in instruments_cache.items():
        if instrument['name'] == symbol and instrument['instrument_type'] in ['CE', 'PE']:
            if instrument['expiry']:
                expiry_dates.add(instrument['expiry'])
    if expiry_dates:
        return sorted(expiry_dates)[0]
    return None


def get_total_volume_and_oi_all_strikes(symbol, option_type, expiry_date, from_date, to_date):
    """Get total volume from historical data AND current OI from quote API"""
    matching_options = []
    for trading_symbol, instrument in instruments_cache.items():
        if (instrument['name'] == symbol and 
            instrument['instrument_type'] == option_type and
            instrument['expiry'] == expiry_date):
            matching_options.append(instrument)
    
    if not matching_options:
        logger.warning(f"No {option_type} options found for {symbol}")
        return 0, 0
    
    logger.info(f"Found {len(matching_options)} {option_type} strikes for {symbol}")
    
    total_volume = 0
    total_oi = 0
    
    # Get volume and OI from historical data with oi=1 parameter
    for idx, option in enumerate(matching_options):
        try:
            data = kite.historical_data(
                instrument_token=option['instrument_token'],
                from_date=from_date,
                to_date=to_date,
                interval="day",
                oi=1  # Request OI data
            )
            if data and len(data) > 0:
                candle = data[0]
                
                # Debug first candle to see format
                if idx == 0:
                    logger.info(f"Historical data with oi=1 - Type: {type(candle)}, Data: {candle}")
                
                # Handle both dict and list formats
                volume = 0
                oi_hist = 0
                
                if isinstance(candle, dict):
                    # Dict format: {'date': ..., 'open': ..., 'oi': ...}
                    volume = candle.get('volume', 0)
                    oi_hist = candle.get('oi', 0)
                elif isinstance(candle, (list, tuple)) and len(candle) >= 7:
                    # List format: [date, open, high, low, close, volume, oi]
                    volume = candle[5] if len(candle) > 5 else 0
                    oi_hist = candle[6] if len(candle) > 6 else 0
                
                total_volume += volume
                total_oi += oi_hist  # Add historical OI (can be 0)
                
                if idx == 0 and oi_hist > 0:
                    logger.info(f"Historical OI found for first strike: {oi_hist}")
        except Exception as e:
            logger.error(f"Error fetching data for strike {option['strike']}: {e}")
            continue
    
    logger.info(f"Total {option_type} - Volume: {total_volume:,} contracts, OI: {total_oi:,} contracts")
    return total_volume, total_oi


def get_live_volume_all_strikes(symbol, option_type, expiry_date):
    """
    Get current live volume across ALL strikes for a symbol
    Returns: (total_volume_all_strikes, strike_to_volume_dict)
    - total_volume_all_strikes: sum of ALL strikes volumes
    - strike_to_volume_dict: {strike_price: volume} for filtering specific strikes
    """
    matching_options = []
    for trading_symbol, instrument in instruments_cache.items():
        if (instrument['name'] == symbol and 
            instrument['instrument_type'] == option_type and
            instrument['expiry'] == expiry_date):
            matching_options.append(instrument)
    
    if not matching_options:
        logger.warning(f"  No {option_type} options found for {symbol}")
        return 0, {}
    
    logger.info(f"  Found {len(matching_options)} {option_type} strikes for {symbol}")
    
    total_volume_qty = 0  # Total in contracts/quantity
    strike_to_volume = {}  # Map strike price to volume for filtering
    
    # Build mapping: instrument_key -> instrument data
    key_to_instrument = {}
    instrument_keys = []
    
    for opt in matching_options:
        key = f"NFO:{opt['tradingsymbol']}"
        key_to_instrument[key] = opt
        instrument_keys.append(key)
    
    # Process in batches of 200 (larger batches = fewer API calls = faster)
    batch_size = 200
    for i in range(0, len(instrument_keys), batch_size):
        batch = instrument_keys[i:i+batch_size]
        try:
            # Use RATE-LIMITED quote API which includes cumulative day volume
            quote_data = rate_limited_api_call(kite.quote, batch)
            batch_volume = 0
            valid_quotes = 0
            
            # Debug: Check first quote structure
            if i == 0 and len(batch) > 0:
                first_key = batch[0]
                if first_key in quote_data:
                    logger.info(f"    Sample quote data: {quote_data[first_key]}")
            
            for key in batch:
                if key in quote_data:
                    quote = quote_data[key]
                    instrument = key_to_instrument[key]
                    
                    # Get volume in quantity (contracts)
                    volume_qty = quote.get('volume', 0)
                    
                    # Store strike -> volume mapping
                    strike_price = instrument['strike']
                    strike_to_volume[strike_price] = volume_qty
                    
                    if volume_qty > 0:
                        total_volume_qty += volume_qty
                        batch_volume += volume_qty
                        valid_quotes += 1
            
            logger.info(f"    Batch {i//batch_size + 1}: {batch_volume:,} contracts ({valid_quotes}/{len(batch)} instruments with volume)")
        except Exception as e:
            logger.error(f"  Error fetching quote data batch {i//batch_size + 1}: {e}")
            # If rate limit error, wait longer before continuing
            if "Too many requests" in str(e):
                logger.warning(f"  Rate limit hit - waiting 2 seconds...")
                time.sleep(2)
            continue
    
    logger.info(f"  Total {option_type} volume: {total_volume_qty:,} contracts")
    return total_volume_qty, strike_to_volume


def filter_volume_for_strikes(strikes_list, strike_to_volume_dict):
    """
    Filter and sum volumes for specific strikes from the fetched data
    Args:
        strikes_list: List of strike prices to include
        strike_to_volume_dict: Dictionary {strike_price: volume} from API call
    Returns:
        Total volume for the specified strikes
    """
    total_volume = 0
    for strike_price in strikes_list:
        if strike_price in strike_to_volume_dict:
            total_volume += strike_to_volume_dict[strike_price]
    return total_volume


def get_live_oi_all_strikes(symbol, option_type, expiry_date):
    """
    Get current live OI across ALL strikes for a symbol
    Total = sum of all strikes' OI in contracts
    """
    matching_options = []
    for trading_symbol, instrument in instruments_cache.items():
        if (instrument['name'] == symbol and 
            instrument['instrument_type'] == option_type and
            instrument['expiry'] == expiry_date):
            matching_options.append(instrument)
    
    if not matching_options:
        logger.warning(f"  No {option_type} options found for {symbol}")
        return 0
    
    logger.info(f"  Found {len(matching_options)} {option_type} strikes for {symbol}")
    
    total_oi = 0  # Total in contracts/quantity
    # Build mapping: instrument_key -> instrument data
    key_to_instrument = {}
    instrument_keys = []
    
    for opt in matching_options:
        key = f"NFO:{opt['tradingsymbol']}"
        key_to_instrument[key] = opt
        instrument_keys.append(key)
    
    # Process in batches of 100 (optimized for rate limits)
    batch_size = 100
    for i in range(0, len(instrument_keys), batch_size):
        batch = instrument_keys[i:i+batch_size]
        try:
            # Use rate-limited quote API which includes OI
            quote_data = rate_limited_api_call(kite.quote, batch)
            batch_oi = 0
            valid_quotes = 0
            
            for key in batch:
                if key in quote_data:
                    quote = quote_data[key]
                    # Get OI in quantity (contracts)
                    oi_qty = quote.get('oi', 0)
                    
                    if oi_qty > 0:
                        total_oi += oi_qty
                        batch_oi += oi_qty
                        valid_quotes += 1
            
            logger.info(f"    Batch {i//batch_size + 1}: {batch_oi:,} contracts ({valid_quotes}/{len(batch)} instruments with OI)")
        except Exception as e:
            logger.error(f"  Error fetching quote data batch {i//batch_size + 1}: {e}")
            # If rate limit error, wait longer before continuing
            if "Too many requests" in str(e):
                logger.warning(f"  Rate limit hit for {symbol} {option_type} - waiting 1 second...")
                time.sleep(1)
            continue
    
    logger.info(f"  Total {option_type} OI: {total_oi:,} contracts")
    return total_oi


def get_spot_price(symbol):
    """Get current spot price for a symbol"""
    try:
        # Try to get spot price from NSE
        quote_key = f"NSE:{symbol}"
        quote_data = rate_limited_api_call(kite.quote, [quote_key])
        
        if quote_key in quote_data:
            spot_price = quote_data[quote_key].get('last_price', 0)
            logger.info(f"[{symbol}] Spot price: {spot_price}")
            return spot_price
        return None
    except Exception as e:
        logger.error(f"Error getting spot price for {symbol}: {e}")
        return None


def get_instrument_token(symbol):
    """Get instrument token for a symbol (same method as historical.html page)"""
    try:
        # Determine exchange
        if symbol in ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]:
            exchange = "NSE"
        elif symbol == "SENSEX":
            exchange = "BSE"
        else:
            exchange = "NSE"
        
        # Get instruments for the exchange
        instruments = kite.instruments(exchange)
        
        # First try to find as index (for NIFTY, BANKNIFTY, etc.)
        if symbol in ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"]:
            for inst in instruments:
                if inst['name'] == symbol and inst['instrument_type'] == 'INDEX':
                    return inst['instrument_token']
        
        # If not found as index, try as EQ (for stocks)
        for inst in instruments:
            if inst['name'] == symbol and inst['instrument_type'] == 'EQ':
                return inst['instrument_token']
        
        # If still not found, try any match
        for inst in instruments:
            if inst['name'] == symbol:
                return inst['instrument_token']
        
        logger.warning(f"Could not find instrument token for {symbol} in exchange {exchange}")
        return None
    except Exception as e:
        logger.error(f"Error getting instrument token for {symbol}: {e}")
        return None


def get_historical_spot_price(symbol, target_date):
    """Get historical spot price for a symbol at 3:30pm (EXACT same method as historical.html page)"""
    try:
        # Handle both datetime and date objects
        if isinstance(target_date, datetime):
            target_date_obj = target_date.date()
        else:
            target_date_obj = target_date
        
        # Create target datetime for 3:30pm (EXACT same as historical data route)
        target_datetime = IST.localize(datetime.combine(target_date_obj, datetime.min.time().replace(hour=15, minute=30)))
        
        logger.info(f"[{symbol}] Getting historical spot price at 3:30pm for date: {target_datetime}")
        
        # EXACT same code as historical data route (lines 2986-3011)
        spot_price = None
        try:
            if symbol in ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]:
                spot_key = f"NSE:{symbol}"
            elif symbol == "SENSEX":
                spot_key = f"BSE:{symbol}"
            else:
                spot_key = f"NSE:{symbol}"
            
            # Get instrument token for spot (EXACT same method)
            spot_instrument_token = get_instrument_token(symbol)
            if spot_instrument_token:
                # Use kite.historical_data directly (EXACT same as historical route, NOT rate_limited)
                spot_hist = kite.historical_data(
                    instrument_token=spot_instrument_token,
                    from_date=target_datetime.date(),
                    to_date=target_datetime.date(),
                    interval='minute'
                )
                
                if spot_hist:
                    # Find closest time (EXACT same method)
                    closest_idx = min(range(len(spot_hist)), 
                                    key=lambda i: abs((spot_hist[i]['date'] - target_datetime).total_seconds()))
                    spot_price = spot_hist[closest_idx]['close']
                    actual_time = spot_hist[closest_idx]['date']
                    logger.info(f"[{symbol}] Historical spot price at {target_datetime}: {spot_price} (from minute data at {actual_time})")
        except Exception as e:
            logger.warning(f"[{symbol}] Could not fetch historical spot price: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
        
        if spot_price and spot_price > 0:
            return spot_price
        else:
            logger.warning(f"[{symbol}] Spot price is None or 0, returning None")
            return None
            
    except Exception as e:
        logger.error(f"[{symbol}] Error getting historical spot price: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def get_oi_for_strikes_list(symbol, strikes_list, option_type, expiry_date):
    """
    Get live OI for a specific list of strikes using BATCH API calls
    Returns total OI across the specified strikes
    """
    if not strikes_list:
        return 0
    
    total_oi = 0
    
    # Build list of instrument keys for batch quote API call
    instrument_keys = []
    strike_to_key = {}  # Map strike price to instrument key for logging
    
    for strike_price in strikes_list:
        # Find the instrument for this strike
        for trading_symbol, instrument in instruments_cache.items():
            if (instrument['name'] == symbol and 
                instrument['instrument_type'] == option_type and
                instrument['expiry'] == expiry_date and
                instrument['strike'] == strike_price):
                
                quote_key = f"NFO:{trading_symbol}"
                instrument_keys.append(quote_key)
                strike_to_key[strike_price] = quote_key
                break
    
    if not instrument_keys:
        logger.warning(f"[{symbol}] No instruments found for {option_type} strikes: {strikes_list}")
        return 0
    
    try:
        # Fetch all strikes in ONE batch API call (much faster!)
        quote_data = rate_limited_api_call(kite.quote, instrument_keys)
        
        # Store strike details for summary
        strike_details = []
        
        # Process results in sorted order
        for strike_price in sorted(strike_to_key.keys()):
            quote_key = strike_to_key[strike_price]
            if quote_key in quote_data:
                oi = quote_data[quote_key].get('oi', 0)
                total_oi += oi
                strike_details.append((strike_price, oi))
                logger.info(f"[{symbol}] {option_type} Strike {strike_price}: OI = {oi:,} contracts")
            else:
                logger.warning(f"[{symbol}] {option_type} Strike {strike_price}: No data available")
        
        # Summary
        logger.info("="*80)
        logger.info(f"[{symbol}] {option_type} SUMMARY - Total {len(strike_details)} strikes summed:")
        logger.info(f"[{symbol}] {option_type} Strike Range: {min(strike_to_key.keys())} to {max(strike_to_key.keys())}")
        logger.info(f"[{symbol}] {option_type} ✅ TOTAL OI = {total_oi:,} contracts")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"Error fetching live OI batch for {symbol} {option_type}: {e}")
        return 0
    
    return total_oi


def get_oi_near_atm_strikes(symbol, spot_price, option_type, expiry_date, num_strikes=21):
    """
    Get live OI for near ATM strikes (default: spot ± 10 strikes = 21 total)
    """
    # Find near ATM strikes
    strikes_list = find_nearest_strikes(symbol, spot_price, expiry_date, option_type, num_strikes)
    
    # Get OI for these strikes
    total_oi = get_oi_for_strikes_list(symbol, strikes_list, option_type, expiry_date)
    
    return total_oi


def get_oi_near_atm_strikes_historical(symbol, spot_price, option_type, expiry_date, from_date, to_date, num_strikes=21):
    """
    Get historical OI for near ATM strikes (default: spot ± 10 strikes = 21 total)
    """
    # Find near ATM strikes around spot price
    strikes_list = find_nearest_strikes(symbol, spot_price, expiry_date, option_type, num_strikes)
    
    if not strikes_list:
        return 0
    
    total_oi = 0
    
    for strike_price in strikes_list:
        strike_oi = get_strike_oi_historical(symbol, strike_price, option_type, expiry_date, from_date, to_date)
        total_oi += strike_oi
        logger.info(f"[{symbol}] Historical {option_type} Strike {strike_price}: OI = {strike_oi}")
    
    logger.info(f"[{symbol}] Total Historical {option_type} OI for {len(strikes_list)} near ATM strikes: {total_oi:,} contracts")
    return total_oi


def find_nearest_strikes(symbol, spot_price, expiry_date, option_type, num_strikes=5):
    """
    Find nearest strikes around spot price (spot ±2 for num_strikes=5, spot ±10 for num_strikes=21)
    Returns list of strike prices sorted
    """
    # Get all available strikes for this symbol
    available_strikes = []
    for trading_symbol, instrument in instruments_cache.items():
        if (instrument['name'] == symbol and 
            instrument['instrument_type'] == option_type and
            instrument['expiry'] == expiry_date):
            available_strikes.append(instrument['strike'])
    
    if not available_strikes:
        return []
    
    # Sort strikes
    available_strikes = sorted(set(available_strikes))
    
    # Find closest strike to spot and use it as CENTER
    closest_strike = min(available_strikes, key=lambda x: abs(x - spot_price))
    closest_idx = available_strikes.index(closest_strike)
    
    # Calculate how many strikes on each side
    # For num_strikes=21: strikes_per_side = (21-1)/2 = 10
    # This gives us: 10 below + center strike + 10 above = 21 total
    strikes_per_side = (num_strikes - 1) // 2
    
    # Select EXACTLY 10 strikes below center and 10 strikes above center
    start_idx = closest_idx - strikes_per_side
    end_idx = closest_idx + strikes_per_side + 1
    
    # Handle edge cases (if not enough strikes on one side)
    if start_idx < 0:
        # Not enough strikes below, shift up
        end_idx += abs(start_idx)
        start_idx = 0
    
    if end_idx > len(available_strikes):
        # Not enough strikes above, shift down
        overflow = end_idx - len(available_strikes)
        start_idx = max(0, start_idx - overflow)
        end_idx = len(available_strikes)
    
    selected_strikes = available_strikes[start_idx:end_idx]
    
    # Log detailed strike selection
    strikes_below = [s for s in selected_strikes if s < closest_strike]
    strikes_above = [s for s in selected_strikes if s > closest_strike]
    logger.info(f"[{symbol}] {option_type} - Original Spot: {spot_price} → Rounded to Strike: {closest_strike}")
    logger.info(f"[{symbol}] {option_type} - Selected {len(strikes_below)} below + 1 center ({closest_strike}) + {len(strikes_above)} above = {len(selected_strikes)} total strikes")
    logger.info(f"[{symbol}] {option_type} - Strike Range: {selected_strikes[0]} to {selected_strikes[-1]}")
    logger.info(f"[{symbol}] {option_type} - Strikes: {selected_strikes}")
    
    return selected_strikes[:num_strikes]


def get_strike_oi_historical(symbol, strike_price, option_type, expiry_date, from_date, to_date):
    """Get OI for a specific strike from historical data"""
    # Find the instrument
    for trading_symbol, instrument in instruments_cache.items():
        if (instrument['name'] == symbol and 
            instrument['instrument_type'] == option_type and
            instrument['expiry'] == expiry_date and
            instrument['strike'] == strike_price):
            
            try:
                data = rate_limited_api_call(
                    kite.historical_data,
                    instrument_token=instrument['instrument_token'],
                    from_date=from_date,
                    to_date=to_date,
                    interval="day",
                    oi=1
                )
                
                if data and len(data) > 0:
                    candle = data[0]
                    
                    if isinstance(candle, dict):
                        return candle.get('oi', 0)
                    elif isinstance(candle, (list, tuple)) and len(candle) >= 7:
                        return candle[6] if len(candle) > 6 else 0
                
            except Exception as e:
                logger.error(f"Error fetching OI for {symbol} {strike_price} {option_type}: {e}")
                return 0
    
    return 0


def get_strike_oi_live(symbol, strike_price, option_type, expiry_date):
    """Get live OI for a specific strike"""
    # Find the instrument
    for trading_symbol, instrument in instruments_cache.items():
        if (instrument['name'] == symbol and 
            instrument['instrument_type'] == option_type and
            instrument['expiry'] == expiry_date and
            instrument['strike'] == strike_price):
            
            try:
                quote_key = f"NFO:{trading_symbol}"
                quote_data = rate_limited_api_call(kite.quote, [quote_key])
                
                if quote_key in quote_data:
                    return quote_data[quote_key].get('oi', 0)
                
            except Exception as e:
                logger.error(f"Error fetching live OI for {symbol} {strike_price} {option_type}: {e}")
                return 0
    
    return 0


def clean_old_alerts():
    """Delete volume and OI alerts from previous days - runs at 9:00 AM"""
    try:
        current_time = datetime.now(IST)
        # Check if it's 9:00 AM (within a 1-minute window)
        if current_time.hour == 9 and current_time.minute == 0:
            today_start = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Delete old volume alerts
            vol_result = alerts_collection.delete_many({'timestamp': {'$lt': today_start}})
            logger.info(f"[9:00 AM Cleanup] Deleted {vol_result.deleted_count} old volume alerts")
            
            # Delete old OI alerts (Total OI alerts)
            oi_result = oi_alerts_collection.delete_many({'timestamp': {'$lt': today_start}})
            logger.info(f"[9:00 AM Cleanup] Deleted {oi_result.deleted_count} old OI alerts")
            
            # Delete old OI Change alerts
            oi_change_result = oi_change_alerts_collection.delete_many({'timestamp': {'$lt': today_start}})
            logger.info(f"[9:00 AM Cleanup] Deleted {oi_change_result.deleted_count} old OI Change alerts")
            
            # Delete old OI Strike alerts
            oi_strike_result = oi_strike_alerts_collection.delete_many({'timestamp': {'$lt': today_start}})
            logger.info(f"[9:00 AM Cleanup] Deleted {oi_strike_result.deleted_count} old OI Strike alerts")
            
            total_deleted = vol_result.deleted_count + oi_result.deleted_count + oi_change_result.deleted_count + oi_strike_result.deleted_count
            logger.info(f"[9:00 AM Cleanup] Total alerts deleted: {total_deleted}")
            return total_deleted
        return 0
    except Exception as e:
        logger.error(f"Error cleaning old alerts: {e}")
        return 0


# ============================================================================
# OPTIONS CHAIN & HISTORICAL DATA FUNCTIONS
# ============================================================================

def get_expiry_dates_for_symbol(symbol):
    """Get available expiry dates for options"""
    try:
        # Determine exchange
        if symbol in ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]:
            exchange = "NFO"
        elif symbol == "SENSEX":
            exchange = "BFO"
        else:
            exchange = "NFO"
        
        logger.info(f"Fetching expiry dates for {symbol} from {exchange}")
        
        # Get all instruments for the exchange
        instruments = kite.instruments(exchange)
        
        # Filter instruments for the selected symbol and get expiry dates
        expiry_dates = set()
        for instrument in instruments:
            instrument_name = instrument.get('name', '')
            trading_symbol = instrument.get('tradingsymbol', '')
            
            if (instrument_name == symbol or trading_symbol.startswith(symbol)) and \
               instrument.get('instrument_type') in ['CE', 'PE']:
                if instrument.get('expiry'):
                    expiry_dates.add(instrument['expiry'])
        
        logger.info(f"Found {len(expiry_dates)} expiry dates for {symbol}")
        
        # Sort expiry dates
        expiry_list = sorted(list(expiry_dates))
        
        # Format dates for display
        formatted_expiries = []
        for expiry in expiry_list:
            formatted_expiries.append({
                'date': expiry.strftime('%Y-%m-%d'),
                'display': expiry.strftime('%d %b %Y')
            })
        
        return formatted_expiries
        
    except Exception as e:
        logger.error(f"Error fetching expiry dates for {symbol}: {str(e)}")
        return []


def get_options_chain_data(symbol, expiry_date):
    """Get options chain data for a symbol and expiry date"""
    try:
        # Determine exchange
        if symbol in ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]:
            exchange = "NFO"
        elif symbol == "SENSEX":
            exchange = "BFO"
        else:
            exchange = "NFO"
        
        logger.info(f"Fetching options chain for {symbol} expiry {expiry_date} from {exchange}")
        
        # Get all instruments
        instruments = kite.instruments(exchange)
        
        # Convert expiry_date string to datetime
        expiry_dt = datetime.strptime(expiry_date, '%Y-%m-%d').date()
        
        # Filter options for the symbol and expiry
        calls = []
        puts = []
        
        for instrument in instruments:
            instrument_name = instrument.get('name', '')
            trading_symbol = instrument.get('tradingsymbol', '')
            instrument_expiry = instrument.get('expiry')
            
            if instrument_expiry:
                if hasattr(instrument_expiry, 'date'):
                    instrument_expiry = instrument_expiry.date()
                    
            if ((instrument_name == symbol or trading_symbol.startswith(symbol)) and 
                instrument_expiry and 
                instrument_expiry == expiry_dt):
                
                lot_size = instrument.get('lot_size', 1)
                
                if instrument.get('instrument_type') == 'CE':
                    calls.append({
                        'strike': instrument['strike'],
                        'tradingsymbol': instrument['tradingsymbol'],
                        'instrument_token': instrument['instrument_token'],
                        'lot_size': lot_size
                    })
                elif instrument.get('instrument_type') == 'PE':
                    puts.append({
                        'strike': instrument['strike'],
                        'tradingsymbol': instrument['tradingsymbol'],
                        'instrument_token': instrument['instrument_token'],
                        'lot_size': lot_size
                    })
        
        logger.info(f"Found {len(calls)} calls and {len(puts)} puts for {symbol}")
        
        # Sort by strike price
        calls.sort(key=lambda x: x['strike'])
        puts.sort(key=lambda x: x['strike'])
        
        # Get live spot price
        spot_price = None
        try:
            if symbol in ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]:
                spot_key = f"NSE:{symbol}"
            elif symbol == "SENSEX":
                spot_key = f"BSE:{symbol}"
            else:
                spot_key = f"NSE:{symbol}"
            
            spot_quote = kite.quote([spot_key])
            if spot_quote and spot_key in spot_quote:
                spot_price = spot_quote[spot_key].get('last_price', 0)
                if spot_price and spot_price > 0:
                    logger.info(f"Live spot price for {symbol}: {spot_price}")
                else:
                    logger.error(f"Invalid spot price received: {spot_price}")
                    return []
            else:
                logger.error(f"No spot quote data received for {symbol}")
                return []
        except Exception as e:
            logger.error(f"Error fetching spot price: {str(e)}")
            return []
        
        # Get unique strike prices
        all_strikes = sorted(set([c['strike'] for c in calls] + [p['strike'] for p in puts]))
        
        if not all_strikes:
            logger.error("No strikes available")
            return []
        
        # Find ATM strike - Round to NEAREST strike (e.g., 423.5 -> 425, 422 -> 420)
        atm_strike = min(all_strikes, key=lambda x: abs(x - spot_price))
        
        # Get ATM strike index
        atm_index = all_strikes.index(atm_strike)
        
        # Select 21 strikes: ATM + 10 below + 10 above = 21 total
        start_index = max(0, atm_index - 10)
        end_index = min(len(all_strikes), atm_index + 11)  # +11 because we want ATM + 10 above
        
        display_strikes = all_strikes[start_index:end_index]
        
        logger.info(f"Live spot: {spot_price}, ATM strike (rounded to nearest): {atm_strike}, Showing {len(display_strikes)} strikes (ATM ±10)")
        
        # Create mappings
        calls_dict = {c['strike']: c for c in calls}
        puts_dict = {p['strike']: p for p in puts}
        
        # Get quotes with OI
        call_tokens = [f"{exchange}:{c['tradingsymbol']}" for c in calls]
        put_tokens = [f"{exchange}:{p['tradingsymbol']}" for p in puts]
        all_tokens = call_tokens + put_tokens
        
        quotes = {}
        batch_size = 500
        for i in range(0, len(all_tokens), batch_size):
            batch = all_tokens[i:i+batch_size]
            try:
                batch_quotes = kite.quote(batch)
                quotes.update(batch_quotes)
                # Log first quote to debug
                if i == 0 and batch_quotes:
                    sample_key = list(batch_quotes.keys())[0]
                    sample_quote = batch_quotes[sample_key]
                    logger.info(f"Sample quote for {sample_key}:")
                    logger.info(f"  - Volume: {sample_quote.get('volume', 'N/A')}")
                    logger.info(f"  - OI: {sample_quote.get('oi', 'N/A')}")
                    logger.info(f"  - Last Price: {sample_quote.get('last_price', 'N/A')}")
            except Exception as e:
                logger.error(f"Error fetching quotes batch: {str(e)}")
        
        # Get previous day OI - CRITICAL for Change in OI calculation
        previous_day_oi = {}
        previous_day = get_previous_trading_day()
        logger.info(f"🔍 Fetching previous day OI from: {previous_day.date()}")
        
        # Process first 3 calls for debugging
        for idx, call in enumerate(calls[:3]):
            try:
                hist_data = kite.historical_data(
                    instrument_token=call['instrument_token'],
                    from_date=previous_day.date(),
                    to_date=previous_day.date(),
                    interval='day',
                    oi=1
                )
                
                if hist_data and len(hist_data) > 0:
                    candle = hist_data[0]
                    prev_oi_qty = candle.get('oi', 0) if isinstance(candle, dict) else (candle[6] if len(candle) >= 7 else 0)
                    
                    if prev_oi_qty > 0:
                        prev_oi_lots = prev_oi_qty / call['lot_size'] if call['lot_size'] > 0 else 0
                        previous_day_oi[call['tradingsymbol']] = prev_oi_lots
                        logger.info(f"✅ CALL {call['tradingsymbol']}: Prev day OI = {prev_oi_lots:.0f} lots (from {prev_oi_qty} qty)")
                    else:
                        logger.warning(f"⚠️ CALL {call['tradingsymbol']}: Previous day OI is 0")
                else:
                    logger.warning(f"⚠️ CALL {call['tradingsymbol']}: No historical data for {previous_day.date()}")
            except Exception as e:
                logger.error(f"❌ CALL {call['tradingsymbol']}: Error fetching prev OI - {str(e)}")
        
        # Process remaining calls silently
        for call in calls[3:]:
            try:
                hist_data = kite.historical_data(
                    instrument_token=call['instrument_token'],
                    from_date=previous_day.date(),
                    to_date=previous_day.date(),
                    interval='day',
                    oi=1
                )
                if hist_data and len(hist_data) > 0:
                    candle = hist_data[0]
                    prev_oi_qty = candle.get('oi', 0) if isinstance(candle, dict) else (candle[6] if len(candle) >= 7 else 0)
                    if prev_oi_qty > 0:
                        prev_oi_lots = prev_oi_qty / call['lot_size'] if call['lot_size'] > 0 else 0
                        previous_day_oi[call['tradingsymbol']] = prev_oi_lots
            except Exception as e:
                pass
        
        # Process first 3 puts for debugging
        for idx, put in enumerate(puts[:3]):
            try:
                hist_data = kite.historical_data(
                    instrument_token=put['instrument_token'],
                    from_date=previous_day.date(),
                    to_date=previous_day.date(),
                    interval='day',
                    oi=1
                )
                
                if hist_data and len(hist_data) > 0:
                    candle = hist_data[0]
                    prev_oi_qty = candle.get('oi', 0) if isinstance(candle, dict) else (candle[6] if len(candle) >= 7 else 0)
                    
                    if prev_oi_qty > 0:
                        prev_oi_lots = prev_oi_qty / put['lot_size'] if put['lot_size'] > 0 else 0
                        previous_day_oi[put['tradingsymbol']] = prev_oi_lots
                        logger.info(f"✅ PUT {put['tradingsymbol']}: Prev day OI = {prev_oi_lots:.0f} lots (from {prev_oi_qty} qty)")
                    else:
                        logger.warning(f"⚠️ PUT {put['tradingsymbol']}: Previous day OI is 0")
                else:
                    logger.warning(f"⚠️ PUT {put['tradingsymbol']}: No historical data for {previous_day.date()}")
            except Exception as e:
                logger.error(f"❌ PUT {put['tradingsymbol']}: Error fetching prev OI - {str(e)}")
        
        # Process remaining puts silently
        for put in puts[3:]:
            try:
                hist_data = kite.historical_data(
                    instrument_token=put['instrument_token'],
                    from_date=previous_day.date(),
                    to_date=previous_day.date(),
                    interval='day',
                    oi=1
                )
                if hist_data and len(hist_data) > 0:
                    candle = hist_data[0]
                    prev_oi_qty = candle.get('oi', 0) if isinstance(candle, dict) else (candle[6] if len(candle) >= 7 else 0)
                    if prev_oi_qty > 0:
                        prev_oi_lots = prev_oi_qty / put['lot_size'] if put['lot_size'] > 0 else 0
                        previous_day_oi[put['tradingsymbol']] = prev_oi_lots
            except Exception as e:
                pass
        
        logger.info(f"📊 Previous day OI fetched for {len(previous_day_oi)} instruments out of {len(calls) + len(puts)} total")
        
        # Build options chain - Calculate totals ONLY for displayed 21 strikes
        options_chain = []
        
        # Initialize totals for ONLY the 21 displayed strikes
        total_call_volume = 0
        total_put_volume = 0
        total_call_oi = 0
        total_put_oi = 0
        total_call_change_oi = 0
        total_put_change_oi = 0
        
        for strike in display_strikes:
            row = {
                'strike': strike,
                'is_atm': strike == atm_strike,  # Highlight the ATM strike (rounded up from spot)
                'spot_price': spot_price
            }
            
            # Put data
            if strike in puts_dict:
                put = puts_dict[strike]
                put_key = f"{exchange}:{put['tradingsymbol']}"
                put_quote = quotes.get(put_key, {})
                lot_size = put.get('lot_size', 1)
                tradingsymbol = put['tradingsymbol']
                
                ohlc = put_quote.get('ohlc', {})
                prev_close = ohlc.get('close', 0)
                last_price = put_quote.get('last_price', 0)
                change = put_quote.get('change', 0)
                
                oi_qty = put_quote.get('oi', 0)
                volume_qty = put_quote.get('volume', 0)
                oi_lots = oi_qty / lot_size if lot_size > 0 else 0
                volume_lots = volume_qty / lot_size if lot_size > 0 else 0
                prev_oi_lots = previous_day_oi.get(tradingsymbol, 0)
                change_in_oi_lots = oi_lots - prev_oi_lots
                
                # Debug calculation for first 2 strikes
                if strike in list(puts_dict.keys())[:2]:
                    logger.info(f"🔢 PUT Strike {strike} ({tradingsymbol}):")
                    logger.info(f"   Current OI: {oi_lots:.0f} lots (from {oi_qty} qty)")
                    logger.info(f"   Prev Day OI: {prev_oi_lots:.0f} lots")
                    logger.info(f"   Change in OI: {change_in_oi_lots:.0f} lots (Current - Previous)")
                    logger.info(f"   Volume: {volume_lots:.0f} lots (from {volume_qty} qty)")
                
                row['put'] = {
                    'oi': oi_lots,
                    'change_in_oi': change_in_oi_lots,
                    'volume': volume_lots,
                    'iv': put_quote.get('iv', 0),
                    'ltp': last_price,
                    'change': change,
                    'change_percent': put_quote.get('change_percent', 0),
                    'prev_close': prev_close,
                    'lot_size': lot_size,
                    'bid_qty': put_quote.get('depth', {}).get('buy', [{}])[0].get('quantity', 0) if put_quote.get('depth', {}).get('buy') else 0,
                    'bid_price': put_quote.get('depth', {}).get('buy', [{}])[0].get('price', 0) if put_quote.get('depth', {}).get('buy') else 0,
                    'ask_price': put_quote.get('depth', {}).get('sell', [{}])[0].get('price', 0) if put_quote.get('depth', {}).get('sell') else 0,
                    'ask_qty': put_quote.get('depth', {}).get('sell', [{}])[0].get('quantity', 0) if put_quote.get('depth', {}).get('sell') else 0,
                }
                
                # Add to PUT totals (ONLY for displayed strikes)
                total_put_volume += volume_lots
                total_put_oi += oi_lots
                total_put_change_oi += change_in_oi_lots
            else:
                row['put'] = None
            
            # Call data
            if strike in calls_dict:
                call = calls_dict[strike]
                call_key = f"{exchange}:{call['tradingsymbol']}"
                call_quote = quotes.get(call_key, {})
                lot_size = call.get('lot_size', 1)
                tradingsymbol = call['tradingsymbol']
                
                ohlc = call_quote.get('ohlc', {})
                prev_close = ohlc.get('close', 0)
                last_price = call_quote.get('last_price', 0)
                change = call_quote.get('change', 0)
                
                oi_qty = call_quote.get('oi', 0)
                volume_qty = call_quote.get('volume', 0)
                oi_lots = oi_qty / lot_size if lot_size > 0 else 0
                volume_lots = volume_qty / lot_size if lot_size > 0 else 0
                prev_oi_lots = previous_day_oi.get(tradingsymbol, 0)
                change_in_oi_lots = oi_lots - prev_oi_lots
                
                # Debug calculation for first 2 strikes
                if strike in list(calls_dict.keys())[:2]:
                    logger.info(f"🔢 CALL Strike {strike} ({tradingsymbol}):")
                    logger.info(f"   Current OI: {oi_lots:.0f} lots (from {oi_qty} qty)")
                    logger.info(f"   Prev Day OI: {prev_oi_lots:.0f} lots")
                    logger.info(f"   Change in OI: {change_in_oi_lots:.0f} lots (Current - Previous)")
                    logger.info(f"   Volume: {volume_lots:.0f} lots (from {volume_qty} qty)")
                
                row['call'] = {
                    'oi': oi_lots,
                    'change_in_oi': change_in_oi_lots,
                    'volume': volume_lots,
                    'iv': call_quote.get('iv', 0),
                    'ltp': last_price,
                    'change': change,
                    'change_percent': call_quote.get('change_percent', 0),
                    'prev_close': prev_close,
                    'lot_size': lot_size,
                    'bid_qty': call_quote.get('depth', {}).get('buy', [{}])[0].get('quantity', 0) if call_quote.get('depth', {}).get('buy') else 0,
                    'bid_price': call_quote.get('depth', {}).get('buy', [{}])[0].get('price', 0) if call_quote.get('depth', {}).get('buy') else 0,
                    'ask_price': call_quote.get('depth', {}).get('sell', [{}])[0].get('price', 0) if call_quote.get('depth', {}).get('sell') else 0,
                    'ask_qty': call_quote.get('depth', {}).get('sell', [{}])[0].get('quantity', 0) if call_quote.get('depth', {}).get('sell') else 0,
                }
                
                # Add to CALL totals (ONLY for displayed strikes)
                total_call_volume += volume_lots
                total_call_oi += oi_lots
                total_call_change_oi += change_in_oi_lots
            else:
                row['call'] = None
            
            options_chain.append(row)
        
        # Add totals to metadata
        if options_chain:
            options_chain[0]['total_call_volume'] = total_call_volume
            options_chain[0]['total_put_volume'] = total_put_volume
            options_chain[0]['total_call_oi'] = total_call_oi
            options_chain[0]['total_put_oi'] = total_put_oi
            options_chain[0]['total_call_change_oi'] = total_call_change_oi
            options_chain[0]['total_put_change_oi'] = total_put_change_oi
            
            # Log totals for verification
            logger.info(f"📊 Options Chain Totals (ONLY {len(options_chain)} displayed strikes):")
            logger.info(f"  CALL: Volume={total_call_volume:.0f}, OI={total_call_oi:.0f}, Change OI={total_call_change_oi:.0f}")
            logger.info(f"  PUT: Volume={total_put_volume:.0f}, OI={total_put_oi:.0f}, Change OI={total_put_change_oi:.0f}")
        
        return options_chain
        
    except Exception as e:
        logger.error(f"Error fetching options chain for {symbol}: {str(e)}")
        return []


# ============================================================================
# ROUTES - MAIN PAGES
# ============================================================================

@app.route('/')
def index():
    """Render the main page"""
    return render_template('index.html')


@app.route('/oi-monitor')
def oi_monitor_page():
    """Render the OI monitor page"""
    return render_template('oi_monitor.html')


@app.route('/oi-alerts')
def oi_alerts_page():
    """Render the OI alerts page"""
    return render_template('oi_alerts.html')


@app.route('/oi-change-alerts')
def oi_change_alerts_page():
    """Render the OI change alerts page (strike level monitoring)"""
    return render_template('oi_change_alerts.html')


@app.route('/api/refresh-previous-day-data', methods=['POST'])
def refresh_previous_day_data():
    """Delete all previous data and fetch fresh previous day volume data"""
    try:
        logger.info("="*80)
        logger.info("REFRESHING PREVIOUS DAY DATA")
        logger.info("="*80)
        
        # Delete all existing data (but preserve prev_day_eod_total_oi_collection - it's manually stored via "Store Live EOD OI" button)
        volume_collection.delete_many({})
        oi_collection.delete_many({})
        oi_strikes_collection.delete_many({})
        oi_near_atm_collection.delete_many({})
        # NOTE: prev_day_eod_total_oi_collection is NOT deleted here - it's manually stored via "Store Live EOD OI" button
        logger.info("Deleted all existing volume, OI, OI strikes, and near ATM OI data from MongoDB (preserved EOD total OI data)")
        
        # Load instruments cache
        load_instruments_cache()
        
        previous_day = get_previous_trading_day()
        logger.info(f"Previous trading day: {previous_day.strftime('%Y-%m-%d')}")
        
        # Filter out major indices
        major_indices = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"]
        stocks_to_process = [s for s in config.STOCK_SYMBOLS if s not in major_indices]
        
        logger.info(f"Processing {len(stocks_to_process)} stocks")
        
        processed_count = 0
        failed_count = 0
        
        for idx, symbol in enumerate(stocks_to_process, 1):
            try:
                logger.info(f"[{idx}/{len(stocks_to_process)}] Processing {symbol}...")
                
                # Get nearest expiry
                expiry_date = get_nearest_expiry(symbol)
                if not expiry_date:
                    logger.warning(f"No expiry found for {symbol}")
                    failed_count += 1
                    continue
                
                # Get lot size
                lot_size = get_lot_size_for_symbol(symbol)
                if not lot_size:
                    logger.warning(f"No lot size found for {symbol}")
                    failed_count += 1
                    continue
                
                # Get previous day volumes AND OI in single API call - ALL STRIKES
                logger.info(f"Fetching previous day volume and OI for {symbol}...")
                
                prev_call_volume, prev_call_oi = get_total_volume_and_oi_all_strikes(
                    symbol, 'CE', expiry_date,
                    previous_day,
                    previous_day + timedelta(days=1)
                )
                
                prev_put_volume, prev_put_oi = get_total_volume_and_oi_all_strikes(
                    symbol, 'PE', expiry_date,
                    previous_day,
                    previous_day + timedelta(days=1)
                )
                
                # Convert to lots
                prev_call_lots = prev_call_volume / lot_size if lot_size > 0 else 0
                prev_put_lots = prev_put_volume / lot_size if lot_size > 0 else 0
                prev_call_oi_lots = prev_call_oi / lot_size if lot_size > 0 else 0
                prev_put_oi_lots = prev_put_oi / lot_size if lot_size > 0 else 0
                
                # Store volume in MongoDB
                volume_collection.insert_one({
                    'symbol': symbol,
                    'date': previous_day.strftime('%Y-%m-%d'),
                    'expiry': expiry_date.strftime('%Y-%m-%d'),
                    'lot_size': lot_size,
                    'call_volume': int(prev_call_volume),
                    'put_volume': int(prev_put_volume),
                    'call_lots': round(prev_call_lots, 2),
                    'put_lots': round(prev_put_lots, 2),
                    'timestamp': datetime.now(IST)
                })
                
                # Store OI in MongoDB (in LOTS, same as volume)
                oi_collection.insert_one({
                    'symbol': symbol,
                    'date': previous_day.strftime('%Y-%m-%d'),
                    'expiry': expiry_date.strftime('%Y-%m-%d'),
                    'lot_size': lot_size,
                    'call_oi': int(prev_call_oi),
                    'put_oi': int(prev_put_oi),
                    'call_oi_lots': round(prev_call_oi_lots, 2),
                    'put_oi_lots': round(prev_put_oi_lots, 2),
                    'timestamp': datetime.now(IST)
                })
                
                # NEW: Store ALL strikes' historical OI for flexible near ATM comparison
                # This allows us to dynamically select strikes based on current spot during monitoring
                try:
                    logger.info(f"[{symbol}] Fetching ALL strikes' historical OI for flexible comparison...")
                    
                    # Get all CE strikes
                    ce_instruments = []
                    for trading_symbol, instrument in instruments_cache.items():
                        if (instrument['name'] == symbol and 
                            instrument['instrument_type'] == 'CE' and
                            instrument['expiry'] == expiry_date):
                            ce_instruments.append(instrument)
                    
                    # Get all PE strikes
                    pe_instruments = []
                    for trading_symbol, instrument in instruments_cache.items():
                        if (instrument['name'] == symbol and 
                            instrument['instrument_type'] == 'PE' and
                            instrument['expiry'] == expiry_date):
                            pe_instruments.append(instrument)
                    
                    # Store CE strikes OI
                    ce_stored = 0
                    for instrument in ce_instruments:
                        strike_oi = get_strike_oi_historical(symbol, instrument['strike'], 'CE', expiry_date, previous_day, previous_day + timedelta(days=1))
                        if strike_oi > 0:
                            oi_near_atm_collection.insert_one({
                                'symbol': symbol,
                                'strike_price': instrument['strike'],
                                'option_type': 'CE',
                                'oi': int(strike_oi),
                                'oi_lots': round(strike_oi / lot_size, 2),
                                'date': previous_day.strftime('%Y-%m-%d'),
                                'expiry': expiry_date.strftime('%Y-%m-%d'),
                                'lot_size': lot_size,
                                'timestamp': datetime.now(IST)
                            })
                            ce_stored += 1
                    
                    # Store PE strikes OI
                    pe_stored = 0
                    for instrument in pe_instruments:
                        strike_oi = get_strike_oi_historical(symbol, instrument['strike'], 'PE', expiry_date, previous_day, previous_day + timedelta(days=1))
                        if strike_oi > 0:
                            oi_near_atm_collection.insert_one({
                                'symbol': symbol,
                                'strike_price': instrument['strike'],
                                'option_type': 'PE',
                                'oi': int(strike_oi),
                                'oi_lots': round(strike_oi / lot_size, 2),
                                'date': previous_day.strftime('%Y-%m-%d'),
                                'expiry': expiry_date.strftime('%Y-%m-%d'),
                                'lot_size': lot_size,
                                'timestamp': datetime.now(IST)
                            })
                            pe_stored += 1
                    
                    logger.info(f"Stored ALL strikes OI for {symbol}: {ce_stored} CE strikes + {pe_stored} PE strikes")
                except Exception as near_atm_error:
                    logger.warning(f"Could not store all strikes OI for {symbol}: {near_atm_error}")
                
                # Store OI for individual strikes (spot ±2) for strike-level monitoring
                try:
                    # Get spot price from previous day (reuse if already fetched)
                    if 'spot_price' not in locals() or not spot_price:
                        spot_price = get_spot_price(symbol)
                    if spot_price:
                        # Get spot ±2 strikes for both CE and PE
                        ce_strikes = find_nearest_strikes(symbol, spot_price, expiry_date, 'CE', num_strikes=5)
                        pe_strikes = find_nearest_strikes(symbol, spot_price, expiry_date, 'PE', num_strikes=5)
                        
                        # Store OI for each CE strike
                        for strike in ce_strikes:
                            strike_oi = get_strike_oi_historical(symbol, strike, 'CE', expiry_date, previous_day, previous_day + timedelta(days=1))
                            if strike_oi > 0:
                                oi_strikes_collection.insert_one({
                                    'symbol': symbol,
                                    'strike_price': strike,
                                    'option_type': 'CE',
                                    'oi': int(strike_oi),
                                    'date': previous_day.strftime('%Y-%m-%d'),
                                    'expiry': expiry_date.strftime('%Y-%m-%d'),
                                    'spot_price': spot_price,
                                    'timestamp': datetime.now(IST)
                                })
                        
                        # Store OI for each PE strike
                        for strike in pe_strikes:
                            strike_oi = get_strike_oi_historical(symbol, strike, 'PE', expiry_date, previous_day, previous_day + timedelta(days=1))
                            if strike_oi > 0:
                                oi_strikes_collection.insert_one({
                                    'symbol': symbol,
                                    'strike_price': strike,
                                    'option_type': 'PE',
                                    'oi': int(strike_oi),
                                    'date': previous_day.strftime('%Y-%m-%d'),
                                    'expiry': expiry_date.strftime('%Y-%m-%d'),
                                    'spot_price': spot_price,
                                    'timestamp': datetime.now(IST)
                                })
                        
                        logger.info(f"Stored OI strikes for {symbol}: {len(ce_strikes)} CE + {len(pe_strikes)} PE strikes")
                except Exception as strike_error:
                    logger.warning(f"Could not store OI strikes for {symbol}: {strike_error}")
                
                # NOTE: EOD total OI storage is now handled by the "Store Live EOD OI" button
                # Click the button at 3:30 PM to capture live OI data, which becomes "previous day EOD OI" tomorrow
                
                processed_count += 1
                logger.info(f"Stored {symbol}: Call={prev_call_lots:.2f} lots, Put={prev_put_lots:.2f} lots, Call OI={prev_call_oi_lots:.2f} lots, Put OI={prev_put_oi_lots:.2f} lots")
                
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
                failed_count += 1
                continue
        
        logger.info("="*80)
        logger.info(f"Data refresh completed! Processed: {processed_count}, Failed: {failed_count}")
        logger.info("="*80)
        
        return mongo_jsonify(
            success=True,
            message='Previous day data refreshed successfully',
            processed_count=processed_count,
            failed_count=failed_count,
            previous_day=previous_day.strftime('%Y-%m-%d')
        )
        
    except Exception as e:
        logger.error(f"Error refreshing data: {e}")
        return error_response(str(e))


def process_single_stock(prev_record):
    """Process a single stock and return its data"""
    try:
        symbol = prev_record['symbol']
        lot_size = prev_record['lot_size']
        prev_call_lots = prev_record['call_lots']
        prev_put_lots = prev_record['put_lots']
        expiry_str = prev_record['expiry']
        
        # Parse expiry date
        expiry_date = datetime.strptime(expiry_str, '%Y-%m-%d').date()
        
        # OPTIMIZATION: Fetch ALL strikes volumes once, then filter for 21 strikes
        # This reduces API calls by 50% (from 4 calls to 2 calls per stock)
        live_call_volume, ce_strike_to_volume = get_live_volume_all_strikes(symbol, 'CE', expiry_date)
        live_put_volume, pe_strike_to_volume = get_live_volume_all_strikes(symbol, 'PE', expiry_date)
        
        # Convert to lots (LEFT TABLE - ALL strikes)
        live_call_lots = live_call_volume / lot_size if lot_size > 0 else 0
        live_put_lots = live_put_volume / lot_size if lot_size > 0 else 0
        
        # Validate: Warn if both volumes are 0
        if live_call_lots == 0 and live_put_lots == 0:
            logger.warning(f"⚠️ [{symbol}] WARNING: Both CALL and PUT volumes are 0! This may indicate an API error or no trading activity.")
        
        # Calculate total volume in lots
        total_live_lots = live_call_lots + live_put_lots
        
        # RIGHT TABLE: Filter for 21 strikes (spot ±10) from already fetched data
        spot_price = get_spot_price(symbol)
        
        live_call_volume_21 = 0
        live_put_volume_21 = 0
        live_call_lots_21 = 0
        live_put_lots_21 = 0
        
        if spot_price:
            # Find nearest 21 strikes around current spot
            ce_strikes_21 = find_nearest_strikes(symbol, spot_price, expiry_date, 'CE', num_strikes=21)
            pe_strikes_21 = find_nearest_strikes(symbol, spot_price, expiry_date, 'PE', num_strikes=21)
            
            if ce_strikes_21 and pe_strikes_21:
                # No API call needed! Filter from already fetched data
                live_call_volume_21 = filter_volume_for_strikes(ce_strikes_21, ce_strike_to_volume)
                live_put_volume_21 = filter_volume_for_strikes(pe_strikes_21, pe_strike_to_volume)
                
                # Convert to lots
                live_call_lots_21 = live_call_volume_21 / lot_size if lot_size > 0 else 0
                live_put_lots_21 = live_put_volume_21 / lot_size if lot_size > 0 else 0
        
        # Check if Call or Put volume doubled separately (LEFT TABLE - uses ALL strikes)
        call_alert = False
        put_alert = False
        call_ratio = 0
        put_ratio = 0
        
        current_time = datetime.now(IST)
        stock_alerts = []
        volume_drop_alert = None
        
        # Check Call volume
        if prev_call_lots > 0:
            call_ratio = live_call_lots / prev_call_lots
            logger.info(f"[{symbol}] CALL Comparison: Prev={prev_call_lots:.2f} lots, Live={live_call_lots:.2f} lots, Ratio={call_ratio:.2f}x")
            
            if call_ratio >= 2.0:
                call_alert = True
                
                # Store Call alert in MongoDB with date for daily cleanup (IST timezone)
                alert_time_ist = datetime.now(IST)  # Ensure IST timezone
                alert_doc = {
                    'symbol': symbol,
                    'timestamp': alert_time_ist,
                    'date': alert_time_ist.replace(hour=0, minute=0, second=0, microsecond=0),  # Date as datetime for MongoDB compatibility
                    'datetime': alert_time_ist,  # Add full datetime for sorting (IST)
                    'alert_type': 'CALL',
                    'prev_lots': round(prev_call_lots, 2),
                    'live_lots': round(live_call_lots, 2),
                    'ratio': round(call_ratio, 2),
                    'alert_message': f"CALL volume {call_ratio:.2f}x of previous day"
                }
                alerts_collection.insert_one(alert_doc)
                
                # Add to stock alerts (with formatted timestamp for display)
                stock_alerts.append({
                    'symbol': symbol,
                    'timestamp': current_time.strftime('%H:%M:%S'),
                    'alert_type': 'CALL',
                    'prev_lots': round(prev_call_lots, 2),
                    'live_lots': round(live_call_lots, 2),
                    'ratio': round(call_ratio, 2),
                    'alert_message': f"CALL volume {call_ratio:.2f}x of previous day"
                })
                logger.warning(f"🚨🚨🚨 CALL ALERT: {symbol} at {current_time.strftime('%H:%M:%S')} - {prev_call_lots:.2f} → {live_call_lots:.2f} lots ({call_ratio:.2f}x)")
        
        # Check Put volume
        if prev_put_lots > 0:
            put_ratio = live_put_lots / prev_put_lots
            logger.info(f"[{symbol}] PUT Comparison: Prev={prev_put_lots:.2f} lots, Live={live_put_lots:.2f} lots, Ratio={put_ratio:.2f}x")
            
            if put_ratio >= 2.0:
                put_alert = True
                
                # Store Put alert in MongoDB with date for daily cleanup (IST timezone)
                alert_time_ist = datetime.now(IST)  # Ensure IST timezone
                alert_doc = {
                    'symbol': symbol,
                    'timestamp': alert_time_ist,
                    'date': alert_time_ist.replace(hour=0, minute=0, second=0, microsecond=0),  # Date as datetime for MongoDB compatibility
                    'datetime': alert_time_ist,  # Add full datetime for sorting (IST)
                    'alert_type': 'PUT',
                    'prev_lots': round(prev_put_lots, 2),
                    'live_lots': round(live_put_lots, 2),
                    'ratio': round(put_ratio, 2),
                    'alert_message': f"PUT volume {put_ratio:.2f}x of previous day"
                }
                alerts_collection.insert_one(alert_doc)
                
                # Add to stock alerts (with formatted timestamp for display)
                stock_alerts.append({
                    'symbol': symbol,
                    'timestamp': current_time.strftime('%H:%M:%S'),
                    'alert_type': 'PUT',
                    'prev_lots': round(prev_put_lots, 2),
                    'live_lots': round(live_put_lots, 2),
                    'ratio': round(put_ratio, 2),
                    'alert_message': f"PUT volume {put_ratio:.2f}x of previous day"
                })
                logger.warning(f"🚨🚨🚨 PUT ALERT: {symbol} at {current_time.strftime('%H:%M:%S')} - {prev_put_lots:.2f} → {live_put_lots:.2f} lots ({put_ratio:.2f}x)")
        
        # Get current timestamp with proper formatting
        current_time = datetime.now(IST)
        formatted_time = current_time.strftime('%H:%M:%S')
        
        # RIGHT TABLE: Check for volume drops by comparing 21-strike volumes with previous cycle
        prev_cycle_data = previous_cycle_volume_collection.find_one({'symbol': symbol})
        
        # Initialize volume drop alerts
        call_volume_drop_alert = None
        put_volume_drop_alert = None
        
        if prev_cycle_data:
            # Check CALL volume drops (21 strikes only)
            prev_call_lots_cycle_21 = float(prev_cycle_data.get('call_lots_21', 0))
            prev_put_lots_cycle_21 = float(prev_cycle_data.get('put_lots_21', 0))
            call_volume_change_21 = live_call_lots_21 - prev_call_lots_cycle_21
            
            # DEBUG: Log comparison for sample stocks
            if symbol in ['NIFTY', 'BANKNIFTY', 'RELIANCE']:
                logger.info(f"🔍 [{symbol}] COMPARISON: Prev_CE_21={prev_call_lots_cycle_21:.2f} | Live_CE_21={live_call_lots_21:.2f} | Change={call_volume_change_21:.2f}")
                logger.info(f"🔍 [{symbol}] COMPARISON: Prev_PE_21={prev_put_lots_cycle_21:.2f} | Live_PE_21={live_put_lots_21:.2f}")
            
            # Check if CALL volume dropped by 100 or more lots (21 strikes)
            if call_volume_change_21 <= -100:
                call_volume_drop_alert = {
                    'symbol': symbol,
                    'timestamp': formatted_time,
                    'option_type': 'CALL',
                    'prev_lots': round(prev_call_lots_cycle_21, 2),
                    'current_lots': round(live_call_lots_21, 2),
                    'volume_change': round(call_volume_change_21, 2),
                    'alert_message': f"CALL volume (21 strikes) dropped by {abs(call_volume_change_21):.2f} lots"
                }
                
                logger.warning(f"📉 CALL VOLUME DROP ALERT (21 strikes): {symbol} at {formatted_time} - {prev_call_lots_cycle_21:.2f} → {live_call_lots_21:.2f} lots (dropped by {abs(call_volume_change_21):.2f} lots)")
                
                # Store the alert in the database with date for daily cleanup
                call_volume_drop_alert['date'] = current_time.replace(hour=0, minute=0, second=0, microsecond=0)  # Date as datetime for MongoDB compatibility
                call_volume_drop_alert['datetime'] = current_time  # Add full datetime for sorting
                volume_drop_alerts_collection.insert_one(call_volume_drop_alert)
                
            # Check PUT volume drops (21 strikes only)
            put_volume_change_21 = live_put_lots_21 - prev_put_lots_cycle_21
            
            if put_volume_change_21 <= -100:
                # Generate PUT volume drop alert (21 strikes)
                put_volume_drop_alert = {
                    'symbol': symbol,
                    'timestamp': formatted_time,
                    'option_type': 'PUT',
                    'prev_lots': round(prev_put_lots_cycle_21, 2),
                    'current_lots': round(live_put_lots_21, 2),
                    'volume_change': round(put_volume_change_21, 2),
                    'alert_message': f"PUT volume (21 strikes) dropped by {abs(put_volume_change_21):.2f} lots"
                }
                
                logger.warning(f"📉 PUT VOLUME DROP ALERT (21 strikes): {symbol} at {formatted_time} - {prev_put_lots_cycle_21:.2f} → {live_put_lots_21:.2f} lots (dropped by {abs(put_volume_change_21):.2f} lots)")
                
                # Store the alert in the database with date for daily cleanup
                put_volume_drop_alert['date'] = current_time.replace(hour=0, minute=0, second=0, microsecond=0)  # Date as datetime for MongoDB compatibility
                put_volume_drop_alert['datetime'] = current_time  # Add full datetime for sorting
                volume_drop_alerts_collection.insert_one(put_volume_drop_alert)
        # First delete the previous cycle data, then store the current data as the new previous cycle
        # This ensures we're always comparing with the most recent previous cycle
        previous_cycle_volume_collection.delete_one({'symbol': symbol})
        
        # Now store the current data as the new previous cycle data (for NEXT cycle's comparison)
        # Store BOTH ALL strikes and 21 strikes volumes
        previous_cycle_volume_collection.insert_one({
            'symbol': symbol,
            'call_lots': live_call_lots,  # LEFT table uses this (ALL strikes)
            'put_lots': live_put_lots,    # LEFT table uses this (ALL strikes)
            'call_lots_21': live_call_lots_21,  # RIGHT table uses this (21 strikes only)
            'put_lots_21': live_put_lots_21,    # RIGHT table uses this (21 strikes only)
            'total_lots': live_call_lots + live_put_lots,
            'timestamp': current_time,
            'formatted_time': formatted_time
        })
        
        # DEBUG: Log what we're storing for verification
        if symbol in ['NIFTY', 'BANKNIFTY', 'RELIANCE']:  # Sample stocks for debugging
            logger.info(f"💾 [{symbol}] STORED for NEXT cycle: CE_21={live_call_lots_21:.2f}, PE_21={live_put_lots_21:.2f}")
        
        # Return table row data and alerts
        return {
            'table_row': {
                'symbol': symbol,
                'prev_call_lots': round(prev_call_lots, 2),
                'prev_put_lots': round(prev_put_lots, 2),
                'live_call_lots': round(live_call_lots, 2),
                'live_put_lots': round(live_put_lots, 2),
                'live_call_lots_21': round(live_call_lots_21, 2),  # For RIGHT table (21 strikes)
                'live_put_lots_21': round(live_put_lots_21, 2),    # For RIGHT table (21 strikes)
                'call_ratio': round(call_ratio, 2) if call_ratio > 0 else 0,
                'put_ratio': round(put_ratio, 2) if put_ratio > 0 else 0,
                'call_alert': call_alert,
                'put_alert': put_alert,
                'total_live_lots': round(total_live_lots, 2)
            },
            'alerts': stock_alerts,
            'call_volume_drop_alert': call_volume_drop_alert,
            'put_volume_drop_alert': put_volume_drop_alert
        }
        
    except Exception as e:
        logger.error(f"[{prev_record.get('symbol', 'UNKNOWN')}] Error processing: {e}")
        return None

@app.route('/api/get-table-data')
def get_table_data():
    print("DEBUG: Request received at /api/get-table-data")
    """Get table data with previous day and live volumes for all stocks - PARALLEL PROCESSING"""
    try:
        # Clean old alerts at 9:00 AM
        clean_old_alerts()
        
        # Load instruments cache
        load_instruments_cache()
        
        # Get all stored previous day data
        previous_data = list(volume_collection.find({}))
        
        if not previous_data:
            return error_response('No previous day data found. Please refresh data first.', 400)
        
        logger.info("="*80)
        logger.info("🚀 STARTING VOLUME MONITORING CYCLE")
        logger.info("="*80)
        logger.info(f"📊 Total stocks to process: {len(previous_data)}")
        logger.info(f"⚙️ Parallel workers: 8 (processing 8 stocks simultaneously)")
        logger.info(f"⏱️ Estimated time: {len(previous_data) // 60} - {len(previous_data) * 2 // 60} minutes (OPTIMIZED)")
        logger.info(f"💡 Watch for progress updates every 10 stocks...")
        logger.info("="*80)
        
        # Clean up old alerts before starting (delete previous day's alerts)
        cleanup_old_alerts()
        
        # CRITICAL FIX: Cache the ACTUAL previous cycle data BEFORE processing starts
        # This is what we'll send to the frontend to show "Prev" values
        # After processing, the DB will contain CURRENT cycle data, not previous!
        logger.info("📦 Caching actual previous cycle data before processing...")
        cached_previous_cycle_data = {}
        all_cached_prev_cycle_records = list(previous_cycle_volume_collection.find({}, {'_id': 0}))
        for record in all_cached_prev_cycle_records:
            symbol = record.get('symbol')
            if symbol:
                cached_previous_cycle_data[symbol] = {
                    'call_lots': float(record.get('call_lots', 0)),
                    'put_lots': float(record.get('put_lots', 0)),
                    'call_lots_21': float(record.get('call_lots_21', 0)),
                    'put_lots_21': float(record.get('put_lots_21', 0)),
                    'total_lots': float(record.get('total_lots', 0)),
                    'formatted_time': record.get('formatted_time', '-')
                }
        logger.info(f"✅ Cached previous cycle data for {len(cached_previous_cycle_data)} stocks")
        
        table_data = []
        new_alerts = []
        
        # OPTIMIZED: Increase workers to 8 for much faster processing
        # Each stock makes 2 quote API calls + 1 spot price call = ~3 calls
        # 8 workers with rate limiting = safe and fast
        # Rate limiting in rate_limited_api_call() prevents exceeding limits
        with ThreadPoolExecutor(max_workers=8) as executor:
            # Submit all stocks for processing
            future_to_stock = {executor.submit(process_single_stock, record): record for record in previous_data}
            
            # Collect results as they complete
            call_volume_drop_alerts = []
            put_volume_drop_alerts = []
            completed_count = 0
            total_stocks = len(previous_data)
            
            for future in as_completed(future_to_stock):
                result = future.result()
                completed_count += 1
                
                # Log progress every 20 stocks (less logging = faster)
                if completed_count % 20 == 0 or completed_count == total_stocks:
                    logger.info(f"📊 Progress: {completed_count}/{total_stocks} stocks processed ({(completed_count/total_stocks*100):.1f}%)")
                
                if result:
                    # Validate: Skip rows with both 0 volumes (likely API error)
                    table_row = result['table_row']
                    if table_row['live_call_lots'] == 0 and table_row['live_put_lots'] == 0:
                        logger.warning(f"⚠️ Skipping {table_row['symbol']} - Both live volumes are 0 (likely API error)")
                        continue
                    
                    table_data.append(table_row)
                    new_alerts.extend(result['alerts'])
                    if result['call_volume_drop_alert']:
                        call_volume_drop_alerts.append(result['call_volume_drop_alert'])
                    if result['put_volume_drop_alert']:
                        put_volume_drop_alerts.append(result['put_volume_drop_alert'])
        
        logger.info("="*80)
        logger.info("✅ MONITORING CYCLE COMPLETED SUCCESSFULLY!")
        logger.info("="*80)
        logger.info(f"📊 Total stocks processed: {len(table_data)}")
        logger.info(f"🚨 New volume alerts: {len(new_alerts)}")
        logger.info(f"📉 Call volume drops: {len(call_volume_drop_alerts)}")
        logger.info(f"📉 Put volume drops: {len(put_volume_drop_alerts)}")
        if new_alerts:
            logger.info("ALERTS SUMMARY:")
            for alert in new_alerts:
                logger.warning(f"  🚨 {alert['symbol']} - {alert['alert_type']}: {alert['prev_lots']} → {alert['live_lots']} lots ({alert['ratio']}x)")
        logger.info("="*80)
        
        # Count unique stocks with alerts in this cycle
        unique_alerted_stocks = set()
        for alert in new_alerts:
            unique_alerted_stocks.add(alert['symbol'])
        
        # Log CALL volume drop alerts
        if call_volume_drop_alerts:
            logger.info(f"CALL VOLUME DROP ALERTS: {len(call_volume_drop_alerts)} stocks")
            for alert in call_volume_drop_alerts:
                logger.warning(f"  📉 {alert['symbol']} CALL: {alert['prev_lots']} → {alert['current_lots']} lots (dropped by {abs(alert['volume_change']):.2f} lots)")
        
        # Log PUT volume drop alerts
        if put_volume_drop_alerts:
            logger.info(f"PUT VOLUME DROP ALERTS: {len(put_volume_drop_alerts)} stocks")
            for alert in put_volume_drop_alerts:
                logger.warning(f"  📉 {alert['symbol']} PUT: {alert['prev_lots']} → {alert['current_lots']} lots (dropped by {abs(alert['volume_change']):.2f} lots)")
        
        # Use the CACHED previous cycle data (retrieved BEFORE processing started)
        # This ensures "Prev" values in frontend show ACTUAL previous cycle, not current
        logger.info(f"📤 Using cached previous cycle data ({len(cached_previous_cycle_data)} stocks)")
        previous_cycle_data = {}
        for symbol, data in cached_previous_cycle_data.items():
            previous_cycle_data[symbol] = {
                'call_lots': data['call_lots'],           # LEFT table (ALL strikes)
                'put_lots': data['put_lots'],             # LEFT table (ALL strikes)
                'call_lots_21': data['call_lots_21'],     # RIGHT table (21 strikes)
                'put_lots_21': data['put_lots_21'],       # RIGHT table (21 strikes)
                'total_lots': data['total_lots'],
                'timestamp': data['formatted_time']
            }
        
        # DEBUG: Log sample data being sent to frontend
        sample_symbols = ['NIFTY', 'BANKNIFTY', 'RELIANCE']
        logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        logger.info("📊 SAMPLE DATA BEING SENT TO FRONTEND (RIGHT TABLE):")
        logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        for row in table_data[:3]:  # First 3 stocks
            symbol = row['symbol']
            if symbol in previous_cycle_data:
                prev = previous_cycle_data[symbol]
                logger.info(f"{symbol}:")
                logger.info(f"  Prev CE_21: {prev['call_lots_21']:.2f} | Live CE_21: {row['live_call_lots_21']:.2f}")
                logger.info(f"  Prev PE_21: {prev['put_lots_21']:.2f} | Live PE_21: {row['live_put_lots_21']:.2f}")
        logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        
        return mongo_jsonify(
            success=True,
            table_data=table_data,
            new_alerts=new_alerts,
            call_volume_drop_alerts=call_volume_drop_alerts,
            put_volume_drop_alerts=put_volume_drop_alerts,
            previous_cycle_data=previous_cycle_data,
            total_stocks=len(table_data),
            unique_alerted_stocks_count=len(unique_alerted_stocks)
        )
        
    except Exception as e:
        logger.error(f"Error getting table data: {e}")
        return error_response(str(e))


@app.route('/api/get-todays-alerts')
def get_todays_alerts():
    """
    Fetch today's alerts for display
    Returns both volume alerts (2x previous day) and volume drop alerts (cycle-to-cycle)
    Sorted by most recent first
    """
    try:
        current_time = datetime.now(IST)
        today_start = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
        today_end = current_time.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        logger.info("="*80)
        logger.info(f"📊 FETCHING TODAY'S ALERTS for {today_start.date()}")
        logger.info("="*80)
        
        # Fetch today's volume alerts (left table - 2x previous day alerts)
        volume_alerts = list(alerts_collection.find(
            {'date': {'$gte': today_start, '$lte': today_end}},
            {'_id': 0}
        ))
        
        # Fetch today's volume drop alerts (right table - cycle-to-cycle drops)
        volume_drop_alerts = list(volume_drop_alerts_collection.find(
            {'date': {'$gte': today_start, '$lte': today_end}},
            {'_id': 0}
        ))
        
        logger.info(f"✅ Found {len(volume_alerts)} volume alerts (2x previous day)")
        logger.info(f"✅ Found {len(volume_drop_alerts)} volume drop alerts (cycle drops)")
        
        # Log some sample alerts for debugging
        if volume_alerts:
            logger.info(f"📊 Sample Volume Alert: {volume_alerts[0].get('symbol')} - {volume_alerts[0].get('alert_message')}")
        if volume_drop_alerts:
            logger.info(f"📉 Sample Volume Drop Alert: {volume_drop_alerts[0].get('symbol')} - {volume_drop_alerts[0].get('alert_message')}")
        
        # Sort both by datetime (most recent first)
        volume_alerts.sort(key=lambda x: x.get('datetime', x.get('timestamp', datetime.min)), reverse=True)
        volume_drop_alerts.sort(key=lambda x: x.get('datetime', x.get('timestamp', datetime.min)), reverse=True)
        
        # Format timestamps for display and remove datetime/date objects for JSON serialization
        for alert in volume_alerts:
            if 'datetime' in alert and isinstance(alert['datetime'], datetime):
                alert['formatted_time'] = alert['datetime'].strftime('%H:%M:%S')
                # Keep datetime for sorting but will be serialized by MongoJSONEncoder
            elif 'timestamp' in alert and isinstance(alert['timestamp'], datetime):
                alert['formatted_time'] = alert['timestamp'].strftime('%H:%M:%S')
            else:
                alert['formatted_time'] = alert.get('timestamp', '-')
        
        for alert in volume_drop_alerts:
            if not alert.get('formatted_time'):
                if isinstance(alert.get('timestamp'), datetime):
                    alert['formatted_time'] = alert['timestamp'].strftime('%H:%M:%S')
                else:
                    alert['formatted_time'] = alert.get('timestamp', '-')  # Use existing timestamp
        
        logger.info(f"📊 Returning {len(volume_alerts)} volume alerts and {len(volume_drop_alerts)} volume drop alerts for {today_start.date()}")
        
        return mongo_jsonify(
            success=True,
            volume_alerts=volume_alerts,
            volume_drop_alerts=volume_drop_alerts,
            total_alerts=len(volume_alerts) + len(volume_drop_alerts),
            date=str(today_start.date())
        )
        
    except Exception as e:
        logger.error(f"❌ Error fetching today's alerts: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return error_response(str(e))


def process_single_stock_oi(vol_record):
    """
    Process a single stock for OI monitoring - Cycle-to-Cycle comparison
    
    Logic:
    - Cycle 1 (9:45): Fetch live OI, store as baseline → No table row (nothing to compare)
    - Cycle 2 (9:46): Fetch live OI, compare with Cycle 1 data → Show in table, trigger alerts if change > 500 lots
    - Cycle 3 (9:47): Fetch live OI, compare with Cycle 2 data → Show in table, trigger alerts if change > 500 lots
    - And so on...
    
    Database:
    - oi_collection has 2 types of records per symbol:
      1. WITH 'date' field: Previous day baseline (from refresh_previous_day_data)
      2. WITHOUT 'date' field: Current cycle data (updated every monitoring cycle)
    """
    stock_alerts = []
    try:
        symbol = vol_record['symbol']
        lot_size = vol_record['lot_size']
        expiry_str = vol_record['expiry']
        
        # Parse expiry date
        expiry_date = datetime.strptime(expiry_str, '%Y-%m-%d').date()
        
        current_time = datetime.now(IST)
        logger.info(f"[{symbol}] [{current_time.strftime('%H:%M:%S')}] Fetching live OI data...")
        
        # NEW: Get spot price first for near ATM calculation
        spot_price = get_spot_price(symbol)
        
        if not spot_price:
            logger.warning(f"[{symbol}] Skipping - Could not get spot price")
            return None
        
        # LEFT TABLE CALCULATION: Sum of OI for NEAR ATM STRIKES (21 strikes)
        # Selection: 10 strikes BELOW spot + Spot strike + 10 strikes ABOVE spot = 21 total
        # Example: If spot = 3105, selects strikes like: 2900, 2920, 2940... 3100... 3280, 3300
        # Then sums up the OI of all these 21 strikes for both CE and PE
        logger.info(f"[{symbol}] Spot price: {spot_price}")
        logger.info(f"[{symbol}] Selecting 21 near ATM strikes (10 below + spot + 10 above)...")
        
        ce_strikes_list = find_nearest_strikes(symbol, spot_price, expiry_date, 'CE', num_strikes=21)
        pe_strikes_list = find_nearest_strikes(symbol, spot_price, expiry_date, 'PE', num_strikes=21)
        
        logger.info(f"[{symbol}] Fetching LIVE OI for selected near ATM strikes...")
        logger.info(f"[{symbol}] === STARTING CALL (CE) OI CALCULATION ===")
        live_call_oi = get_oi_for_strikes_list(symbol, ce_strikes_list, 'CE', expiry_date)
        
        logger.info(f"[{symbol}] === STARTING PUT (PE) OI CALCULATION ===")
        live_put_oi = get_oi_for_strikes_list(symbol, pe_strikes_list, 'PE', expiry_date)
        
        # Convert to lots
        live_call_oi_lots = live_call_oi / lot_size if lot_size > 0 else 0
        live_put_oi_lots = live_put_oi / lot_size if lot_size > 0 else 0
        
        # Final LEFT TABLE summary
        logger.info("🔵"*40)
        logger.info(f"[{symbol}] ✅ LEFT TABLE FINAL RESULT:")
        logger.info(f"[{symbol}]    CE Strikes Selected: {len(ce_strikes_list)} strikes from {min(ce_strikes_list)} to {max(ce_strikes_list)}")
        logger.info(f"[{symbol}]    CE Total OI: {live_call_oi:,} contracts = {live_call_oi_lots:.2f} lots")
        logger.info(f"[{symbol}]    PE Strikes Selected: {len(pe_strikes_list)} strikes from {min(pe_strikes_list)} to {max(pe_strikes_list)}")
        logger.info(f"[{symbol}]    PE Total OI: {live_put_oi:,} contracts = {live_put_oi_lots:.2f} lots")
        logger.info(f"[{symbol}]    Lot Size: {lot_size}")
        logger.info("🔵"*40)
        
        # Skip if both OI values are 0 (likely API error)
        if live_call_oi == 0 and live_put_oi == 0:
            logger.warning(f"[{symbol}] Skipping - Both OI values are 0")
            return None
        
        # Get previous cycle OI (BEFORE updating with new data)
        # Query: Find record without 'date' field (this is the last cycle's live data)
        prev_cycle = oi_collection.find_one({'symbol': symbol, 'date': {'$exists': False}})
        
        # RIGHT TABLE CALCULATION: Change in OI (vs Previous Day)
        # CRITICAL: Uses the SAME 21 strikes for BOTH live and previous day
        # Both are based on CURRENT LIVE spot price
        # Compares: Today's OI (sum of 21 strikes at live spot) - Yesterday's OI (sum of SAME 21 strikes at live spot)
        # 
        # Example:
        #   - Live spot: 5100 → selects strikes 5000-5200 (21 strikes)
        #   - Previous day OI: Also uses strikes 5000-5200 (SAME strikes, even though prev day spot was 5050)
        #   - Change in OI = Live OI (5100±10) - Previous Day OI (5100±10) - SAME STRIKES!
        live_call_oi_change = 0  # Current cycle's total change
        live_put_oi_change = 0
        prev_call_oi_change = 0  # Previous cycle's total change (for alerts)
        prev_put_oi_change = 0   # Previous cycle's total change (for alerts)
        
        # Get HISTORICAL OI for the SAME selected strikes (based on LIVE spot) from database
        prev_day_call_oi_near_atm = 0
        prev_day_put_oi_near_atm = 0
        
        logger.info(f"[{symbol}] Fetching previous day OI for the SAME 21 strikes (RIGHT table - Change in OI)...")
        logger.info(f"[{symbol}]   Using SAME strikes for both live and previous day (based on LIVE spot {spot_price})")
        
        # Fetch CE historical OI for selected strikes (same strikes as live)
        for strike in ce_strikes_list:
            strike_record = oi_near_atm_collection.find_one({
                'symbol': symbol, 
                'strike_price': strike, 
                'option_type': 'CE',
                'date': {'$exists': True}
            })
            if strike_record:
                prev_day_call_oi_near_atm += strike_record.get('oi', 0)
        
        # Fetch PE historical OI for selected strikes (same strikes as live)
        for strike in pe_strikes_list:
            strike_record = oi_near_atm_collection.find_one({
                'symbol': symbol, 
                'strike_price': strike, 
                'option_type': 'PE',
                'date': {'$exists': True}
            })
            if strike_record:
                prev_day_put_oi_near_atm += strike_record.get('oi', 0)
        
        # Convert historical OI to lots
        prev_day_call_oi_near_atm_lots = prev_day_call_oi_near_atm / lot_size if lot_size > 0 else 0
        prev_day_put_oi_near_atm_lots = prev_day_put_oi_near_atm / lot_size if lot_size > 0 else 0
        
        logger.info(f"[{symbol}] Previous day near ATM OI (sum of SAME 21 strikes at live spot): CE={prev_day_call_oi_near_atm_lots:.2f} lots, PE={prev_day_put_oi_near_atm_lots:.2f} lots")
        
        # Calculate change (Today - Yesterday for SAME strikes) - This goes to RIGHT table
        # Initialize to 0 in case there's no previous day data
        live_call_oi_change = 0
        live_put_oi_change = 0
        if prev_day_call_oi_near_atm > 0 or prev_day_put_oi_near_atm > 0:
            live_call_oi_change = live_call_oi_lots - prev_day_call_oi_near_atm_lots
            live_put_oi_change = live_put_oi_lots - prev_day_put_oi_near_atm_lots
            
            logger.info("="*80)
            logger.info(f"[{symbol}] ✅ CHANGE IN OI CALCULATION (RIGHT TABLE):")
            logger.info(f"[{symbol}]   This uses SAME strikes for both live and previous day!")
            logger.info(f"[{symbol}]   CALL:")
            logger.info(f"[{symbol}]     - Live OI: {live_call_oi_lots:.2f} lots (at LIVE spot {spot_price}, strikes: {min(ce_strikes_list) if ce_strikes_list else 'N/A'}-{max(ce_strikes_list) if ce_strikes_list else 'N/A'})")
            logger.info(f"[{symbol}]     - Prev Day OI: {prev_day_call_oi_near_atm_lots:.2f} lots (at SAME LIVE spot {spot_price}, SAME strikes: {min(ce_strikes_list) if ce_strikes_list else 'N/A'}-{max(ce_strikes_list) if ce_strikes_list else 'N/A'})")
            logger.info(f"[{symbol}]     - Change in OI: {live_call_oi_change:+.2f} lots")
            logger.info(f"[{symbol}]   PUT:")
            logger.info(f"[{symbol}]     - Live OI: {live_put_oi_lots:.2f} lots (at LIVE spot {spot_price}, strikes: {min(pe_strikes_list) if pe_strikes_list else 'N/A'}-{max(pe_strikes_list) if pe_strikes_list else 'N/A'})")
            logger.info(f"[{symbol}]     - Prev Day OI: {prev_day_put_oi_near_atm_lots:.2f} lots (at SAME LIVE spot {spot_price}, SAME strikes: {min(pe_strikes_list) if pe_strikes_list else 'N/A'}-{max(pe_strikes_list) if pe_strikes_list else 'N/A'})")
            logger.info(f"[{symbol}]     - Change in OI: {live_put_oi_change:+.2f} lots")
            logger.info("="*80)
        
        # NEW: Calculate DIFFERENCE (live OI - prev EOD OI) using DIFFERENT spot prices
        # CRITICAL DIFFERENCE from "Change in OI":
        # 
        # "Change in OI" (RIGHT TABLE):
        #   - Live OI: Uses LIVE spot price to select 21 strikes (spot ±10)
        #   - Previous Day OI: Uses SAME LIVE spot price to select 21 strikes (spot ±10)
        #   - Both use the SAME strikes based on current live spot
        #   - Formula: Live OI (at live spot ±10) - Previous Day OI (at live spot ±10)
        #
        # "Difference" (COMPARISON TABLE):
        #   - Live OI: Uses LIVE spot price to select 21 strikes (spot ±10)
        #   - Previous Day OI: Uses PREVIOUS DAY 3:30PM spot price to select 21 strikes (spot ±10)
        #   - They use DIFFERENT strikes because spot prices are different!
        #   - Formula: Live OI (at live spot ±10) - Previous Day OI (at prev day spot ±10)
        #
        # Example:
        #   - Live spot: 5100 → selects strikes 5000-5200 (21 strikes)
        #   - Prev day 3:30pm spot: 5050 → selects strikes 4950-5150 (21 strikes) - DIFFERENT!
        #   - Difference = Live OI (5100±10) - Prev Day OI (5050±10)
        prev_eod_call_oi_lots = 0
        prev_eod_put_oi_lots = 0
        call_difference = 0
        put_difference = 0
        call_diff_vs_chng_alert = False
        put_diff_vs_chng_alert = False
        
        # Get previous day EOD total OI (stored from yesterday's "Store Live EOD OI" button click)
        # This data was captured at 3:30pm yesterday with live spot price
        collection_name = prev_day_eod_total_oi_collection.name
        logger.info(f"[{symbol}] Looking for EOD data in collection: '{collection_name}'")
        
        # Get yesterday's date to find the stored EOD data
        yesterday = (datetime.now(IST) - timedelta(days=1)).date()
        # Try to find data for yesterday first, then try any record for this symbol
        prev_eod_record = prev_day_eod_total_oi_collection.find_one({
            'symbol': symbol,
            'date': yesterday.strftime('%Y-%m-%d')
        })
        
        # If not found for yesterday, try to find the most recent record for this symbol
        if not prev_eod_record:
            prev_eod_record = prev_day_eod_total_oi_collection.find_one({'symbol': symbol})
            if prev_eod_record:
                logger.info(f"[{symbol}] Found EOD record for date {prev_eod_record.get('date')} (not yesterday, but using it)")
        if prev_eod_record:
            prev_eod_spot_price = prev_eod_record.get('spot_price', 0)
            prev_eod_call_oi_lots = prev_eod_record.get('call_oi_lots', 0)
            prev_eod_put_oi_lots = prev_eod_record.get('put_oi_lots', 0)
            prev_eod_ce_strikes = prev_eod_record.get('ce_strikes', [])
            prev_eod_pe_strikes = prev_eod_record.get('pe_strikes', [])
            logger.info(f"[{symbol}] ✅ Found EOD record: CE={prev_eod_call_oi_lots:.2f} lots, PE={prev_eod_put_oi_lots:.2f} lots, Spot={prev_eod_spot_price}")
            
            # Calculate difference: live OI (at live spot ±10) - prev EOD OI (at prev day spot ±10)
            call_difference = live_call_oi_lots - prev_eod_call_oi_lots
            put_difference = live_put_oi_lots - prev_eod_put_oi_lots
            
            logger.info("="*80)
            logger.info(f"[{symbol}] ✅ DIFFERENCE CALCULATION (COMPARISON TABLE):")
            logger.info(f"[{symbol}]   This uses DIFFERENT strikes for live vs previous day!")
            logger.info(f"[{symbol}]   CALL:")
            logger.info(f"[{symbol}]     - Live OI: {live_call_oi_lots:.2f} lots (at LIVE spot {spot_price}, strikes: {min(ce_strikes_list) if ce_strikes_list else 'N/A'}-{max(ce_strikes_list) if ce_strikes_list else 'N/A'})")
            logger.info(f"[{symbol}]     - Prev EOD OI: {prev_eod_call_oi_lots:.2f} lots (at PREV DAY spot {prev_eod_spot_price}, strikes: {min(prev_eod_ce_strikes) if prev_eod_ce_strikes else 'N/A'}-{max(prev_eod_ce_strikes) if prev_eod_ce_strikes else 'N/A'})")
            logger.info(f"[{symbol}]     - Difference: {call_difference:+.2f} lots")
            logger.info(f"[{symbol}]   PUT:")
            logger.info(f"[{symbol}]     - Live OI: {live_put_oi_lots:.2f} lots (at LIVE spot {spot_price}, strikes: {min(pe_strikes_list) if pe_strikes_list else 'N/A'}-{max(pe_strikes_list) if pe_strikes_list else 'N/A'})")
            logger.info(f"[{symbol}]     - Prev EOD OI: {prev_eod_put_oi_lots:.2f} lots (at PREV DAY spot {prev_eod_spot_price}, strikes: {min(prev_eod_pe_strikes) if prev_eod_pe_strikes else 'N/A'}-{max(prev_eod_pe_strikes) if prev_eod_pe_strikes else 'N/A'})")
            logger.info(f"[{symbol}]     - Difference: {put_difference:+.2f} lots")
            logger.info("="*80)
            
            # Alert if difference between "difference" and "chng in OI" is greater than 500 lots
            if live_call_oi_change != 0:
                call_diff_vs_chng = abs(call_difference - live_call_oi_change)
                if call_diff_vs_chng > 500:
                    call_diff_vs_chng_alert = True
                    alert_doc = {
                        'symbol': symbol,
                        'timestamp': current_time,
                        'alert_type': 'CALL_DIFF_VS_CHNG_ALERT',
                        'difference': round(call_difference, 2),
                        'chng_in_oi': round(live_call_oi_change, 2),
                        'diff_vs_chng': round(call_diff_vs_chng, 2),
                        'alert_message': f"CALL Difference vs Chng in OI gap: {call_diff_vs_chng:.2f} lots (Difference: {call_difference:+.2f}, Chng in OI: {live_call_oi_change:+.2f})"
                    }
                    oi_alerts_collection.insert_one(alert_doc)
                    stock_alerts.append({
                        'symbol': symbol,
                        'timestamp': current_time.strftime('%H:%M:%S'),
                        'alert_type': 'CALL_DIFF_VS_CHNG_ALERT',
                        'difference': round(call_difference, 2),
                        'chng_in_oi': round(live_call_oi_change, 2),
                        'diff_vs_chng': round(call_diff_vs_chng, 2),
                        'alert_message': f"CALL Difference vs Chng in OI gap: {call_diff_vs_chng:.2f} lots"
                    })
                    logger.warning(f"🚨 CALL DIFF VS CHNG ALERT: {symbol} - Gap: {call_diff_vs_chng:.2f} lots")
            
            if live_put_oi_change != 0:
                put_diff_vs_chng = abs(put_difference - live_put_oi_change)
                if put_diff_vs_chng > 500:
                    put_diff_vs_chng_alert = True
                    alert_doc = {
                        'symbol': symbol,
                        'timestamp': current_time,
                        'alert_type': 'PUT_DIFF_VS_CHNG_ALERT',
                        'difference': round(put_difference, 2),
                        'chng_in_oi': round(live_put_oi_change, 2),
                        'diff_vs_chng': round(put_diff_vs_chng, 2),
                        'alert_message': f"PUT Difference vs Chng in OI gap: {put_diff_vs_chng:.2f} lots (Difference: {put_difference:+.2f}, Chng in OI: {live_put_oi_change:+.2f})"
                    }
                    oi_alerts_collection.insert_one(alert_doc)
                    stock_alerts.append({
                        'symbol': symbol,
                        'timestamp': current_time.strftime('%H:%M:%S'),
                        'alert_type': 'PUT_DIFF_VS_CHNG_ALERT',
                        'difference': round(put_difference, 2),
                        'chng_in_oi': round(live_put_oi_change, 2),
                        'diff_vs_chng': round(put_diff_vs_chng, 2),
                        'alert_message': f"PUT Difference vs Chng in OI gap: {put_diff_vs_chng:.2f} lots"
                    })
                    logger.warning(f"🚨 PUT DIFF VS CHNG ALERT: {symbol} - Gap: {put_diff_vs_chng:.2f} lots")
        else:
            logger.warning(f"[{symbol}] ⚠️ NO EOD record found in prev_day_eod_total_oi_collection for symbol {symbol}")
            logger.warning(f"[{symbol}]    This means refresh data was not run or failed for this symbol")
            logger.warning(f"[{symbol}]    Difference will be 0. Please refresh previous day data first!")
            logger.warning(f"[{symbol}]    Prev EOD OI values will be 0 in the comparison table")
        
        table_row = None
        
        if prev_cycle:
            # Previous cycle exists - compare
            prev_call_oi_lots = prev_cycle.get('call_oi_lots', 0)
            prev_put_oi_lots = prev_cycle.get('put_oi_lots', 0)
            
            call_oi_change_lots = live_call_oi_lots - prev_call_oi_lots
            put_oi_change_lots = live_put_oi_lots - prev_put_oi_lots
            
            logger.info(f"[{symbol}] CALL: {prev_call_oi_lots:.2f} → {live_call_oi_lots:.2f} lots (Change: {call_oi_change_lots:+.2f})")
            logger.info(f"[{symbol}] PUT:  {prev_put_oi_lots:.2f} → {live_put_oi_lots:.2f} lots (Change: {put_oi_change_lots:+.2f})")
            
            call_alert = False
            put_alert = False
            
            # Check alerts
            if abs(call_oi_change_lots) > 500:
                call_alert = True
                alert_doc = {
                    'symbol': symbol,
                    'timestamp': current_time,
                    'alert_type': 'CALL_OI_CHANGE',
                    'prev_oi_lots': round(prev_call_oi_lots, 2),
                    'live_oi_lots': round(live_call_oi_lots, 2),
                    'oi_change_lots': round(call_oi_change_lots, 2),
                    'alert_message': f"CALL OI changed by {call_oi_change_lots:+.2f} lots"
                }
                oi_alerts_collection.insert_one(alert_doc)
                stock_alerts.append({
                    'symbol': symbol,
                    'timestamp': current_time.strftime('%H:%M:%S'),
                    'alert_type': 'CALL_OI_CHANGE',
                    'prev_oi_lots': round(prev_call_oi_lots, 2),
                    'live_oi_lots': round(live_call_oi_lots, 2),
                    'oi_change_lots': round(call_oi_change_lots, 2),
                    'alert_message': f"CALL OI changed by {call_oi_change_lots:+.2f} lots"
                })
                logger.warning(f"🚨 CALL OI ALERT: {symbol} - {call_oi_change_lots:+.2f} lots")
            
            if abs(put_oi_change_lots) > 500:
                put_alert = True
                alert_doc = {
                    'symbol': symbol,
                    'timestamp': current_time,
                    'alert_type': 'PUT_OI_CHANGE',
                    'prev_oi_lots': round(prev_put_oi_lots, 2),
                    'live_oi_lots': round(live_put_oi_lots, 2),
                    'oi_change_lots': round(put_oi_change_lots, 2),
                    'alert_message': f"PUT OI changed by {put_oi_change_lots:+.2f} lots"
                }
                oi_alerts_collection.insert_one(alert_doc)
                stock_alerts.append({
                    'symbol': symbol,
                    'timestamp': current_time.strftime('%H:%M:%S'),
                    'alert_type': 'PUT_OI_CHANGE',
                    'prev_oi_lots': round(prev_put_oi_lots, 2),
                    'live_oi_lots': round(live_put_oi_lots, 2),
                    'oi_change_lots': round(put_oi_change_lots, 2),
                    'alert_message': f"PUT OI changed by {put_oi_change_lots:+.2f} lots"
                })
                logger.warning(f"🚨 PUT OI ALERT: {symbol} - {put_oi_change_lots:+.2f} lots")
            
            # Check alerts for "Total Change in OI" (right table - near ATM)
            call_oi_change_alert = False
            put_oi_change_alert = False
            prev_cycle_change = oi_near_atm_change_collection.find_one({'symbol': symbol})
            
            # Get previous cycle's total change for comparison
            if prev_cycle_change:
                prev_call_oi_change = prev_cycle_change.get('total_call_oi_change_lots', 0)
                prev_put_oi_change = prev_cycle_change.get('total_put_oi_change_lots', 0)
                
                # Check if Total Change in OI changed by more than 500 lots
                call_oi_change_diff = live_call_oi_change - prev_call_oi_change
                put_oi_change_diff = live_put_oi_change - prev_put_oi_change
                
                logger.info(f"[{symbol}] Total OI Change (Near ATM) - CALL: {prev_call_oi_change:.2f} → {live_call_oi_change:.2f} (Diff: {call_oi_change_diff:+.2f})")
                logger.info(f"[{symbol}] Total OI Change (Near ATM) - PUT: {prev_put_oi_change:.2f} → {live_put_oi_change:.2f} (Diff: {put_oi_change_diff:+.2f})")
                
                # Alert if change difference > 500 lots
                if abs(call_oi_change_diff) > 500:
                    call_oi_change_alert = True
                    alert_doc = {
                        'symbol': symbol,
                        'timestamp': current_time,
                        'alert_type': 'CALL_TOTAL_OI_CHANGE',
                        'prev_total_change': round(prev_call_oi_change, 2),
                        'live_total_change': round(live_call_oi_change, 2),
                        'change_diff': round(call_oi_change_diff, 2),
                        'alert_message': f"CALL Total OI Change (Near ATM) shifted by {call_oi_change_diff:+.2f} lots"
                    }
                    oi_change_alerts_collection.insert_one(alert_doc)
                    stock_alerts.append({
                        'symbol': symbol,
                        'timestamp': current_time.strftime('%H:%M:%S'),
                        'alert_type': 'CALL_TOTAL_OI_CHANGE',
                        'prev_total_change': round(prev_call_oi_change, 2),
                        'live_total_change': round(live_call_oi_change, 2),
                        'change_diff': round(call_oi_change_diff, 2),
                        'alert_message': f"CALL Total OI Change (Near ATM) shifted by {call_oi_change_diff:+.2f} lots"
                    })
                    logger.warning(f"🚨 CALL TOTAL OI CHANGE ALERT (Near ATM): {symbol} - {prev_call_oi_change:.2f} → {live_call_oi_change:.2f} (Shift: {call_oi_change_diff:+.2f} lots)")
                
                if abs(put_oi_change_diff) > 500:
                    put_oi_change_alert = True
                    alert_doc = {
                        'symbol': symbol,
                        'timestamp': current_time,
                        'alert_type': 'PUT_TOTAL_OI_CHANGE',
                        'prev_total_change': round(prev_put_oi_change, 2),
                        'live_total_change': round(live_put_oi_change, 2),
                        'change_diff': round(put_oi_change_diff, 2),
                        'alert_message': f"PUT Total OI Change (Near ATM) shifted by {put_oi_change_diff:+.2f} lots"
                    }
                    oi_change_alerts_collection.insert_one(alert_doc)
                    stock_alerts.append({
                        'symbol': symbol,
                        'timestamp': current_time.strftime('%H:%M:%S'),
                        'alert_type': 'PUT_TOTAL_OI_CHANGE',
                        'prev_total_change': round(prev_put_oi_change, 2),
                        'live_total_change': round(live_put_oi_change, 2),
                        'change_diff': round(put_oi_change_diff, 2),
                        'alert_message': f"PUT Total OI Change (Near ATM) shifted by {put_oi_change_diff:+.2f} lots"
                    })
                    logger.warning(f"🚨 PUT TOTAL OI CHANGE ALERT (Near ATM): {symbol} - {prev_put_oi_change:.2f} → {live_put_oi_change:.2f} (Shift: {put_oi_change_diff:+.2f} lots)")
            
            # Log summary for both tables (both using near ATM strikes - spot ±10 = 21 strikes)
            logger.info("="*60)
            logger.info(f"[{symbol}] LEFT TABLE - 📈 Total OI (Near ATM 21 Strikes):")
            logger.info(f"  Prev Call OI: {prev_call_oi_lots:.2f} lots | Live Call OI: {live_call_oi_lots:.2f} lots | Cycle Change: {call_oi_change_lots:+.2f} lots")
            logger.info(f"  Prev Put OI:  {prev_put_oi_lots:.2f} lots | Live Put OI:  {live_put_oi_lots:.2f} lots | Cycle Change: {put_oi_change_lots:+.2f} lots")
            logger.info(f"[{symbol}] RIGHT TABLE - 📊 Total Change in OI vs Previous Day (Near ATM 21 Strikes):")
            logger.info(f"  Prev CE OI Change: {prev_call_oi_change:+.2f} lots | Live CE OI Change: {live_call_oi_change:+.2f} lots")
            logger.info(f"  Prev PE OI Change: {prev_put_oi_change:+.2f} lots | Live PE OI Change: {live_put_oi_change:+.2f} lots")
            logger.info("="*60)
            
            table_row = {
                'symbol': symbol,
                'prev_call_oi_lots': round(prev_call_oi_lots, 2),
                'prev_put_oi_lots': round(prev_put_oi_lots, 2),
                'live_call_oi_lots': round(live_call_oi_lots, 2),
                'live_put_oi_lots': round(live_put_oi_lots, 2),
                'call_oi_change_lots': round(call_oi_change_lots, 2),
                'put_oi_change_lots': round(put_oi_change_lots, 2),
                'call_alert': call_alert,
                'put_alert': put_alert,
                'prev_call_oi_change': round(prev_call_oi_change, 2),
                'live_call_oi_change': round(live_call_oi_change, 2),
                'prev_put_oi_change': round(prev_put_oi_change, 2),
                'live_put_oi_change': round(live_put_oi_change, 2),
                'call_oi_change_alert': call_oi_change_alert,
                'put_oi_change_alert': put_oi_change_alert,
                # New comparison data
                'prev_eod_call_oi_lots': round(prev_eod_call_oi_lots, 2),
                'prev_eod_put_oi_lots': round(prev_eod_put_oi_lots, 2),
                'call_difference': round(call_difference, 2),
                'put_difference': round(put_difference, 2),
                'call_diff_vs_chng_alert': call_diff_vs_chng_alert,
                'put_diff_vs_chng_alert': put_diff_vs_chng_alert
            }
        else:
            # First cycle - show baseline data with near ATM OI change
            logger.info(f"[{symbol}] First cycle - showing baseline OI with near ATM change")
            
            # For first cycle, show live OI and near ATM change vs previous day
            table_row = {
                'symbol': symbol,
                'prev_call_oi_lots': 0,  # No previous cycle yet
                'prev_put_oi_lots': 0,   # No previous cycle yet
                'live_call_oi_lots': round(live_call_oi_lots, 2),
                'live_put_oi_lots': round(live_put_oi_lots, 2),
                'call_oi_change_lots': 0,  # No cycle-to-cycle change yet
                'put_oi_change_lots': 0,   # No cycle-to-cycle change yet
                'call_alert': False,
                'put_alert': False,
                'prev_call_oi_change': 0,  # No previous cycle comparison
                'live_call_oi_change': round(live_call_oi_change, 2),
                'prev_put_oi_change': 0,   # No previous cycle comparison
                'live_put_oi_change': round(live_put_oi_change, 2),
                'call_oi_change_alert': False,
                'put_oi_change_alert': False,
                # New comparison data
                'prev_eod_call_oi_lots': round(prev_eod_call_oi_lots, 2),
                'prev_eod_put_oi_lots': round(prev_eod_put_oi_lots, 2),
                'call_difference': round(call_difference, 2),
                'put_difference': round(put_difference, 2),
                'call_diff_vs_chng_alert': False,
                'put_diff_vs_chng_alert': False
            }
            
            logger.info("="*60)
            logger.info(f"[{symbol}] FIRST CYCLE DATA (both tables use near ATM 21 strikes):")
            logger.info(f"  LEFT TABLE - 📈 Total OI (Near ATM): Call={live_call_oi_lots:.2f} lots, Put={live_put_oi_lots:.2f} lots")
            logger.info(f"  RIGHT TABLE - 📊 Total Change in OI vs Previous Day (Near ATM): CE={live_call_oi_change:+.2f} lots, PE={live_put_oi_change:+.2f} lots")
            logger.info("="*60)
        
        # Update current cycle data (this becomes "prev_cycle" for next iteration)
        # This update happens AFTER comparison, so next cycle will compare against this
        # Important: Update only the record WITHOUT 'date' field (cycle tracking, not baseline)
        oi_collection.update_one(
            {'symbol': symbol, 'date': {'$exists': False}},
            {'$set': {
                'symbol': symbol,
                'expiry': expiry_str,
                'lot_size': lot_size,
                'call_oi': int(live_call_oi),
                'put_oi': int(live_put_oi),
                'call_oi_lots': round(live_call_oi_lots, 2),
                'put_oi_lots': round(live_put_oi_lots, 2),
                'timestamp': datetime.now(IST)
            }},
            upsert=True
        )
        
        # Store "Total Change in OI" for second table (vs previous day) using near ATM OI
        # This is separate from cycle-to-cycle comparison above
        # NEW: Use near ATM OI change for storage (for next cycle comparison)
        if 'live_call_oi_change' in locals() and 'live_put_oi_change' in locals():
            # Store current cycle's near ATM total change (becomes "prev" for next cycle's table display)
            if live_call_oi_change != 0 or live_put_oi_change != 0:
                oi_near_atm_change_collection.update_one(
                    {'symbol': symbol},
                    {'$set': {
                        'symbol': symbol,
                        'total_call_oi_change_lots': round(live_call_oi_change, 2),
                        'total_put_oi_change_lots': round(live_put_oi_change, 2),
                        'timestamp': datetime.now(IST)
                    }},
                    upsert=True
                )
        
        return {'table_row': table_row, 'alerts': stock_alerts}
        
    except Exception as e:
        logger.error(f"[{vol_record.get('symbol', 'UNKNOWN')}] Error: {e}")
        return None

@app.route('/api/store-live-eod-oi', methods=['POST'])
def store_live_eod_oi():
    """Store LIVE OI totals at current spot price (spot ±10) for all stocks - to be used as previous day EOD data tomorrow"""
    try:
        logger.info("="*80)
        logger.info("STORING LIVE EOD OI DATA (for tomorrow's comparison)")
        logger.info("="*80)
        
        load_instruments_cache()
        
        # Get all stocks from config (excluding major indices)
        major_indices = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"]
        stocks_to_process = [s for s in config.STOCK_SYMBOLS if s not in major_indices]
        
        if not stocks_to_process:
            return error_response('No stocks found in config.', 400)
        
        logger.info(f"Processing {len(stocks_to_process)} stocks from config to store LIVE EOD OI data...")
        
        today = datetime.now(IST).date()
        processed_count = 0
        failed_count = 0
        
        # Delete existing EOD data for today (in case button is clicked multiple times)
        prev_day_eod_total_oi_collection.delete_many({'date': today.strftime('%Y-%m-%d')})
        logger.info(f"Cleared any existing EOD data for today ({today.strftime('%Y-%m-%d')})")
        
        for idx, symbol in enumerate(stocks_to_process, 1):
            try:
                logger.info(f"[{idx}/{len(stocks_to_process)}] Processing {symbol}...")
                
                # Get nearest expiry
                expiry_date = get_nearest_expiry(symbol)
                if not expiry_date:
                    logger.warning(f"[{symbol}] No expiry found, skipping...")
                    failed_count += 1
                    continue
                
                # Get lot size
                lot_size = get_lot_size_for_symbol(symbol)
                if not lot_size:
                    logger.warning(f"[{symbol}] No lot size found, skipping...")
                    failed_count += 1
                    continue
                
                expiry_str = expiry_date.strftime('%Y-%m-%d')
                
                # Get LIVE spot price
                spot_price = get_spot_price(symbol)
                if not spot_price or spot_price <= 0:
                    logger.warning(f"[{symbol}] Could not get live spot price, skipping...")
                    failed_count += 1
                    continue
                
                logger.info(f"[{symbol}] Live spot price: {spot_price}")
                
                # Calculate 21 strikes based on LIVE spot price (spot ±10)
                ce_strikes_list = find_nearest_strikes(symbol, spot_price, expiry_date, 'CE', num_strikes=21)
                pe_strikes_list = find_nearest_strikes(symbol, spot_price, expiry_date, 'PE', num_strikes=21)
                
                logger.info(f"[{symbol}] Selected 21 strikes: CE={min(ce_strikes_list) if ce_strikes_list else 'N/A'}-{max(ce_strikes_list) if ce_strikes_list else 'N/A'}, PE={min(pe_strikes_list) if pe_strikes_list else 'N/A'}-{max(pe_strikes_list) if pe_strikes_list else 'N/A'}")
                
                # Get LIVE OI for these strikes
                live_call_oi = get_oi_for_strikes_list(symbol, ce_strikes_list, 'CE', expiry_date)
                live_put_oi = get_oi_for_strikes_list(symbol, pe_strikes_list, 'PE', expiry_date)
                
                # Convert to lots
                live_call_oi_lots = live_call_oi / lot_size if lot_size > 0 else 0
                live_put_oi_lots = live_put_oi / lot_size if lot_size > 0 else 0
                
                # Store in database (this will be used as "previous day EOD OI" tomorrow)
                eod_doc = {
                    'symbol': symbol,
                    'date': today.strftime('%Y-%m-%d'),  # Today's date - tomorrow this becomes "previous day"
                    'expiry': expiry_str,
                    'spot_price': spot_price,  # LIVE spot price at time of capture
                    'call_oi': int(live_call_oi),
                    'put_oi': int(live_put_oi),
                    'call_oi_lots': round(live_call_oi_lots, 2),
                    'put_oi_lots': round(live_put_oi_lots, 2),
                    'lot_size': lot_size,
                    'ce_strikes': ce_strikes_list,
                    'pe_strikes': pe_strikes_list,
                    'timestamp': datetime.now(IST)
                }
                
                prev_day_eod_total_oi_collection.insert_one(eod_doc)
                logger.info(f"[{symbol}] ✅ Stored LIVE EOD OI: Spot={spot_price}, CE={live_call_oi_lots:.2f} lots, PE={live_put_oi_lots:.2f} lots")
                
                processed_count += 1
                
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
                import traceback
                logger.error(traceback.format_exc())
                failed_count += 1
                continue
        
        logger.info("="*80)
        logger.info(f"LIVE EOD OI storage completed! Processed: {processed_count}, Failed: {failed_count}")
        logger.info("="*80)
        
        return mongo_jsonify(
            success=True,
            message='Live EOD OI data stored successfully',
            processed_count=processed_count,
            failed_count=failed_count,
            date=today.strftime('%Y-%m-%d')
        )
        
    except Exception as e:
        logger.error(f"Error storing live EOD OI: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return error_response(str(e))


@app.route('/api/get-oi-table-data')
def get_oi_table_data():
    """Get OI table data with RATE-LIMITED processing"""
    try:
        # Clean old alerts at 9:00 AM
        clean_old_alerts()
        
        load_instruments_cache()
        volume_data = list(volume_collection.find({}))
        
        if not volume_data:
            return error_response('No data found. Please refresh previous day data first.', 400)
        
        logger.info(f"Processing {len(volume_data)} stocks with rate limiting for OI monitoring...")
        
        table_data = []
        new_alerts = []
        
        # Process stocks in smaller batches with rate limiting
        # KiteConnect limit: 15 req/sec, each stock makes ~2-4 requests (CE + PE batches)
        # Safe: 5 workers max to stay under 15 req/sec
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_stock = {executor.submit(process_single_stock_oi, record): record for record in volume_data}
            
            for future in as_completed(future_to_stock):
                result = future.result()
                if result and result['table_row']:
                    table_data.append(result['table_row'])
                    new_alerts.extend(result['alerts'])
        
        cycle_end_time = datetime.now(IST)
        logger.info("="*80)
        logger.info(f"OI MONITORING CYCLE COMPLETED at {cycle_end_time.strftime('%H:%M:%S')}")
        logger.info(f"Total stocks processed: {len(table_data)}")
        logger.info(f"New alerts this cycle: {len(new_alerts)}")
        if new_alerts:
            logger.info("ALERTS SUMMARY:")
            for alert in new_alerts:
                logger.warning(f"  🚨 {alert['symbol']} - {alert['alert_type']}: {alert['alert_message']}")
        logger.info("="*80)
        
        return mongo_jsonify(
            success=True,
            table_data=table_data,
            new_alerts=new_alerts,
            total_stocks=len(table_data),
            cycle_time=cycle_end_time.strftime('%H:%M:%S')
        )
        
    except Exception as e:
        logger.error(f"Error getting OI table data: {e}")
        return error_response(str(e))


@app.route('/api/get-oi-alerts')
def get_oi_alerts():
    """Get all OI alerts from today (Total OI alerts)"""
    try:
        current_time = datetime.now(IST)
        today_start = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
        
        alerts = list(oi_alerts_collection.find(
            {'timestamp': {'$gte': today_start}},
            {'_id': 0}
        ).sort('timestamp', -1))
        
        # Format timestamps for display
        for alert in alerts:
            alert['timestamp'] = alert['timestamp'].strftime('%H:%M:%S')
        
        return mongo_jsonify(
            success=True,
            alerts=alerts,
            alert_count=len(alerts)
        )
        
    except Exception as e:
        logger.error(f"Error getting OI alerts: {e}")
        return error_response(str(e))


@app.route('/api/get-oi-change-alerts')
def get_oi_change_alerts():
    """Get all strike-level OI change alerts from today (change <= -100)"""
    try:
        current_time = datetime.now(IST)
        today_start = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Get all alerts from today
        all_alerts = list(oi_strike_daily_alerts_collection.find(
            {'timestamp': {'$gte': today_start}},
            {'_id': 0}
        ).sort('timestamp', -1))  # Sort by timestamp descending (newest first)
        
        # Keep only the most recent alert for each symbol
        symbol_latest_alerts = {}
        for alert in all_alerts:
            symbol = alert['symbol']
            if symbol not in symbol_latest_alerts:
                symbol_latest_alerts[symbol] = alert
        
        # Convert back to list
        alerts = list(symbol_latest_alerts.values())
        
        # Sort alerts by timestamp (newest first)
        alerts.sort(key=lambda x: x['timestamp'], reverse=True)
        
        # Convert UTC timestamps to IST and format for display
        for alert in alerts:
            # MongoDB stores timestamps in UTC, convert to IST
            if isinstance(alert['timestamp'], datetime):
                # Add 5 hours and 30 minutes to convert from UTC to IST
                ist_time = alert['timestamp'] + timedelta(hours=5, minutes=30)
                alert['timestamp'] = ist_time.strftime('%H:%M:%S')
            else:
                # If it's already a string, keep it as is
                alert['timestamp'] = alert['timestamp']
        
        return mongo_jsonify(
            success=True,
            alerts=alerts,
            alert_count=len(alerts)
        )
        
    except Exception as e:
        logger.error(f"Error getting OI strike change alerts: {e}")
        return error_response(str(e))


@app.route('/api/monitor-oi-strike-changes', methods=['POST'])
def monitor_oi_strike_changes():
    """Monitor live OI changes at strike level and trigger alerts when change <= -100"""
    try:
        logger.info("="*80)
        logger.info("MONITORING OI STRIKE CHANGES")
        logger.info("="*80)
        
        # Load instruments cache
        load_instruments_cache()
        
        # Get all stored previous day OI strike data
        previous_strikes_data = list(oi_strikes_collection.find({}))
        
        if not previous_strikes_data:
            return error_response('No previous day OI strikes data found. Please refresh data first.', 400)
        
        logger.info(f"Found {len(previous_strikes_data)} strike records to monitor")
        
        # Group by symbol for processing
        symbols_data = {}
        for record in previous_strikes_data:
            symbol = record['symbol']
            if symbol not in symbols_data:
                symbols_data[symbol] = {
                    'expiry': record['expiry'],
                    'strikes': []
                }
            symbols_data[symbol]['strikes'].append(record)
        
        logger.info(f"Processing {len(symbols_data)} stocks...")
        
        new_alerts = []
        processed_count = 0
        
        for symbol, data in symbols_data.items():
            try:
                logger.info(f"[{symbol}] Checking OI changes...")
                expiry_date = datetime.strptime(data['expiry'], '%Y-%m-%d').date()
                
                # Get current spot price
                spot_price = get_spot_price(symbol)
                if not spot_price:
                    logger.warning(f"[{symbol}] Could not get spot price, skipping...")
                    continue
                
                # Get +/- 2 strike prices around the spot price (5 strikes total: 2 below + spot + 2 above)
                logger.info(f"[{symbol}] Current Spot: {spot_price}, selecting 5 strikes (spot ± 2)...")
                ce_strikes = find_nearest_strikes(symbol, spot_price, expiry_date, 'CE', num_strikes=5)
                pe_strikes = find_nearest_strikes(symbol, spot_price, expiry_date, 'PE', num_strikes=5)
                
                logger.info(f"[{symbol}] Monitoring CE strikes: {ce_strikes}")
                logger.info(f"[{symbol}] Monitoring PE strikes: {pe_strikes}")
                
                # Combine all strike prices we want to check
                valid_strikes = set()
                for strike in ce_strikes:
                    valid_strikes.add((strike, 'CE'))
                for strike in pe_strikes:
                    valid_strikes.add((strike, 'PE'))
                
                logger.info(f"[{symbol}] Total strikes to monitor: {len(valid_strikes)} (checking for OI drops >= -100 lots)")
                
                # Check OI changes only for the valid strikes
                for strike_record in data['strikes']:
                    strike_price = strike_record['strike_price']
                    option_type = strike_record['option_type']
                    
                    # Skip if not in our valid strikes list
                    if (strike_price, option_type) not in valid_strikes:
                        continue
                    
                    prev_oi = strike_record['oi']
                    
                    # Get live OI for this strike
                    live_oi = get_strike_oi_live(symbol, strike_price, option_type, expiry_date)
                    
                    # Calculate change in OI (contracts)
                    # Correct formula: live_oi - prev_oi (positive means increase, negative means decrease)
                    change_in_oi = live_oi - prev_oi
                    
                    # Get lot size for this symbol
                    lot_size = get_lot_size_for_symbol(symbol)
                    if not lot_size:
                        logger.warning(f"No lot size found for {symbol}, using default lot size of 1")
                        lot_size = 1  # Default to 1 if not found
                    
                    # Calculate change in lots - ensure we're using the correct lot size
                    # We're dividing the change in contracts by the lot size
                    # Make sure to handle the case where lot_size might be 0 or None
                    if lot_size and lot_size > 0:
                        change_in_oi_lots = change_in_oi / lot_size
                    else:
                        change_in_oi_lots = change_in_oi
                    
                    # Log the calculation for debugging
                    logger.info(f"[{symbol}] {strike_price} {option_type} - Prev OI: {prev_oi}, Live OI: {live_oi}, Change: {change_in_oi} contracts, Lot Size: {lot_size}, Change in Lots: {change_in_oi_lots}")
                    
                    # Ensure we're using IST timezone for timestamps
                    current_time = datetime.now(IST)
                    
                    # Check if change in lots <= -100 (significant decrease in OI)
                    if change_in_oi_lots <= -100:
                        logger.warning(f"⚠️ [{symbol}] {option_type} Strike {strike_price}: OI dropped by {change_in_oi_lots:.2f} lots (Prev: {prev_oi:,} → Live: {live_oi:,})")
                        
                        # Check if we already have an alert for this symbol today
                        today_start = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
                        existing_alert = oi_strike_daily_alerts_collection.find_one({
                            'symbol': symbol,
                            'timestamp': {'$gte': today_start}
                        })
                        
                        # Only store a new alert if:
                        # 1. There's no existing alert for this symbol today, or
                        # 2. The change is more significant than the existing alert
                        if not existing_alert or change_in_oi_lots < existing_alert.get('change_in_oi_lots', 0):
                            # Store in the daily alerts collection
                            daily_alert_doc = {
                                'symbol': symbol,
                                'strike_price': strike_price,
                                'option_type': option_type,
                                'prev_oi': int(prev_oi),
                                'live_oi': int(live_oi),
                                'change_in_oi': int(change_in_oi),
                                'change_in_oi_lots': round(change_in_oi_lots, 2),
                                'lot_size': lot_size,
                                'timestamp': current_time
                            }
                            
                            # If there's an existing alert, update it; otherwise insert a new one
                            if existing_alert:
                                oi_strike_daily_alerts_collection.update_one(
                                    {'_id': existing_alert['_id']},
                                    {'$set': daily_alert_doc}
                                )
                                logger.info(f"✅ [{symbol}] Updated existing alert (previous: {existing_alert.get('change_in_oi_lots', 0):.2f} lots → current: {change_in_oi_lots:.2f} lots)")
                            else:
                                oi_strike_daily_alerts_collection.insert_one(daily_alert_doc)
                                logger.info(f"✅ [{symbol}] Created new alert for today")
                            
                            # Store in the original alerts collection too for backward compatibility
                            oi_strike_alerts_collection.insert_one(daily_alert_doc.copy())
                            
                            # Log the alert
                            logger.warning(f"🚨 OI ALERT SAVED: {symbol} {option_type} Strike {strike_price} dropped by {change_in_oi_lots:.2f} lots")
                        
                        # Add to response as a simple list item
                        new_alerts.append({
                            'symbol': symbol,
                            'strike_price': strike_price,
                            'option_type': option_type,
                            'prev_oi': int(prev_oi),
                            'live_oi': int(live_oi),
                            'change_in_oi': int(change_in_oi),
                            'change_in_oi_lots': round(change_in_oi_lots, 2),
                            'lot_size': lot_size,
                            'timestamp': current_time.strftime('%H:%M:%S')
                        })
                        
                processed_count += 1
                
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
                continue
        
        logger.info("="*80)
        logger.info(f"OI Strike monitoring completed! Processed: {processed_count} stocks")
        logger.info(f"New alerts: {len(new_alerts)}")
        logger.info("="*80)
        
        return mongo_jsonify(
            success=True,
            message='OI strike monitoring completed',
            processed_count=processed_count,
            new_alerts=new_alerts,
            alert_count=len(new_alerts)
        )
        
    except Exception as e:
        logger.error(f"Error monitoring OI strikes: {e}")
        return error_response(str(e))


# ============================================================================
# OPTIONS CHAIN & HISTORICAL DATA ROUTES
# ============================================================================

@app.route('/options')
def options_chain_page():
    """Options chain page - live data for all strikes"""
    return render_template('options_chain.html', symbols=config.STOCK_SYMBOLS)


@app.route('/historical')
def historical_page():
    """Historical data page - historical data for specific date/time"""
    return render_template('historical.html', symbols=config.STOCK_SYMBOLS)


@app.route('/get_expiry_dates', methods=['POST'])
def get_expiry_dates_route():
    """API: Get expiry dates for a symbol"""
    try:
        data = request.get_json()
        symbol = data.get('symbol')
        
        if not symbol:
            return jsonify({'error': 'Symbol is required'}), 400
        
        expiry_dates = get_expiry_dates_for_symbol(symbol)
        
        if expiry_dates:
            return jsonify({'expiry_dates': expiry_dates})
        else:
            return jsonify({'error': 'Failed to fetch expiry dates'}), 500
            
    except Exception as e:
        logger.error(f"Error in get_expiry_dates: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/get_options_chain', methods=['POST'])
def get_options_chain_route():
    """API: Get options chain data"""
    try:
        data = request.get_json()
        symbol = data.get('symbol')
        expiry_date = data.get('expiry_date')
        
        if not symbol or not expiry_date:
            return jsonify({'error': 'Symbol and expiry date are required'}), 400
        
        options_chain = get_options_chain_data(symbol, expiry_date)
        
        if options_chain:
            return jsonify({'options_chain': options_chain})
        else:
            return jsonify({'error': 'Failed to fetch options chain'}), 500
            
    except Exception as e:
        logger.error(f"Error in get_options_chain: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/get_historical_data', methods=['POST'])
def get_historical_data_route():
    """API: Get historical options chain data for a specific date/time"""
    try:
        data = request.get_json()
        symbol = data.get('symbol')
        expiry_date = data.get('expiry_date')
        date_str = data.get('date')
        time_str = data.get('time', '15:30')
        
        if not symbol or not expiry_date or not date_str:
            return jsonify({'error': 'Symbol, expiry date, and date are required'}), 400
        
        # Determine exchange
        if symbol in ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]:
            exchange = "NFO"
        elif symbol == "SENSEX":
            exchange = "BFO"
        else:
            exchange = "NFO"
        
        # Parse target datetime
        target_datetime = IST.localize(datetime.strptime(f"{date_str} {time_str}:00", '%Y-%m-%d %H:%M:%S'))
        
        logger.info(f"Fetching historical data: {symbol} expiry {expiry_date} at {target_datetime}")
        
        # Get instruments
        instruments = kite.instruments(exchange)
        expiry_dt = datetime.strptime(expiry_date, '%Y-%m-%d').date()
        
        # Filter options
        calls = []
        puts = []
        
        for instrument in instruments:
            instrument_name = instrument.get('name', '')
            trading_symbol = instrument.get('tradingsymbol', '')
            instrument_expiry = instrument.get('expiry')
            
            if instrument_expiry:
                if hasattr(instrument_expiry, 'date'):
                    instrument_expiry = instrument_expiry.date()
            
            if ((instrument_name == symbol or trading_symbol.startswith(symbol)) and 
                instrument_expiry and instrument_expiry == expiry_dt):
                
                lot_size = instrument.get('lot_size', 1)
                
                if instrument.get('instrument_type') == 'CE':
                    calls.append({
                        'strike': instrument['strike'],
                        'tradingsymbol': instrument['tradingsymbol'],
                        'instrument_token': instrument['instrument_token'],
                        'lot_size': lot_size
                    })
                elif instrument.get('instrument_type') == 'PE':
                    puts.append({
                        'strike': instrument['strike'],
                        'tradingsymbol': instrument['tradingsymbol'],
                        'instrument_token': instrument['instrument_token'],
                        'lot_size': lot_size
                    })
        
        if not calls and not puts:
            return jsonify({'error': f'No options found for {symbol} with expiry {expiry_date}'}), 404
        
        calls.sort(key=lambda x: x['strike'])
        puts.sort(key=lambda x: x['strike'])
        
        all_strikes = sorted(set([c['strike'] for c in calls] + [p['strike'] for p in puts]))
        
        if not all_strikes:
            return jsonify({'error': 'No strikes available'}), 404
        
        # Get historical spot price at the target time
        spot_price = None
        try:
            if symbol in ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]:
                spot_key = f"NSE:{symbol}"
            elif symbol == "SENSEX":
                spot_key = f"BSE:{symbol}"
            else:
                spot_key = f"NSE:{symbol}"
            
            # Get instrument token for spot
            spot_instrument_token = get_instrument_token(symbol)
            if spot_instrument_token:
                spot_hist = kite.historical_data(
                    instrument_token=spot_instrument_token,
                    from_date=target_datetime.date(),
                    to_date=target_datetime.date(),
                    interval='minute'
                )
                
                if spot_hist:
                    # Find closest time
                    closest_idx = min(range(len(spot_hist)), 
                                    key=lambda i: abs((spot_hist[i]['date'] - target_datetime).total_seconds()))
                    spot_price = spot_hist[closest_idx]['close']
                    logger.info(f"Historical spot price for {symbol} at {target_datetime}: {spot_price}")
        except Exception as e:
            logger.warning(f"Could not fetch historical spot price: {str(e)}")
        
        # If no spot price, use middle strike as approximation
        if not spot_price or spot_price <= 0:
            spot_price = all_strikes[len(all_strikes) // 2]
            logger.warning(f"Using middle strike as spot approximation: {spot_price}")
        
        # Find ATM strike - Round to NEAREST strike
        atm_strike = min(all_strikes, key=lambda x: abs(x - spot_price))
        atm_index = all_strikes.index(atm_strike)
        
        # Select 21 strikes: ATM + 10 below + 10 above = 21 total
        start_index = max(0, atm_index - 10)
        end_index = min(len(all_strikes), atm_index + 11)
        display_strikes = all_strikes[start_index:end_index]
        
        logger.info(f"Historical spot: {spot_price}, ATM strike: {atm_strike}, Showing {len(display_strikes)} strikes (ATM ±10)")
        
        calls_dict = {c['strike']: c for c in calls}
        puts_dict = {p['strike']: p for p in puts}
        
        # Find previous trading day
        previous_day = target_datetime.date() - timedelta(days=1)
        for _ in range(10):
            while previous_day.weekday() >= 5:
                previous_day = previous_day - timedelta(days=1)
            
            try:
                sample_token = calls[0]['instrument_token'] if calls else puts[0]['instrument_token']
                hist_data = kite.historical_data(
                    instrument_token=sample_token,
                    from_date=previous_day,
                    to_date=previous_day,
                    interval='day'
                )
                if hist_data and len(hist_data) > 0:
                    logger.info(f"Found previous trading day: {previous_day}")
                    break
            except:
                pass
            
            previous_day = previous_day - timedelta(days=1)
        
        # Fetch historical data for ONLY displayed 21 strikes
        options_chain = []
        
        # Initialize totals for ONLY the 21 displayed strikes
        total_call_volume = 0
        total_put_volume = 0
        total_call_oi = 0
        total_put_oi = 0
        total_call_change_oi = 0
        total_put_change_oi = 0
        
        for strike in display_strikes:
            row = {
                'strike': strike,
                'is_atm': strike == atm_strike,
                'spot_price': spot_price
            }
            
            # Put data
            if strike in puts_dict:
                put = puts_dict[strike]
                try:
                    put_hist = kite.historical_data(
                        instrument_token=put['instrument_token'],
                        from_date=target_datetime.date(),
                        to_date=target_datetime.date(),
                        interval='minute',
                        oi=1
                    )
                    
                    if put_hist:
                        closest_idx = min(range(len(put_hist)), 
                                        key=lambda i: abs((put_hist[i]['date'] - target_datetime).total_seconds()))
                        closest_record = put_hist[closest_idx]
                        
                        current_oi_qty = closest_record.get('oi', 0)
                        current_oi = current_oi_qty / put['lot_size'] if current_oi_qty and put['lot_size'] > 0 else 0
                        
                        volume_qty = sum(candle.get('volume', 0) for i, candle in enumerate(put_hist) if i <= closest_idx)
                        volume_lots = volume_qty / put['lot_size'] if put['lot_size'] > 0 else 0
                        
                        # Get previous day OI
                        change_in_oi = 0
                        prev_close = 0
                        change = 0
                        change_percent = 0
                        
                        try:
                            prev_day_hist = kite.historical_data(
                                instrument_token=put['instrument_token'],
                                from_date=previous_day,
                                to_date=previous_day,
                                interval='day',
                                oi=1
                            )
                            if prev_day_hist and len(prev_day_hist) > 0:
                                prev_oi_qty = prev_day_hist[0].get('oi', 0)
                                prev_oi_lots = prev_oi_qty / put['lot_size'] if prev_oi_qty and put['lot_size'] > 0 else 0
                                change_in_oi = current_oi - prev_oi_lots
                                prev_close = prev_day_hist[0].get('close', 0)
                                if prev_close > 0:
                                    change = closest_record['close'] - prev_close
                                    change_percent = (change / prev_close) * 100
                        except:
                            pass
                        
                        row['put'] = {
                            'ltp': closest_record['close'],
                            'change': change,
                            'change_percent': change_percent,
                            'prev_close': prev_close,
                            'open': closest_record['open'],
                            'high': closest_record['high'],
                            'low': closest_record['low'],
                            'volume': volume_lots,
                            'oi': current_oi,
                            'change_in_oi': change_in_oi,
                            'iv': 0,
                            'lot_size': put['lot_size'],
                            'bid_qty': 0,
                            'bid_price': 0,
                            'ask_price': 0,
                            'ask_qty': 0,
                            'timestamp': closest_record['date'].strftime('%Y-%m-%d %H:%M:%S')
                        }
                        
                        # Add to PUT totals (ONLY for displayed strikes)
                        total_put_volume += volume_lots
                        total_put_oi += current_oi
                        total_put_change_oi += change_in_oi
                except Exception as e:
                    logger.error(f"Error fetching PUT data for strike {strike}: {str(e)}")
            
            # Call data
            if strike in calls_dict:
                call = calls_dict[strike]
                try:
                    call_hist = kite.historical_data(
                        instrument_token=call['instrument_token'],
                        from_date=target_datetime.date(),
                        to_date=target_datetime.date(),
                        interval='minute',
                        oi=1
                    )
                    
                    if call_hist:
                        closest_idx = min(range(len(call_hist)), 
                                        key=lambda i: abs((call_hist[i]['date'] - target_datetime).total_seconds()))
                        closest_record = call_hist[closest_idx]
                        
                        current_oi_qty = closest_record.get('oi', 0)
                        current_oi = current_oi_qty / call['lot_size'] if current_oi_qty and call['lot_size'] > 0 else 0
                        
                        volume_qty = sum(candle.get('volume', 0) for i, candle in enumerate(call_hist) if i <= closest_idx)
                        volume_lots = volume_qty / call['lot_size'] if call['lot_size'] > 0 else 0
                        
                        # Get previous day OI
                        change_in_oi = 0
                        prev_close = 0
                        change = 0
                        change_percent = 0
                        
                        try:
                            prev_day_hist = kite.historical_data(
                                instrument_token=call['instrument_token'],
                                from_date=previous_day,
                                to_date=previous_day,
                                interval='day',
                                oi=1
                            )
                            if prev_day_hist and len(prev_day_hist) > 0:
                                prev_oi_qty = prev_day_hist[0].get('oi', 0)
                                prev_oi_lots = prev_oi_qty / call['lot_size'] if prev_oi_qty and call['lot_size'] > 0 else 0
                                change_in_oi = current_oi - prev_oi_lots
                                prev_close = prev_day_hist[0].get('close', 0)
                                if prev_close > 0:
                                    change = closest_record['close'] - prev_close
                                    change_percent = (change / prev_close) * 100
                        except:
                            pass
                        
                        row['call'] = {
                            'ltp': closest_record['close'],
                            'change': change,
                            'change_percent': change_percent,
                            'prev_close': prev_close,
                            'open': closest_record['open'],
                            'high': closest_record['high'],
                            'low': closest_record['low'],
                            'volume': volume_lots,
                            'oi': current_oi,
                            'change_in_oi': change_in_oi,
                            'iv': 0,
                            'lot_size': call['lot_size'],
                            'bid_qty': 0,
                            'bid_price': 0,
                            'ask_price': 0,
                            'ask_qty': 0,
                            'timestamp': closest_record['date'].strftime('%Y-%m-%d %H:%M:%S')
                        }
                        
                        # Add to CALL totals (ONLY for displayed strikes)
                        total_call_volume += volume_lots
                        total_call_oi += current_oi
                        total_call_change_oi += change_in_oi
                except Exception as e:
                    logger.error(f"Error fetching CALL data for strike {strike}: {str(e)}")
            
            if 'put' in row or 'call' in row:
                options_chain.append(row)
        
        # Add totals to metadata
        if options_chain:
            options_chain[0]['total_call_volume'] = total_call_volume
            options_chain[0]['total_put_volume'] = total_put_volume
            options_chain[0]['total_call_oi'] = total_call_oi
            options_chain[0]['total_put_oi'] = total_put_oi
            options_chain[0]['total_call_change_oi'] = total_call_change_oi
            options_chain[0]['total_put_change_oi'] = total_put_change_oi
            
            logger.info(f"📊 Historical Totals (ONLY {len(options_chain)} displayed strikes):")
            logger.info(f"  CALL: Volume={total_call_volume:.0f}, OI={total_call_oi:.0f}, Change OI={total_call_change_oi:.0f}")
            logger.info(f"  PUT: Volume={total_put_volume:.0f}, OI={total_put_oi:.0f}, Change OI={total_put_change_oi:.0f}")
        
        return jsonify({
            'symbol': symbol,
            'expiry_date': expiry_date,
            'date': date_str,
            'time': time_str,
            'options_chain': options_chain
        })
        
    except Exception as e:
        logger.error(f"Error fetching historical data: {str(e)}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    import os
    # Get port from environment variable (Render provides this)
    port = int(os.environ.get('PORT', 5000))
    # Disable debug in production
    debug_mode = os.environ.get('FLASK_ENV', 'development') == 'development'
    # Run with increased timeout for long-running requests (150+ stocks)
    # threaded=True allows multiple requests simultaneously
    app.run(debug=debug_mode, host='0.0.0.0', port=port, threaded=True)
