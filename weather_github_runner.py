import requests
import json
import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional, Dict, List, Set


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('weather_service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Environment variables
THINGER_TOKEN = os.environ.get('THINGER_TOKEN')
THINGER_USERNAME = os.environ.get('THINGER_USERNAME', 'Alfacon')
THINGER_SERVER = os.environ.get('THINGER_SERVER', 'https://alfacon.aws.thinger.io')
WEATHER_API_KEY = os.environ.get('WEATHER_API_KEY')

# Constants
ATHENS_LAT = 37.9838
ATHENS_LON = 23.7275
DEVICE_PREFIX = "CAL"
DEVICE_START = 251
DEVICE_END = 351

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
REQUEST_TIMEOUT = 15  # seconds

def get_athens_weather() -> Optional[Dict]:
    """Get weather data for Athens, Greece with retry logic."""
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={ATHENS_LAT}&lon={ATHENS_LON}&appid={WEATHER_API_KEY}&units=metric"
    
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Fetching weather data (attempt {attempt + 1}/{MAX_RETRIES})")
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()

            temperature = data['main']['temp']
            humidity = data['main']['humidity']
            
            weather_info = {
                "temperature": temperature,
                "humidity": humidity,
                "city": "Athens",
                "country": "GR",
                "description": data['weather'][0].get('description', 'Unknown') if data.get('weather') else 'Unknown',
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            logger.info(f"✅ Weather: {temperature}°C, {humidity}% humidity, {weather_info['description']}")
            return weather_info
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"⚠️ Weather API attempt {attempt + 1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))  # Exponential backoff
            else:
                logger.error(f"❌ All weather API attempts failed")
                return None

def check_device_exists(device_id: str) -> bool:
    """Check if a device exists and has OutTemp resource."""
    headers = {
        "Authorization": f"Bearer {THINGER_TOKEN}",
        "Accept": "application/json"
    }
    
    url = f"{THINGER_SERVER}/v1/users/{THINGER_USERNAME}/devices/{device_id}/OutTemp"
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        return response.status_code == 200
    except Exception:
        return False

def discover_available_devices(device_range: List[str]) -> Set[str]:
    """Discover which devices are available in the full range."""
    logger.info(f"🔍 Discovering available devices from {len(device_range)} potential devices...")
    
    available_devices = set()
    
    # Use threading to check devices faster
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit all device checks
        future_to_device = {
            executor.submit(check_device_exists, device_id): device_id 
            for device_id in device_range
        }
        
        # Process results as they complete
        for future in as_completed(future_to_device):
            device_id = future_to_device[future]
            try:
                if future.result(timeout=15):
                    available_devices.add(device_id)
                    logger.info(f"✅ Found device: {device_id}")
            except Exception as e:
                logger.debug(f"⚠️ Device check failed for {device_id}: {e}")
    
    logger.info(f"🎯 Discovery complete: {len(available_devices)} devices available")
    return available_devices

def send_to_thinger_api(device_id: str, temperature: float) -> bool:
    """Send temperature data to a specific Thinger.io device with retry logic."""
    url = f"{THINGER_SERVER}/v1/users/{THINGER_USERNAME}/devices/{device_id}/OutTemp"
    
    headers = {
        "Authorization": f"Bearer {THINGER_TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "exterror": 0,
        "webout": float(temperature)
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT)
            if response.status_code == 200:
                logger.info(f"✅ {device_id}: {temperature}°C sent successfully")
                return True
            elif response.status_code == 404:
                logger.warning(f"⚠️ {device_id}: Device or resource not found")
                return False
            else:
                logger.warning(f"⚠️ {device_id}: HTTP {response.status_code}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"⚠️ {device_id} attempt {attempt + 1}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
    
    logger.error(f"❌ {device_id}: All attempts failed")
    return False

def process_device_batch(devices: List[str], temperature: float) -> dict:
    """Process a batch of available devices (no need to check existence again)."""
    results = {"success": 0, "failed": 0}
    
    for device_id in devices:
        # Send data directly (we already know these devices exist)
        if send_to_thinger_api(device_id, temperature):
            results["success"] += 1
        else:
            results["failed"] += 1
        
        # Small delay between devices to avoid rate limiting
        time.sleep(0.1)
    
    return results

def save_device_cache(available_devices: Set[str]):
    """Save discovered devices to a cache file for debugging."""
    try:
        cache_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "devices": sorted(list(available_devices)),
            "count": len(available_devices)
        }
        
        with open('device_cache.json', 'w') as f:
            json.dump(cache_data, f, indent=2)
        
        logger.info(f"💾 Device cache saved: {len(available_devices)} devices")
    except Exception as e:
        logger.warning(f"⚠️ Failed to save device cache: {e}")

def create_heartbeat_file():
    """Create a heartbeat file to track last successful run."""
    try:
        with open('last_run.txt', 'w') as f:
            f.write(datetime.now(timezone.utc).isoformat())
        logger.info("📝 Heartbeat file updated")
    except Exception as e:
        logger.warning(f"⚠️ Failed to create heartbeat file: {e}")

def main():
    """Main function with dynamic device discovery."""
    start_time = datetime.now(timezone.utc)
    
    logger.info("=" * 70)
    logger.info(f"🚀 Weather Update Service Started (Dynamic Discovery)")
    logger.info(f"⏰ Execution Time: {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    logger.info("=" * 70)
    
    # Validate environment variables
    missing_vars = []
    if not THINGER_TOKEN:
        missing_vars.append("THINGER_TOKEN")
    if not WEATHER_API_KEY:
        missing_vars.append("WEATHER_API_KEY")
    
    if missing_vars:
        logger.error(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        return
    
    logger.info(f"🏛️ Target: Athens, Greece ({ATHENS_LAT}, {ATHENS_LON})")
    logger.info(f"🔧 Server: {THINGER_SERVER}")
    logger.info(f"👤 Username: {THINGER_USERNAME}")
    
    # Generate full device range to search
    all_possible_devices = [f"{DEVICE_PREFIX}{i}" for i in range(DEVICE_START, DEVICE_END + 1)]
    logger.info(f"📡 Searching range: {len(all_possible_devices)} devices ({DEVICE_PREFIX}{DEVICE_START} to {DEVICE_PREFIX}{DEVICE_END})")
    
    # Step 1: Discover available devices
    available_devices = discover_available_devices(all_possible_devices)
    
    if not available_devices:
        logger.error("❌ No available devices found in the specified range. Aborting.")
        return
    
    # Convert to sorted list for consistent ordering
    available_device_list = sorted(list(available_devices))
    logger.info(f"📋 Available devices: {', '.join(available_device_list)}")
    
    # Save device discovery results
    save_device_cache(available_devices)
    
    # Step 2: Get weather data
    logger.info("🌤️ Fetching weather data...")
    weather = get_athens_weather()
    
    if not weather:
        logger.error("❌ Failed to get weather data. Aborting run.")
        return
    
    temperature = weather['temperature']
    logger.info(f"🌡️ Temperature: {temperature}°C")
    logger.info(f"💧 Humidity: {weather['humidity']}%")
    logger.info(f"☁️ Conditions: {weather['description']}")
    
    # Step 3: Send data to all available devices
    logger.info(f"📤 Starting updates for {len(available_device_list)} available devices...")
    
    batch_size = 8
    total_success = 0
    total_failed = 0
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        
        # Process available devices in batches
        for i in range(0, len(available_device_list), batch_size):
            batch = available_device_list[i:i + batch_size]
            batch_num = i // batch_size + 1
            logger.info(f"🔄 Processing batch {batch_num} ({len(batch)} devices): {', '.join(batch)}")
            
            future = executor.submit(process_device_batch, batch, temperature)
            futures.append(future)
        
        # Process results as they complete
        for future in as_completed(futures):
            try:
                result = future.result(timeout=30)
                total_success += result["success"]
                total_failed += result["failed"]
            except Exception as e:
                logger.error(f"❌ Batch processing error: {e}")
                total_failed += batch_size
    
    # Calculate execution time
    end_time = datetime.now(timezone.utc)
    execution_time = (end_time - start_time).total_seconds()
    
    # Create heartbeat file
    create_heartbeat_file()
    
    # Final summary
    logger.info("\n" + "=" * 70)
    logger.info(f"📊 EXECUTION SUMMARY")
    logger.info("=" * 70)
    logger.info(f"🌡️ Athens Temperature: {temperature}°C ({weather['description']})")
    logger.info(f"🔍 Devices Searched: {len(all_possible_devices)} ({DEVICE_PREFIX}{DEVICE_START} to {DEVICE_PREFIX}{DEVICE_END})")
    logger.info(f"📱 Available Devices: {len(available_device_list)}")
    logger.info(f"✅ Successful Updates: {total_success}")
    logger.info(f"❌ Failed Updates: {total_failed}")
    logger.info(f"📈 Success Rate: {(total_success/len(available_device_list)*100):.1f}%" if available_device_list else "N/A")
    logger.info(f"⏱️ Execution Time: {execution_time:.2f} seconds")
    logger.info("=" * 70)
    
    # Success criteria: Based on available devices only
    if available_device_list:
        success_rate = (total_success / len(available_device_list)) * 100
        if success_rate < 80:
            logger.error(f"❌ Low success rate ({success_rate:.1f}%) on available devices")
            exit(1)
        else:
            logger.info(f"🎯 Excellent! {success_rate:.1f}% success rate on available devices")
    
    logger.info("🎯 Weather service execution completed successfully!")

if __name__ == "__main__":
    main()
