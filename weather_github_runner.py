import requests
import json
import os
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional, Dict, List


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
    """Check if a device exists with retry logic."""
    headers = {
        "Authorization": f"Bearer {THINGER_TOKEN}",
        "Accept": "application/json"
    }
    
    url = f"{THINGER_SERVER}/v1/users/{THINGER_USERNAME}/devices/{device_id}/OutTemp"
    
    for attempt in range(2):  # Only 2 attempts for device check
        try:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            return response.status_code == 200
        except Exception as e:
            logger.debug(f"Device check attempt {attempt + 1} failed for {device_id}: {e}")
            if attempt < 1:
                time.sleep(1)
    
    return False

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
    """Process a batch of devices with improved error handling."""
    results = {"success": 0, "failed": 0, "not_found": 0}
    
    for device_id in devices:
        # Check device existence first
        if not check_device_exists(device_id):
            logger.debug(f"⚠️ {device_id} does not exist")
            results["not_found"] += 1
            continue
        
        # Send data
        if send_to_thinger_api(device_id, temperature):
            results["success"] += 1
        else:
            results["failed"] += 1
        
        # Small delay between devices to avoid rate limiting
        time.sleep(0.1)
    
    return results

def create_heartbeat_file():
    """Create a heartbeat file to track last successful run."""
    try:
        with open('last_run.txt', 'w') as f:
            f.write(datetime.now(timezone.utc).isoformat())
        logger.info("📝 Heartbeat file updated")
    except Exception as e:
        logger.warning(f"⚠️ Failed to create heartbeat file: {e}")

def main():
    """Main function with comprehensive error handling and logging."""
    start_time = datetime.now(timezone.utc)
    
    logger.info("=" * 70)
    logger.info(f"🚀 Weather Update Service Started")
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
    
    # Generate device list
    devices = [f"{DEVICE_PREFIX}{i}" for i in range(DEVICE_START, DEVICE_END + 1)]
    logger.info(f"📡 Target devices: {len(devices)} ({DEVICE_PREFIX}{DEVICE_START} to {DEVICE_PREFIX}{DEVICE_END})")
    
    # Get weather data
    logger.info("🌤️ Fetching weather data...")
    weather = get_athens_weather()
    
    if not weather:
        logger.error("❌ Failed to get weather data. Aborting run.")
        return
    
    temperature = weather['temperature']
    logger.info(f"🌡️ Temperature: {temperature}°C")
    logger.info(f"💧 Humidity: {weather['humidity']}%")
    logger.info(f"☁️ Conditions: {weather['description']}")
    
    # Process devices in batches
    logger.info(f"📤 Starting updates for {len(devices)} devices...")
    
    batch_size = 8  # Reduced batch size for better reliability
    total_success = 0
    total_failed = 0
    total_not_found = 0
    
    with ThreadPoolExecutor(max_workers=3) as executor:  # Reduced workers
        futures = []
        
        for i in range(0, len(devices), batch_size):
            batch = devices[i:i + batch_size]
            batch_num = i // batch_size + 1
            logger.info(f"🔄 Submitting batch {batch_num} ({len(batch)} devices)")
            
            future = executor.submit(process_device_batch, batch, temperature)
            futures.append(future)
        
        # Process results as they complete
        for future in as_completed(futures):
            try:
                result = future.result(timeout=30)  # 30 second timeout per batch
                total_success += result["success"]
                total_failed += result["failed"]
                total_not_found += result["not_found"]
            except Exception as e:
                logger.error(f"❌ Batch processing error: {e}")
                total_failed += batch_size  # Assume all failed in this batch
    
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
    logger.info(f"📡 Total Devices: {len(devices)}")
    logger.info(f"✅ Successful Updates: {total_success}")
    logger.info(f"❌ Failed Updates: {total_failed}")
    logger.info(f"⚠️ Devices Not Found: {total_not_found}")
    logger.info(f"📈 Success Rate: {(total_success/len(devices)*100):.1f}%")
    logger.info(f"⏱️ Execution Time: {execution_time:.2f} seconds")
    logger.info("=" * 70)
    
    if total_not_found > 0:
        logger.info(f"ℹ️ Note: {total_not_found} devices need to be created in Thinger.io")
    
    # Set exit code based on success rate
    success_rate = (total_success / len(devices)) * 100
    if success_rate < 50:
        logger.error("❌ Low success rate detected - this may indicate a systemic issue")
        exit(1)
    
    logger.info("🎯 Weather service execution completed!")

if __name__ == "__main__":
    main()
