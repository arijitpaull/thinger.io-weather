import requests
import time
import json
import os
from concurrent.futures import ThreadPoolExecutor
import logging
from typing import Optional, Dict, List

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("weather_thinger.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# === CONFIGURATION ===

# Your Thinger.io details
THINGER_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiJ3ZWF0aGVydXBkYXRlIiwic3ZyIjoiYWxmYWNvbi5hd3MudGhpbmdlci5pbyIsInVzciI6IkFsZmFjb24ifQ.-gzjtN1pZbZiWWdyk9kreEpxWkHmAbWN57M9sJVrknk"
THINGER_USERNAME = "Alfacon"
THINGER_SERVER = "https://alfacon.aws.thinger.io"

# Your OpenWeatherMap API details
WEATHER_API_KEY = "a18f09605bb56a5e2bdb573549f79b8a"

# Athens, Greece coordinates
ATHENS_LAT = 37.9838
ATHENS_LON = 23.7275

# Device configuration
DEVICE_PREFIX = "CAL"
DEVICE_START = 251
DEVICE_END = 351

# Device configuration file path
DEVICES_CONFIG_FILE = "devices.json"

# === FUNCTIONS ===

def load_devices_config():
    """Load device IDs from file or generate them."""
    try:
        if not os.path.exists(DEVICES_CONFIG_FILE):
            # Generate all device IDs from CAL251 to CAL351
            devices = []
            for i in range(DEVICE_START, DEVICE_END + 1):
                devices.append({
                    "device_id": f"{DEVICE_PREFIX}{i}",
                    "_comment": f"Device {i} - Athens, Greece location"
                })
            
            with open(DEVICES_CONFIG_FILE, 'w') as f:
                json.dump(devices, f, indent=4)
            logger.info(f"Created device configuration file with {len(devices)} devices")
            return devices
        
        with open(DEVICES_CONFIG_FILE, 'r') as f:
            devices = json.load(f)
            logger.info(f"Loaded {len(devices)} devices from configuration")
            return devices
    except Exception as e:
        logger.error(f"Error loading device configuration: {e}")
        # Fallback: generate devices in memory
        devices = []
        for i in range(DEVICE_START, DEVICE_END + 1):
            devices.append({"device_id": f"{DEVICE_PREFIX}{i}"})
        return devices

def get_athens_weather() -> Optional[Dict]:
    """Get weather data for Athens, Greece."""
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={ATHENS_LAT}&lon={ATHENS_LON}&appid={WEATHER_API_KEY}&units=metric"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        temperature = data['main']['temp']
        humidity = data['main']['humidity']
        
        weather_info = {
            "temperature": temperature,
            "humidity": humidity,
            "city": "Athens",
            "country": "GR",
            "description": data['weather'][0].get('description', 'Unknown') if data.get('weather') else 'Unknown'
        }
        
        logger.info(f"Athens weather: {temperature}°C, {humidity}% humidity, {weather_info['description']}")
        return weather_info
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching weather data for Athens: {e}")
        return None

def check_device_exists(device_id: str) -> bool:
    """Check if a device exists by testing OutTemp resource access."""
    headers = {
        "Authorization": f"Bearer {THINGER_TOKEN}",
        "Accept": "application/json"
    }
    
    # Check OutTemp resource directly since v3 API doesn't work but v1 does
    url = f"{THINGER_SERVER}/v1/users/{THINGER_USERNAME}/devices/{device_id}/OutTemp"
    
    try:
        response = requests.get(url, headers=headers)
        return response.status_code == 200
    except Exception as e:
        logger.debug(f"Error checking device {device_id}: {e}")
        return False

def send_to_thinger_api(device_id: str, temperature: float) -> bool:
    """Send temperature data to a specific Thinger.io device using the API resource."""
    url = f"{THINGER_SERVER}/v1/users/{THINGER_USERNAME}/devices/{device_id}/OutTemp"
    
    headers = {
        "Authorization": f"Bearer {THINGER_TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "exterror": 0,
        "webout": float(temperature)
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            logger.info(f"[Success] Device {device_id}: Temperature {temperature}°C sent successfully")
            return True
        else:
            if response.status_code == 404:
                logger.error(f"[Error] Device {device_id}: Device or OutTemp resource not found")
            else:
                logger.error(f"[Error] Device {device_id}: Status {response.status_code}")
                logger.debug(f"Response: {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"[Error] Device {device_id}: {e}")
        return False

def process_device_batch(devices: List[Dict], temperature: float) -> dict:
    """Process a batch of devices."""
    results = {"success": 0, "failed": 0, "not_found": 0}
    
    for device in devices:
        device_id = device["device_id"]
        
        # First check if device exists
        if not check_device_exists(device_id):
            logger.warning(f"Device {device_id} does not exist in Thinger.io")
            results["not_found"] += 1
            continue
        
        # Send temperature data
        if send_to_thinger_api(device_id, temperature):
            results["success"] += 1
        else:
            results["failed"] += 1
    
    return results

def update_all_devices():
    """Fetch Athens weather once and update all devices."""
    devices = load_devices_config()
    
    if not devices:
        logger.warning("No devices configured")
        return
    
    # Get Athens weather
    logger.info("Fetching weather data for Athens, Greece...")
    weather = get_athens_weather()
    
    if not weather:
        logger.error("Failed to get weather data. Skipping this update cycle.")
        return
    
    temperature = weather['temperature']
    logger.info(f"Starting update for {len(devices)} devices with Athens temperature: {temperature}°C")
    
    # Process devices in batches
    batch_size = 10
    total_success = 0
    total_failed = 0
    total_not_found = 0
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        
        for i in range(0, len(devices), batch_size):
            batch = devices[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} devices)")
            
            future = executor.submit(process_device_batch, batch, temperature)
            futures.append(future)
            
            # Small delay between batches
            if i + batch_size < len(devices):
                time.sleep(1)
        
        # Collect results
        for future in futures:
            result = future.result()
            total_success += result["success"]
            total_failed += result["failed"]
            total_not_found += result["not_found"]
    
    # Summary
    logger.info("=" * 60)
    logger.info(f"Update completed for Athens, Greece ({temperature}°C)")
    logger.info(f"Total devices: {len(devices)}")
    logger.info(f"Successful updates: {total_success}")
    logger.info(f"Failed updates: {total_failed}")
    logger.info(f"Devices not found: {total_not_found}")
    logger.info("=" * 60)
    
    if total_not_found > 0:
        logger.warning(f"Note: {total_not_found} devices do not exist in Thinger.io")
        logger.warning("These devices need to be created before they can receive data")

def test_connection():
    """Test if the token works by checking OutTemp access."""
    headers = {"Authorization": f"Bearer {THINGER_TOKEN}"}
    
    # Test with CAL251 (first new device)
    test_device = "CAL251"
    
    logger.info("Testing Thinger.io connection...")
    
    # Test OutTemp access directly since that's what we'll use
    logger.info(f"Testing OutTemp access on {test_device}...")
    outtemp_url = f"{THINGER_SERVER}/v1/users/{THINGER_USERNAME}/devices/{test_device}/OutTemp"
    
    try:
        response = requests.get(outtemp_url, headers=headers)
        if response.status_code == 200:
            logger.info(f"✅ Can access OutTemp on {test_device}")
        elif response.status_code == 404:
            logger.warning(f"⚠️  Device {test_device} or OutTemp resource not found")
            logger.warning("Make sure devices are created with OutTemp resource")
        elif response.status_code == 401:
            logger.error("❌ Authentication failed. Check your token.")
            return False
        else:
            logger.warning(f"⚠️  OutTemp access returned: {response.status_code}")
    except Exception as e:
        logger.error(f"❌ Error accessing OutTemp: {e}")
        return False
    
    # Test weather API
    logger.info("Testing weather API...")
    if get_athens_weather():
        logger.info("✅ Weather API working")
    else:
        logger.error("❌ Weather API failed")
        return False
    
    return True

def display_service_info():
    """Display service information and current configuration."""
    logger.info("=" * 60)
    logger.info("WEATHER SERVICE CONFIGURATION")
    logger.info("=" * 60)
    logger.info(f"Server: {THINGER_SERVER}")
    logger.info(f"Username: {THINGER_USERNAME}")
    logger.info(f"Location: Athens, Greece ({ATHENS_LAT}, {ATHENS_LON})")
    logger.info(f"Devices: {DEVICE_PREFIX}{DEVICE_START} to {DEVICE_PREFIX}{DEVICE_END}")
    logger.info("Update interval: 30 minutes")
    logger.info("=" * 60)

# === MAIN LOOP ===

def main():
    display_service_info()
    
    # Test connection before starting
    if not test_connection():
        logger.error("Critical tests failed. Please check your configuration.")
        return
    
    # Check how many devices exist
    logger.info("\nChecking device availability...")
    devices = load_devices_config()
    existing_count = 0
    
    for device in devices[:10]:  # Check first 10 as sample
        if check_device_exists(device["device_id"]):
            existing_count += 1
    
    if existing_count == 0:
        logger.warning("\n⚠️  WARNING: No devices found in first 10 checked!")
        logger.warning("The devices need to be created in Thinger.io before data can be sent.")
        logger.warning("Continuing anyway - the service will skip non-existent devices.")
    else:
        logger.info(f"Found {existing_count}/10 devices in sample check")
    
    # Give user chance to cancel
    logger.info("\nStarting service in 5 seconds (Ctrl+C to cancel)...")
    try:
        time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Cancelled by user")
        return
    
    # Run one update immediately
    logger.info("\nRunning initial update...")
    update_all_devices()
    
    # Then continue with scheduled updates
    while True:
        try:
            next_update_time = time.localtime(time.time() + 1800)
            logger.info(f"\nNext update scheduled for {time.strftime('%Y-%m-%d %H:%M:%S', next_update_time)}")
            logger.info("Sleeping for 30 minutes...")
            time.sleep(1800)  # Sleep for 30 minutes
            
            logger.info(f"\n{'='*60}")
            logger.info(f"Starting weather update cycle at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"{'='*60}")
            update_all_devices()
            
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            logger.info("Waiting 60 seconds before retry...")
            time.sleep(60)

if __name__ == "__main__":
    try:
        logger.info("Weather-to-Thinger.io service starting...")
        logger.info("This service updates all devices with Athens, Greece weather")
        main()
    except KeyboardInterrupt:
        logger.info("\nService stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")