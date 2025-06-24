import requests
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Optional, Dict, List


THINGER_TOKEN = os.environ.get('THINGER_TOKEN')
THINGER_USERNAME = os.environ.get('THINGER_USERNAME', 'Alfacon')
THINGER_SERVER = os.environ.get('THINGER_SERVER', 'https://alfacon.aws.thinger.io')
WEATHER_API_KEY = os.environ.get('WEATHER_API_KEY')


ATHENS_LAT = 37.9838
ATHENS_LON = 23.7275


DEVICE_PREFIX = "CAL"
DEVICE_START = 251
DEVICE_END = 351

def get_athens_weather() -> Optional[Dict]:
    """Get weather data for Athens, Greece."""
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={ATHENS_LAT}&lon={ATHENS_LON}&appid={WEATHER_API_KEY}&units=metric"
    try:
        response = requests.get(url, timeout=30)
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
        
        print(f"🌤️  Athens weather: {temperature}°C, {humidity}% humidity, {weather_info['description']}")
        return weather_info
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching weather data for Athens: {e}")
        return None

def check_device_exists(device_id: str) -> bool:
    """Check if a device exists by testing OutTemp resource access."""
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
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        if response.status_code == 200:
            print(f"✅ Device {device_id}: Temperature {temperature}°C sent successfully")
            return True
        else:
            if response.status_code == 404:
                print(f"⚠️  Device {device_id}: Device or OutTemp resource not found")
            else:
                print(f"❌ Device {device_id}: Status {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Device {device_id}: {e}")
        return False

def process_device_batch(devices: List[str], temperature: float) -> dict:
    """Process a batch of devices."""
    results = {"success": 0, "failed": 0, "not_found": 0}
    
    for device_id in devices:

        if not check_device_exists(device_id):
            print(f"⚠️  Device {device_id} does not exist in Thinger.io")
            results["not_found"] += 1
            continue
        

        if send_to_thinger_api(device_id, temperature):
            results["success"] += 1
        else:
            results["failed"] += 1
    
    return results

def main():
    """Main function - runs once per GitHub Actions execution."""
    print("=" * 70)
    print(f"🚀 Weather Update Service Started")
    print(f"⏰ Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 70)
    

    if not THINGER_TOKEN:
        print("❌ THINGER_TOKEN environment variable not set")
        return
    if not WEATHER_API_KEY:
        print("❌ WEATHER_API_KEY environment variable not set") 
        return
    
    print(f"🏛️  Target Location: Athens, Greece ({ATHENS_LAT}, {ATHENS_LON})")
    print(f"🔧 Server: {THINGER_SERVER}")
    print(f"👤 Username: {THINGER_USERNAME}")
    

    devices = [f"{DEVICE_PREFIX}{i}" for i in range(DEVICE_START, DEVICE_END + 1)]
    print(f"📡 Total Devices: {len(devices)} ({DEVICE_PREFIX}{DEVICE_START} to {DEVICE_PREFIX}{DEVICE_END})")
    

    print("\n🌤️  Fetching weather data...")
    weather = get_athens_weather()
    
    if not weather:
        print("❌ Failed to get weather data. Aborting this run.")
        return
    
    temperature = weather['temperature']
    print(f"🌡️  Current Temperature: {temperature}°C")
    print(f"💧 Humidity: {weather['humidity']}%")
    print(f"☁️  Conditions: {weather['description']}")
    
    print(f"\n📤 Starting update for {len(devices)} devices...")
    

    batch_size = 10
    total_success = 0
    total_failed = 0
    total_not_found = 0
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        
        for i in range(0, len(devices), batch_size):
            batch = devices[i:i + batch_size]
            print(f"🔄 Processing batch {i//batch_size + 1} ({len(batch)} devices)")
            
            future = executor.submit(process_device_batch, batch, temperature)
            futures.append(future)
        

        for future in futures:
            result = future.result()
            total_success += result["success"]
            total_failed += result["failed"]
            total_not_found += result["not_found"]
    

    print("\n" + "=" * 70)
    print(f"📊 UPDATE SUMMARY")
    print("=" * 70)
    print(f"🌡️  Athens Temperature: {temperature}°C ({weather['description']})")
    print(f"📡 Total Devices: {len(devices)}")
    print(f"✅ Successful Updates: {total_success}")
    print(f"❌ Failed Updates: {total_failed}")
    print(f"⚠️  Devices Not Found: {total_not_found}")
    print(f"📈 Success Rate: {(total_success/len(devices)*100):.1f}%")
    print("=" * 70)
    
    if total_not_found > 0:
        print(f"ℹ️  Note: {total_not_found} devices need to be created in Thinger.io")
    
    print("🎯 GitHub Actions execution completed successfully!")

if __name__ == "__main__":
    main()
