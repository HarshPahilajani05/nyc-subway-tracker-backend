import pg8000.native
from nyct_gtfs import NYCTFeed
from dotenv import load_dotenv
import os
import requests
from datetime import datetime
import struct

load_dotenv()

LINES = ['1', '2', '3', '4', '5', '6', '7', 
         'A', 'C', 'E', 'B', 'D', 'F', 'M', 
         'G', 'J', 'Z', 'L', 'N', 'Q', 'R', 'W', 'S']

ALERT_FEED_URL = 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fsubway-alerts.json'

def get_db_connection():
    return pg8000.native.Connection(
        host=os.getenv('DB_HOST'),
        port=int(os.getenv('DB_PORT', 5432)),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        ssl_context=True
    )

def scrape_all_feeds():
    print(f"\n🚇 Scraping MTA feeds at {datetime.now().strftime('%H:%M:%S')}...")
    
    all_delays = []
    
    for line in LINES:
        try:
            feed = NYCTFeed(line)
            trains = feed.trips
            delayed = 0
            
            for train in trains:
                for stop in train.stop_time_updates:
                    delay_seconds = getattr(stop, 'delay', 0) or 0
                    if delay_seconds > 120:
                        all_delays.append({
                            'line': line,
                            'stop_id': stop.stop_id,
                            'delay_seconds': delay_seconds,
                            'delay_minutes': round(delay_seconds / 60, 1),
                            'timestamp': datetime.now()
                        })
                        delayed += 1
            
            print(f"  Line {line}: {len(trains)} trains, {delayed} delays")
        
        except Exception as e:
            print(f"  Line {line}: Error - {e}")
    
    if all_delays:
        conn = get_db_connection()
        for delay in all_delays:
            conn.run(
                "INSERT INTO delays (line, stop_id, delay_seconds, delay_minutes, timestamp) VALUES (:line, :stop_id, :delay_seconds, :delay_minutes, :timestamp)",
                line=delay['line'],
                stop_id=delay['stop_id'],
                delay_seconds=delay['delay_seconds'],
                delay_minutes=delay['delay_minutes'],
                timestamp=delay['timestamp']
            )
        conn.close()
        print(f"✅ Saved {len(all_delays)} delays to database")
    else:
        print("ℹ️ No delays recorded this run")
    
    print(f"✅ Done!\n")

def scrape_alerts():
    print(f"\n🚨 Scraping MTA service alerts at {datetime.now().strftime('%H:%M:%S')}...")

    try:
        response = requests.get(ALERT_FEED_URL)
        response.raise_for_status()
        
        data = response.json()
        
        conn = get_db_connection()
        conn.run("DELETE FROM alerts")

        alerts_saved = 0
        
        if 'entity' not in data:
            print("  No entities in feed")
            conn.close()
            return

        for entity in data.get('entity', []):
            if 'alert' not in entity:
                continue
                
            alert = entity['alert']
            
            # Get affected lines
            affected_lines = set()
            for informed in alert.get('informed_entity', []):
                route_id = informed.get('route_id', '').strip().upper()
                if route_id in LINES:
                    affected_lines.add(route_id)
            
            if not affected_lines:
                continue
            
            # Get header and description
            header = ''
            description = ''
            
            if 'header_text' in alert and 'translation' in alert['header_text']:
                translations = alert['header_text']['translation']
                if translations:
                    header = translations[0].get('text', '')
            
            if 'description_text' in alert and 'translation' in alert['description_text']:
                translations = alert['description_text']['translation']
                if translations:
                    description = translations[0].get('text', '')
            
            # Determine alert type
            header_lower = header.lower()
            if 'delay' in header_lower:
                alert_type = 'delay'
            elif 'suspended' in header_lower:
                alert_type = 'suspended'
            elif 'skipped' in header_lower:
                alert_type = 'stops_skipped'
            elif 'express' in header_lower or 'local' in header_lower:
                alert_type = 'express_to_local'
            elif 'reduced' in header_lower:
                alert_type = 'reduced_service'
            elif 'planned' in header_lower:
                alert_type = 'planned_work'
            else:
                alert_type = 'service_change'
            
            for line in affected_lines:
                conn.run(
                    """INSERT INTO alerts (line, alert_type, header, description, created_at)
                       VALUES (:line, :alert_type, :header, :description, :created_at)""",
                    line=line,
                    alert_type=alert_type,
                    header=header,
                    description=description,
                    created_at=datetime.now()
                )
                alerts_saved += 1
                print(f"  Alert for Line {line}: [{alert_type}] {header[:60]}...")

        conn.close()
        print(f"✅ Saved {alerts_saved} alerts to database\n")

    except Exception as e:
        print(f"❌ Failed to scrape alerts: {e}\n")

if __name__ == "__main__":
    scrape_all_feeds()
    scrape_alerts()