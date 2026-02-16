import pg8000.native
from nyct_gtfs import NYCTFeed
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

LINES = ['1', '2', '3', '4', '5', '6', '7', 
         'A', 'C', 'E', 'B', 'D', 'F', 'M', 
         'G', 'J', 'Z', 'L', 'N', 'Q', 'R', 'W', 'S']

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

if __name__ == "__main__":
    scrape_all_feeds()