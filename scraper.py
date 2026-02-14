import psycopg2
from nyct_gtfs import NYCTFeed
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

# All subway lines to track
LINES = ['1', 'A', 'B', 'G', 'J', 'N', 'L', '7', 'S']

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
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
                    delay_seconds = 0
                    if hasattr(stop, 'arrival') and stop.arrival:
                        try:
                            scheduled = stop.arrival
                            # Count trains that have delays
                            delay_seconds = getattr(stop, 'delay', 0) or 0
                        except:
                            pass
                    
                    if delay_seconds > 60:
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
    
    # Save to database
    if all_delays:
        conn = get_db_connection()
        cur = conn.cursor()
        
        for delay in all_delays:
            cur.execute("""
                INSERT INTO delays (line, stop_id, delay_seconds, delay_minutes, timestamp)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                delay['line'],
                delay['stop_id'],
                delay['delay_seconds'],
                delay['delay_minutes'],
                delay['timestamp']
            ))
        
        conn.commit()
        cur.close()
        conn.close()
        print(f"✅ Saved {len(all_delays)} delays to database")
    else:
        print("ℹ️ No delays recorded this run")
    
    print(f"✅ Done!\n")

if __name__ == "__main__":
    scrape_all_feeds()