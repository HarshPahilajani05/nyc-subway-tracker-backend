import psycopg2
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import random

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    database=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)

cur = conn.cursor()

lines = ['1', '2', '3', 'A', 'C', 'E', 'B', 'D', 'F', 'M', 'G', 'J', 'Z', 'N', 'Q', 'R', 'W', 'L', '4', '5', '6', '7']

print("Seeding test data...")

# Insert 7 days of historical data
for days_ago in range(0, 1):
    for hour in range(24):
        # Rush hours have more delays
        if hour in [7, 8, 9, 17, 18, 19]:
            num_delays = random.randint(5, 20)
        elif hour in [0, 1, 2, 3, 4]:
            num_delays = random.randint(0, 3)
        else:
            num_delays = random.randint(1, 8)
        
        for _ in range(num_delays):
            line = random.choice(lines)
            delay_seconds = random.randint(90, 600)
            delay_minutes = round(delay_seconds / 60, 1)
            timestamp = datetime.now() - timedelta(hours=hour)
            
            cur.execute("""
                INSERT INTO delays (line, stop_id, delay_seconds, delay_minutes, timestamp)
                VALUES (%s, %s, %s, %s, %s)
            """, (line, f"STOP_{random.randint(100,999)}", delay_seconds, delay_minutes, timestamp))

conn.commit()
cur.close()
conn.close()

print("✅ Test data inserted successfully!")
print("Now run: python check_db.py to verify")