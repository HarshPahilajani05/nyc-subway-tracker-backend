import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def init_database():
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    
    cur = conn.cursor()
    
    # Create delays table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS delays (
            id SERIAL PRIMARY KEY,
            line VARCHAR(10) NOT NULL,
            stop_id VARCHAR(50),
            delay_seconds INTEGER,
            delay_minutes DECIMAL(5,1),
            timestamp TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create indexes for faster queries
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_delays_line 
        ON delays(line)
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_delays_timestamp 
        ON delays(timestamp)
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_delays_line_timestamp 
        ON delays(line, timestamp)
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    
    print("✅ Database tables created successfully!")

if __name__ == "__main__":
    init_database()