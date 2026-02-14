from flask import Flask, jsonify
from flask_cors import CORS
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)
CORS(app)

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

# ---- ROUTES ----

@app.route('/api/health')
def health():
    return jsonify({'status': 'ok'})

@app.route('/api/lines')
def get_lines():
    """Get delay summary for all lines"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    cur.execute("""
        SELECT 
            line,
            COUNT(*) as total_delays,
            ROUND(AVG(delay_minutes)::numeric, 1) as avg_delay,
            MAX(delay_minutes) as max_delay,
            MAX(timestamp) as last_updated
        FROM delays
        WHERE timestamp > NOW() - INTERVAL '24 hours'
        GROUP BY line
        ORDER BY total_delays DESC
    """)
    
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return jsonify([dict(row) for row in rows])

@app.route('/api/lines/<line>/history')
def get_line_history(line):
    """Get delay history for a specific line (last 7 days)"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    cur.execute("""
        SELECT 
            DATE_TRUNC('hour', timestamp) as hour,
            COUNT(*) as delay_count,
            ROUND(AVG(delay_minutes)::numeric, 1) as avg_delay
        FROM delays
        WHERE line = %s
        AND timestamp > NOW() - INTERVAL '7 days'
        GROUP BY DATE_TRUNC('hour', timestamp)
        ORDER BY hour ASC
    """, (line.upper(),))
    
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return jsonify([dict(row) for row in rows])

@app.route('/api/stats')
def get_stats():
    """Get overall stats"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    cur.execute("""
        SELECT 
            COUNT(*) as total_delays_recorded,
            COUNT(DISTINCT line) as lines_tracked,
            ROUND(AVG(delay_minutes)::numeric, 1) as overall_avg_delay,
            MAX(timestamp) as last_scrape
        FROM delays
    """)
    
    row = cur.fetchone()
    cur.close()
    conn.close()
    
    return jsonify(dict(row))

@app.route('/api/worst-times')
def get_worst_times():
    """Get worst delay times by hour of day"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    cur.execute("""
        SELECT 
            EXTRACT(HOUR FROM timestamp) as hour_of_day,
            COUNT(*) as delay_count,
            ROUND(AVG(delay_minutes)::numeric, 1) as avg_delay
        FROM delays
        GROUP BY EXTRACT(HOUR FROM timestamp)
        ORDER BY hour_of_day ASC
    """)
    
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return jsonify([dict(row) for row in rows])

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)