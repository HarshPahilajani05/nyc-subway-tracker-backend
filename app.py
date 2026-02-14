from flask import Flask, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
import pg8000.native
import os

load_dotenv()

app = Flask(__name__)
CORS(app)

def get_db_connection():
    return pg8000.native.Connection(
        host=os.getenv('DB_HOST'),
        port=int(os.getenv('DB_PORT', 5432)),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        ssl_context=True
    )

@app.route('/api/health')
def health():
    return jsonify({'status': 'ok'})

@app.route('/api/lines')
def get_lines():
    conn = get_db_connection()
    rows = conn.run("""
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
    conn.close()
    result = []
    for row in rows:
        result.append({
            'line': row[0],
            'total_delays': row[1],
            'avg_delay': str(row[2]),
            'max_delay': str(row[3]),
            'last_updated': str(row[4])
        })
    return jsonify(result)

@app.route('/api/stats')
def get_stats():
    conn = get_db_connection()
    rows = conn.run("""
        SELECT 
            COUNT(*) as total_delays_recorded,
            COUNT(DISTINCT line) as lines_tracked,
            ROUND(AVG(delay_minutes)::numeric, 1) as overall_avg_delay,
            MAX(timestamp) as last_scrape
        FROM delays
    """)
    conn.close()
    row = rows[0]
    return jsonify({
        'total_delays_recorded': row[0],
        'lines_tracked': row[1],
        'overall_avg_delay': str(row[2]),
        'last_scrape': str(row[3])
    })

@app.route('/api/worst-times')
def get_worst_times():
    conn = get_db_connection()
    rows = conn.run("""
        SELECT 
            EXTRACT(HOUR FROM timestamp) as hour_of_day,
            COUNT(*) as delay_count,
            ROUND(AVG(delay_minutes)::numeric, 1) as avg_delay
        FROM delays
        GROUP BY EXTRACT(HOUR FROM timestamp)
        ORDER BY hour_of_day ASC
    """)
    conn.close()
    result = []
    for row in rows:
        result.append({
            'hour_of_day': row[0],
            'delay_count': row[1],
            'avg_delay': str(row[2])
        })
    return jsonify(result)

@app.route('/api/lines/<line>/history')
def get_line_history(line):
    conn = get_db_connection()
    rows = conn.run("""
        SELECT 
            DATE_TRUNC('hour', timestamp) as hour,
            COUNT(*) as delay_count,
            ROUND(AVG(delay_minutes)::numeric, 1) as avg_delay
        FROM delays
        WHERE line = :line
        AND timestamp > NOW() - INTERVAL '7 days'
        GROUP BY DATE_TRUNC('hour', timestamp)
        ORDER BY hour ASC
    """, line=line.upper())
    conn.close()
    result = []
    for row in rows:
        result.append({
            'hour': str(row[0]),
            'delay_count': row[1],
            'avg_delay': str(row[2])
        })
    return jsonify(result)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)