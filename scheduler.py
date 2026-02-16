import schedule
import time
import os
import resend
import pg8000.native
from scraper import scrape_all_feeds, scrape_alerts
from datetime import datetime

resend.api_key = os.getenv('RESEND_API_KEY')

def get_db_connection():
    return pg8000.native.Connection(
        host=os.getenv('DB_HOST'),
        port=int(os.getenv('DB_PORT', 5432)),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        ssl_context=True
    )

def send_delay_alerts():
    try:
        conn = get_db_connection()
        
        delayed_lines = conn.run("""
            SELECT DISTINCT line, ROUND(AVG(delay_minutes)::numeric, 1) as avg_delay
            FROM delays
            WHERE timestamp > NOW() - INTERVAL '5 minutes'
            GROUP BY line
        """)
        
        if not delayed_lines:
            conn.close()
            return
        
        for row in delayed_lines:
            line = row[0]
            avg_delay = row[1]
            
            already_sent = conn.run("""
                SELECT COUNT(*) FROM email_alert_log
                WHERE line = :line AND sent_at > NOW() - INTERVAL '1 hour'
            """, line=line)
            
            if already_sent[0][0] > 0:
                continue
            
            subscribers = conn.run(
                "SELECT email FROM email_subscriptions WHERE line = :line",
                line=line
            )
            
            if not subscribers:
                continue
            
            for sub in subscribers:
                email = sub[0]
                try:
                    resend.Emails.send({
                        "from": "onboarding@resend.dev",
                        "to": email,
                        "subject": f"🚇 Line {line} Delay Alert - {avg_delay} min avg delay",
                        "html": f"""
                        <div style="font-family: sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
                            <h2 style="color: #333;">🚇 NYC Subway Delay Alert</h2>
                            <div style="background: #f5f5f5; padding: 20px; border-radius: 8px; margin: 20px 0;">
                                <h3 style="margin: 0 0 10px 0;">Line {line} is experiencing delays</h3>
                                <p style="margin: 0; font-size: 18px; color: #e53e3e;">
                                    Average delay: <strong>{avg_delay} minutes</strong>
                                </p>
                            </div>
                            <p>Check the live dashboard for real-time updates:</p>
                            <a href="https://nyc-subway-tracker-frontend.vercel.app" 
                               style="background: #3b82f6; color: white; padding: 12px 24px; border-radius: 6px; text-decoration: none; display: inline-block;">
                                View Dashboard
                            </a>
                            <p style="margin-top: 20px; font-size: 12px; color: #999;">
                                You're receiving this because you subscribed to Line {line} alerts.<br>
                                <a href="https://nyc-subway-tracker-frontend.vercel.app" style="color: #999;">Unsubscribe</a>
                            </p>
                        </div>
                        """
                    })
                    print(f"📧 Sent delay alert to {email} for Line {line}")
                except Exception as e:
                    print(f"Failed to send email to {email}: {e}")
            
            conn.run(
                "INSERT INTO email_alert_log (email, line) VALUES ('batch', :line)",
                line=line
            )
        
        conn.close()
        
    except Exception as e:
        print(f"Error sending alerts: {e}")

def run_scraper_and_alerts():
    scrape_all_feeds()
    scrape_alerts()
    send_delay_alerts()

print("🚇 NYC Subway Tracker - Auto Scheduler Started")
print("Running scraper every 60 seconds...")
print("Press CTRL+C to stop\n")

run_scraper_and_alerts()

schedule.every(60).seconds.do(run_scraper_and_alerts)

while True:
    schedule.run_pending()
    time.sleep(1)