import schedule
import time
from scraper import scrape_all_feeds
from datetime import datetime

print("🚇 NYC Subway Tracker - Auto Scheduler Started")
print("Running scraper every 60 seconds...")
print("Press CTRL+C to stop\n")

# Run immediately on start
scrape_all_feeds()

# Then run every 60 seconds
schedule.every(60).seconds.do(scrape_all_feeds)

while True:
    schedule.run_pending()
    time.sleep(1)