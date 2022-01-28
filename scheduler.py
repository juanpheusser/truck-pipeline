from run import main
import schedule
import time


schedule.every(1).minutes.do(main)

while True:
    schedule.run_pending()
    time.sleep(1)