import sqlite3
conn = sqlite3.connect('data/app.db')
cur = conn.cursor()
cur.execute("UPDATE event_store SET status='FAILED' WHERE status='PROCESSING'")
print(f"Fixed: {cur.rowcount}")
conn.commit()
conn.close()