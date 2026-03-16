import sqlite3
 
conn = sqlite3.connect('data/app.db')
cursor = conn.cursor()
 
cursor.execute("UPDATE event_store SET status='FAILED' WHERE status='NEW' OR status='RETRY'")
affected = cursor.rowcount
conn.commit()
conn.close()
 
print(f"Done — {affected} events marked as FAILED")
 