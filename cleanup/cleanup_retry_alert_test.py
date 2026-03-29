import sqlite3

conn = sqlite3.connect("data/app.db")
cur = conn.cursor()

cur.execute("DELETE FROM event_store WHERE id = ?", ("evt-retry-alert-test-1",))

conn.commit()
conn.close()

print("RETRY alert test event removed")