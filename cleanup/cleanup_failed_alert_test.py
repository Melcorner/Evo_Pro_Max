import sqlite3

conn = sqlite3.connect("data/app.db")
cur = conn.cursor()

cur.execute("DELETE FROM event_store WHERE id = ?", ("evt-failed-alert-test-1",))

conn.commit()
conn.close()

print("FAILED alert test event removed")