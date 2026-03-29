import sqlite3

conn = sqlite3.connect("data/app.db")
cur = conn.cursor()

cur.execute("DELETE FROM errors WHERE tenant_id = ?", ("test-tenant",))
cur.execute("DELETE FROM event_store WHERE tenant_id = ?", ("test-tenant",))

conn.commit()
conn.close()

print("Test dashboard data removed")