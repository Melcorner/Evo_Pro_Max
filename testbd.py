import sqlite3
conn = sqlite3.connect('data/app.db')
cur = conn.cursor()
cur.execute("SELECT status, count(*) FROM event_store GROUP BY status")
for row in cur.fetchall():
    print(row)
conn.close()