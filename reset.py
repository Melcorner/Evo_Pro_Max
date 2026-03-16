import sqlite3
conn = sqlite3.connect('data/app.db')
conn.execute("DELETE FROM event_store WHERE status='FAILED'")
conn.execute("DELETE FROM errors")
conn.commit()
conn.close()
print('Done')