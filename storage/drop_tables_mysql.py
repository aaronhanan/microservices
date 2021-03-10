import mysql.connector


db_conn = mysql.connector.connect(host="localhost", user="root", password="Anantharajah123!", database="orders")
db_cursor = db_conn.cursor()


db_cursor.execute('''DROP TABLE food_order, scheduled_order''')


db_conn.commit()
db_conn.close()
