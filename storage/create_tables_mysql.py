import mysql.connector


db_conn = mysql.connector.connect(host="kafka-service.westus2.cloudapp.azure.com", user="root", password="password", database="events")
db_cursor = db_conn.cursor()


db_cursor.execute('''
          CREATE TABLE food_order
          (id INT NOT NULL AUTO_INCREMENT,
           customer_id VARCHAR(100) NOT NULL,
           name VARCHAR(100) NOT NULL,
           phone VARCHAR(50) NOT NULL,
           order_date VARCHAR(100) NOT NULL,
           CONSTRAINT food_order_pk PRIMARY KEY (id))
          ''')

db_cursor.execute('''
          CREATE TABLE scheduled_order
          (id INT NOT NULL AUTO_INCREMENT,
           customer_id VARCHAR(100) NOT NULL,
           name VARCHAR(100) NOT NULL,
           phone VARCHAR(50) NOT NULL,
           order_date VARCHAR(100) NOT NULL,
           scheduled_date VARCHAR(100) NOT NULL,
           CONSTRAINT scheduled_order_pk PRIMARY KEY (id))
          ''')

db_conn.commit()
db_conn.close()
