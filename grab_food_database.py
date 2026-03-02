
from extract_data_from_zip_file import read_data_from_unzip_file, extract_grab_food_data
from food_data_validation import RestaurantDetail, FoodItem

from pydantic import ValidationError, TypeAdapter
from typing import List, Tuple
import json

import time
import mysql.connector # Must include .connector



start = time.time()

DB_CONFIG = {
    "host" : "localhost",
    "user" : "root",
    "password" : "actowiz",
    "port" : "3306",
    "database" : "restaurant_db"
}

def get_connection():
    ## here ** is unpacking DB_CONFIG dictionary.
    connection = mysql.connector.connect(**DB_CONFIG)
    ## it is protect to autocommit
    connection.autocommit = False
    return connection

batch_size_length = 500
def data_commit_batches_wise(connection, cursor, sql_query : str, sql_query_value: List[Tuple], batch_size: int = batch_size_length ):
    ## this is save data in database batches wise.
    batch_count = 0
    for index in range(0, len(sql_query_value), batch_size):
        batch = sql_query_value[index: index + batch_size]
        cursor.executemany(sql_query, batch)
        batch_count += 1
        connection.commit()
    return batch_count

folder_path = "C:/Users/vishal.kushvanshi/PycharmProjects/grab_food_pages/grab_food_pages/grab_food_pages/"
## get all grab food pages data
all_pages_data_list = read_data_from_unzip_file(folder_path)
## get extracted data
restaurant_detail_list = extract_grab_food_data(all_pages_data_list)


## apply data validation using pydantic
try:
    ## TypeAdapter and adapter.validate_python(restaurant_detail_list) use for list inside dictionary data pass
    adapter  = TypeAdapter(List[RestaurantDetail])
    validated_data = adapter.validate_python(restaurant_detail_list)
    all_validate_data_list = [obj.model_dump() for obj in validated_data]
    print("all validate data : ", type(all_validate_data_list) )
    ## below code execute when data validation is true
    try:
        print("Connecting...")
        # Connection logic here
        connection = get_connection()
        cursor = connection.cursor()

        # show all databases
        #  databases create .
        # cursor.execute("create database restaurant_db")
        # cursor.execute("SHOW DATABASES")
        # for db in cursor:
        #     print(db[0])

        # create new table  -- if table not exist then
        cursor.execute("CREATE TABLE IF NOT EXISTS restaurant_detail ( id INT AUTO_INCREMENT PRIMARY KEY, restaurant_id VARCHAR(100), restaurant_name VARCHAR(100), cuisine VARCHAR(100), rating DECIMAL(10,2), restaurant_image TEXT, distance VARCHAR(90), opening_time JSON ) ")
        cursor.execute("CREATE TABLE IF NOT EXISTS menu_detail ( id INT AUTO_INCREMENT PRIMARY KEY, restaurant_detail_id INT, categories VARCHAR(100), food_id VARCHAR(100) UNIQUE, food_name VARCHAR(100),  price DECIMAL(10,2), image_url TEXT, description VARCHAR(700) , FOREIGN KEY (restaurant_detail_id) REFERENCES restaurant_detail(id) ) ")

        # insert data in table
        parent_sql = """INSERT INTO restaurant_detail
                            (restaurant_id, restaurant_name, cuisine, rating, restaurant_image, distance, opening_time)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)"""

        child_sql = """INSERT INTO menu_detail
                               (restaurant_detail_id, categories, food_id, food_name, price, image_url, description )
                               VALUES (%s, %s, %s, %s, %s, %s, %s )
                               ON DUPLICATE KEY UPDATE 
                               food_name = VALUES(food_name),
                               price = VALUES(price),
                               image_url = VALUES(image_url),
                               description = VALUES(description);"""

        try:
            # parent table data insert
            parent_values = []
            for data in restaurant_detail_list:
                parent_values.append((
                    data["restaurant_id"],
                    data["restaurant_name"],
                    data["cuisine"],
                    data["rating"],
                    data["restaurant_image"],
                    data["distance"],
                    json.dumps(data["opening_time"])  # Dictionary -> JSON String
                ))
            ## call data_commit_batches_wise function for batches wise data save in database.
            batch_count = data_commit_batches_wise(connection, cursor, parent_sql, parent_values )
            print(f"Successfully inserted {len(parent_values)} restaurant detail in batches.")
            print(f"Total number of batches {batch_count} of restaurants.")
            ## this select query get data from cursor object (all query operation data store inside cursor object  )
            cursor.execute("SELECT id, restaurant_id FROM restaurant_detail")
            ## get restaurant primary id and restaurant_id from cursor and insert key value wise in dictionary
            id_map = {res_id: db_id for (db_id, res_id) in cursor.fetchall()}
            # child table data insert
            all_child_values = []
            for data in restaurant_detail_list:
                # Get the actual DB primary key using the JSON restaurant_id
                db_parent_id = id_map.get(data["restaurant_id"])
                menu_data = data.get("menu_detail", {})
                # if not menu_data:
                #     dict_count += 1
                for category_name, items_list in menu_data.items():
                    for item in items_list:
                        all_child_values.append((
                            db_parent_id,
                            category_name,
                            item["food_id"],
                            item["food_name"],
                            item["price"],
                            item["image_url"],
                            item["description"]
                        ))
            ## call data_commit_batches_wise function for batches wise data save in database.
            batch_count = data_commit_batches_wise(connection, cursor, child_sql, all_child_values )
            print(f"Successfully inserted {len(all_child_values)} menu detail in batches.")
            print(f"Total number of batches {batch_count} of menu.")
        except Exception as e:
            ## this exception execute when error occur in try block and rollback until last save on database .
            connection.rollback()
            print(f"Transaction failed, rolled back. Error: {e}")
        finally:
            cursor.close()
        # connection.commit()
        print("process done ")

    except Exception as e:
        print(f"Error: {e}")


except ValidationError as e:
    print("--- Error Summary ---")
    for error in e.errors():
        field_name = " -> ".join( str(x) for x in error["loc"] )
        message = error['msg']
        print(f"Error in Field: [{field_name}] | Reason: {message}")




end = time.time()
print("Time different : ", end-start)
print("yes complete ...")