from os import listdir
import os
import gzip
import json


def read_data_from_unzip_file(folder_path):
    # read data from unzip grab food pages.
    all_pages_data_list = []
    for file_name in listdir(folder_path):
        ## join one by one file name
        file_path = os.path.join(folder_path,file_name)
        with gzip.open(file_path, "rt", encoding='utf-8') as f:
            ## get one page data and add in list.
            single_page_data = json.load(f)
        all_pages_data_list.append(single_page_data)
    return all_pages_data_list

def extract_grab_food_data(all_pages_data_list):
    ## extract from list of data of grab food pages.
    restaurant_list =[]
    for dict_data in all_pages_data_list:
        Rest_Data = {}
        grab_food_dict = {}
        if not dict_data.get("merchant") :
            continue
        grab_food_dict["restaurant_id"] = dict_data.get("merchant").get("ID")
        grab_food_dict["restaurant_name"] = dict_data.get("merchant").get("name")
        grab_food_dict["cuisine"] = dict_data.get("merchant").get("cuisine")
        grab_food_dict["rating"] = dict_data.get("merchant").get("rating")
        grab_food_dict["restaurant_image"] = dict_data.get("merchant").get("photoHref")
        opening_time_dict = dict_data.get("merchant").get("openingHours")
        grab_food_dict["opening_time"] = {
            "sunday" : opening_time_dict.get("sun"),
            "monday" : opening_time_dict.get("mon"),
            "tuesday" : opening_time_dict.get("tue"),
            "wednesday" : opening_time_dict.get("wed"),
            "thursday" : opening_time_dict.get("thu"),
            "friday" : opening_time_dict.get("fri"),
            "saturday" : opening_time_dict.get("sat")
        }
        grab_food_dict["distance"] = str (dict_data.get("merchant").get("distanceInKm") ) + " " + "Km"
        grab_food_dict["cuisine"] = dict_data.get("merchant").get("cuisine")

        menu_list = dict_data.get("merchant").get("menu").get("categories", [])
        products_list = []
        if not menu_list:
            Rest_Data["restaurant_detail"] = grab_food_dict
            restaurant_list.append(Rest_Data)
            continue
        for data in menu_list:
            item_name = data["name"]
            for food_dict in data.get("items", []):
                if food_dict.get("priceInMinorUnit"):
                    price_amount = (food_dict.get("priceInMinorUnit")) / 100
                    # print("not price value ")
                else:
                    price_amount = 0
                item_dict = {
                    "restaurant_id" : dict_data.get("merchant").get("ID"),
                    "category_name": item_name,
                    "food_id" : food_dict.get("ID"),
                    "food_name" : food_dict.get("name"),
                    "price" : price_amount,
                    "image_url" : food_dict.get("imgHref"),
                    "description" : food_dict.get("description")
                }
                products_list.append(item_dict)

        Rest_Data["restaurant_detail"] = grab_food_dict
        Rest_Data["Menu_Items"] = products_list
        restaurant_list.append(Rest_Data)
    return restaurant_list

