#       Create 2 databases, name fruits and clothes (create at least 2 collections and populate with 25-50 
# (random number in that range) items in each collection).
# Item descriptors are {​name, weight, cost, size, color}​ if appliacable. (Price, weight, size range is 9.99 < price < 99.99)
# Get me a report of each collection:

from random_word import RandomWords
import random
from time import sleep
from typing import List
from pymongo import MongoClient


name = input(f"enter name of DB : ")
collection = input(f"enter name of collection : ")

r = RandomWords()
collection_lenght = random.randint(25,50)
print(collection_lenght)

client = MongoClient("localhost", 37017)
db = client[name]
coll = db[collection]


for x in range(collection_lenght):
    product_name = r.get_random_word()
    product_price = round(random.uniform(9.99, 99.99),2)
    product_weight = round(random.uniform(9.99, 99.99),2)
    product_size = round(random.uniform(9.99, 99.99),2)
    coll.products.insert_one({"name": product_name, "Price": product_price, "Weight": product_weight, "Size": product_size })
    sleep(0.05)
 

print("progress: Done!")
