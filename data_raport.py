# Generate log from all databases about: collection size, average price of goods in collection, expencive and cheapest item, 
# heaviest and lightest item and total value of product in  given collection
# extra: calculate average value of collection  by both MongoDB and Pythonic way and compare which is faster

from typing import List, Tuple
from pymongo import MongoClient
import logging
import time


logging.basicConfig(filename='product_database.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

coll1 = {"Db":"Fruits", "coll":"Europe.products"}
coll2 = {"Db":"Fruits", "coll":"USA.products"}
coll3 = {"Db":"Chlothes", "coll":"Winter.products"}
coll4 = {"Db":"Chlothes", "coll":"Summer.products"}

my_dict = [coll1, coll2, coll3, coll4]


class Basic():
    """ Class Basic have methods for printing Collection size and Average price of items in given collection """

    def __init__(self, db_name: str, coll_name: str) -> None:
        client = MongoClient("localhost", 37017)
        db = client[db_name]
        collection = db[coll_name]
        self.collection = collection
        self.coll_name = coll_name
        self.db_name = db_name

    def coll_size(self) -> str:
        result = self.collection.aggregate([{"$match" : {"Price" : {"$gte" : 1 } }},{"$count" : "total"}])
        for i in result:
            return i['total']

    def avg_price_aggregate(self) -> Tuple[float, float]:
        st = time.time()
        result = self.collection.aggregate([{"$match" : {"Price" : {"$gt" : 1 } }},{"$group" : { "_id" : 0 ,"avg": { "$avg" : "$Price"},} }])
        answ = [round((x["avg"]), 2) for x in result][0]
        et = time.time()
        elapsed_time = et - st
        return elapsed_time, answ
    
    def avg_price_pythonic(self) -> Tuple[float, float]:
        st2 = time.time()
        answ2 = round(((sum([x["Price"] for x in self.collection.find()])) / (len(([x["Price"] for x in self.collection.find()])))),2)
        et2 = time.time()
        elapsed_time2 = et2 - st2
        return elapsed_time2, answ2

class Advanced(Basic):
    """ Advanced class have methods for finding cheapest/mosts expensive; lightest/ heaviest items in collection """

    def __init__(self, db_name: str, coll_name: str):
        super().__init__(db_name, coll_name)          

    def cheapest_expensive_value(self) -> tuple:
        result = self.collection.aggregate([{"$match" : {"Price" : {"$gt" : 1 } }},{"$group" : { "_id" : "$name" ,"Price": { "$min" : "$Price"},} }, {"$sort": {"Price": 1}}])
        goods_list = [x for x in result]
        return (goods_list[len(goods_list)-1])['Price'], (goods_list[len(goods_list)-1])['_id'], (goods_list[0])['Price'], (goods_list[0])['_id']

     
    def weight(self) -> tuple:
        result = self.collection.aggregate([{"$match" : {"Price" : {"$gt" : 1 } }},{"$group" : { "_id" : "$name" ,"Weight": { "$min" : "$Weight"},} }, {"$sort": {"Weight": 1}}])
        goods_list = [x for x in result]
        return (goods_list[len(goods_list)-1])['Weight'], (goods_list[len(goods_list)-1])['_id'], (goods_list[0])['Weight'], (goods_list[0])['_id']

    
    def all_goods_price(self) -> float:
        result = self.collection.aggregate([{"$match" : {"Price" : {"$gt" : 1 } }},{"$group" : { "_id" : "$name" ,"Price": { "$min" : "$Price"},} }])
        return  round(sum([x["Price"] for x in result]), 2)


      
def main(coll_name):

    db_name = x["Db"]
    coll_name = x["coll"]

    basic = Basic(db_name, coll_name)
    coll_size = basic.coll_size()
    elapsed_time, avg_price = basic.avg_price_aggregate()
    elapsed_time2, avg_price_2 = basic.avg_price_pythonic()

    adv = Advanced(db_name, coll_name)
    expensive_price, expensive_item, cheapest_price, cheapest_item = adv.cheapest_expensive_value()
    heaviest_weight, heaviest_item, lightest_weight, lightest_item = adv.weight()
    total_value = adv.all_goods_price()
    print(avg_price)
    print(avg_price_2)

    print('Execution time, of mongo db  Aggregation Pipeline:', elapsed_time, 'seconds')
    print('Execution time, of python:', elapsed_time2, 'seconds')

    logging.info(f"In database: \"{db_name}\", Collection name: \"{coll_name}\"\
 We found {coll_size} products.\n Where average price for product was: {avg_price} $.\n\
 The most expensive item was: \"{expensive_item}\", which price is: {expensive_price} $.\n\
 The cheapest product was:  \"{cheapest_item}\", which price was:  {cheapest_price} $.\n\
 The heaviest product was: \"{heaviest_item}\" which weight is: {heaviest_weight} kg.\n\
 The lightest product was:  \"{lightest_item}\" which weigt is: {lightest_weight} kg.\n\
 The total value of \"{coll_name}\" products is {total_value} $\n\n")
        


if __name__ == "__main__":
    for x in my_dict:
        main(x)
