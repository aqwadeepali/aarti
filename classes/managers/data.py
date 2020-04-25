import os
# from xml.etree import ElementTree
from pymongo import MongoClient


class DataManager():
    def __init__(self):
        self.client = MongoClient('127.0.0.1', username="", password="", authSource='analyse_db') #, authMechanism='SCRAM-SHA-1')
        print("Connections ready to use...")
    def get_connection(self):
        return self.client
	
	
	
