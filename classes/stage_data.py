
import sys, os
import csv
import time
from pymongo import MongoClient
from datetime import datetime

import logging
import logging.config
from logging import getLogger, INFO, ERROR, WARNING, CRITICAL, NOTSET, DEBUG
from concurrent_log_handler import ConcurrentRotatingFileHandler

from settings import Settings_Stage

class RunStage():
	def __init__(self, dateparam):
		print(dateparam)
		self.log = logging.getLogger()
		self.settings = Settings_Stage("stage_config.ini")

		self.collection = self.settings.config["collection"]
		self.material_type = self.settings.config["materials"]
		self.furnaces = self.settings.config["furnaces"]
		self._date = dateparam #datetime.strftime(datetime.now(), "%Y-%m-%d")
		self.connection = self.getConnection()
		self.log.info("In Stage Init: "+ dateparam)
		self.read_database(dateparam)


	def getConnection(self):
		connection = MongoClient('127.0.0.1', username="", password="", authSource='analyse_db')#, authMechanism='SCRAM-SHA-1')
		myDb = connection["analyse_db"]
		
		return myDb


	def read_database(self, dateparam):
		myDb = self.getConnection()

		db = myDb[self.collection]

		ls_date = dateparam+" 23:59:59"
		gt_date = dateparam+" 00:00:01"

		lt_tm = time.mktime(datetime.strptime(ls_date,'%Y-%m-%d %H:%M:%S').timetuple())
		gt_tm = time.mktime(datetime.strptime(gt_date,'%Y-%m-%d %H:%M:%S').timetuple())
		to_find = {"$gte": gt_tm, "$lte": lt_tm }
		dbresult = db.find({"Archive-opc-CUTTACK_TIMESTAMP_EP":to_find})
		# dbresult = db.find()

		uniqueNumlist = []
		columns = []
		headers = []
		all_rows = []
		dataDict = {}
		index = 1
		for row in dbresult:
			columns = row.keys()
			new_row = {}
			batchid = ""
			tstamp = datetime.strftime(row["Archive-opc-CUTTACK_TIMESTAMP"], "%Y-%m-%d")
			for col in columns:
				collist = col.split("/")
				if col not in ["_id", "Archive-opc-CUTTACK_TIMESTAMP", "Archive-opc-CUTTACK_TIMESTAMP_EP"]:
					if "BATCH_ID:Value" == collist[5]:
						batchid = row[col]
						# if batchid in [142, "142", "142.0", 142.0]:
						if batchid not in dataDict.keys():
							dataDict.setdefault(batchid, {})
							new_row.setdefault("Batch", batchid)
						else:
							new_row.setdefault("Batch", batchid)

						if tstamp not in dataDict[batchid].keys():
							dataDict[batchid].setdefault(tstamp, [])

			# if batchid in [142, "142", "142.0", 142.0]:
			for col in columns:
				if col not in ["_id", "Archive-opc-CUTTACK_TIMESTAMP", "Archive-opc-CUTTACK_TIMESTAMP_EP"]:
					collist = col.split("/")
					# print(collist[4],'---',collist[5],'---', collist[5].split())
					if "MATERIAL" in collist[5]:
						num = int(''.join(list(filter(str.isdigit, collist[5]))))
						uniqueNumlist.append(num)
						val = str(int(row[col]))
						material_name = self.material_type.get(val, "--")
						new_row.setdefault("Material_Type_" + str(num), material_name)
					if "FURNACE" in collist[5]:
						num = int(row[col])
						uniqueNumlist.append(num)
						furnace_name = self.furnaces.get(str(num), "--")
						new_row.setdefault("Furnace", furnace_name)
					if "BEAN" == collist[4]:
						num = int(''.join(list(filter(str.isdigit, collist[5]))))
						uniqueNumlist.append(num)
						val = row[col]
						new_row.setdefault("Set_Qty_" + str(num), val)
					if "BEAN_ACT_QTY" == collist[4]:
						num = int(''.join(list(filter(str.isdigit, collist[5]))))
						uniqueNumlist.append(num)
						val = row[col]
						new_row.setdefault("Actual_Qty_" + str(num), val)
				else:
					if col != "_id":
						new_row.setdefault(col, row[col])
			dataDict[batchid][tstamp].append(new_row)
			index += 1

		uniqueNumlist = list(set(uniqueNumlist))

		batch_process = {}
		uniquetimestamp = []
		# print(dataDict.keys())
		index = 0
		for batch in dataDict:
			# print (batch,'---', dataDict[batch].keys())
			for tstamp in dataDict[batch]:
				batchlist = dataDict[batch][tstamp]
				batchrow = {}

				for row in batchlist:
					for val in uniqueNumlist:
						if val not in [0, "0"]:
							if row["Actual_Qty_"+str(val)] not in [0, "0"]:
								if "Actual_Qty_"+str(val) not in batchrow.keys():
									batchrow.setdefault("Actual_Qty_"+str(val), row["Actual_Qty_"+str(val)])
									batchrow.setdefault("Set_Qty_"+str(val), row["Set_Qty_"+str(val)])
									batchrow.setdefault("Material_Type_"+str(val), row["Material_Type_"+str(val)])
									batchrow.setdefault("Batch", batch)
									batchrow.setdefault("Archive-opc-CUTTACK_TIMESTAMP_"+str(val), row["Archive-opc-CUTTACK_TIMESTAMP"])
									batchrow.setdefault("Archive-opc-CUTTACK_TIMESTAMP_EP_"+str(val), row["Archive-opc-CUTTACK_TIMESTAMP_EP"])
									batchrow.setdefault("Furnace", row["Furnace"])
									# uniquetimestamp.append((row["Archive-opc-CUTTACK_TIMESTAMP"], row["Archive-opc-CUTTACK_TIMESTAMP_EP"]))

								else:
									batchrow["Actual_Qty_"+str(val)] = row["Actual_Qty_"+str(val)]
									batchrow["Set_Qty_"+str(val)] = row["Set_Qty_"+str(val)]
									batchrow["Material_Type_"+str(val)] = row["Material_Type_"+str(val)]
									batchrow["Archive-opc-CUTTACK_TIMESTAMP_"+str(val)] = row["Archive-opc-CUTTACK_TIMESTAMP"]
									batchrow["Archive-opc-CUTTACK_TIMESTAMP_EP_"+str(val)] = row["Archive-opc-CUTTACK_TIMESTAMP_EP"]
									batchrow["Furnace"] = row["Furnace"]
									batchrow["Batch"] = batch
									# uniquetimestamp.append((row["Archive-opc-CUTTACK_TIMESTAMP"], row["Archive-opc-CUTTACK_TIMESTAMP_EP"]))

				if batchrow != {}:
					# print(batchrow)
					# print("---------------------------------------------------------------------------------------------")
					all_rows.append(batchrow)

		newDict = {}
		# uniquetimestamp = list(set(uniquetimestamp))

		for row in all_rows:
			# if row["Batch"] in ["141", 141]:
			if row["Batch"] not in newDict.keys():
				newDict.setdefault(row["Batch"], {})
			for key in row:
				for val in uniqueNumlist:
					if "Archive-opc-CUTTACK_TIMESTAMP_"+str(val) in row:
						# print("Archive-opc-CUTTACK_TIMESTAMP_"+str(val), "------", row["Archive-opc-CUTTACK_TIMESTAMP_"+str(val)])
						if row["Archive-opc-CUTTACK_TIMESTAMP_"+str(val)] not in newDict[row["Batch"]]:
							# print("Key Created")
							newDict[row["Batch"]].setdefault(row["Archive-opc-CUTTACK_TIMESTAMP_"+str(val)], {})

						newDict[row["Batch"]][row["Archive-opc-CUTTACK_TIMESTAMP_"+str(val)]].setdefault("Actual_Qty_"+str(val), round(row.get("Actual_Qty_"+str(val),0.0),2))
						newDict[row["Batch"]][row["Archive-opc-CUTTACK_TIMESTAMP_"+str(val)]].setdefault("Set_Qty_"+str(val), round(row.get("Set_Qty_"+str(val), 0),2))
						newDict[row["Batch"]][row["Archive-opc-CUTTACK_TIMESTAMP_"+str(val)]].setdefault("Material_Type_"+str(val), row.get("Material_Type_"+str(val), 0))
						newDict[row["Batch"]][row["Archive-opc-CUTTACK_TIMESTAMP_"+str(val)]].setdefault("Furnace", row.get("Furnace", ""))
						newDict[row["Batch"]][row["Archive-opc-CUTTACK_TIMESTAMP_"+str(val)]].setdefault("Archive-opc-CUTTACK_TIMESTAMP_EP", row.get("Archive-opc-CUTTACK_TIMESTAMP_EP_"+str(val), ""))
						newDict[row["Batch"]][row["Archive-opc-CUTTACK_TIMESTAMP_"+str(val)]].setdefault("Archive-opc-CUTTACK_TIMESTAMP", row.get("Archive-opc-CUTTACK_TIMESTAMP_"+str(val), ""))

		rowsToPrint = []
		index = 0
		for batch in newDict:
			# if batch in ["141", 141]:
				# print(newDict[batch].keys())
			for tstamp in sorted(newDict[batch].keys()):
				row = {}
				row.setdefault("Batch", batch)
				row.setdefault("Archive-opc-CUTTACK_TIMESTAMP", tstamp)
				row.setdefault("Furnace", newDict[batch][tstamp]["Furnace"])
				row.setdefault("as_of_date", self._date)
				
				for key in newDict[batch][tstamp]:
					row.setdefault(key, newDict[batch][tstamp][key])

				for val in uniqueNumlist:
					if "Archive-opc-CUTTACK_TIMESTAMP_" + str(val) in row:
						del row["Archive-opc-CUTTACK_TIMESTAMP_"+ str(val)]
						row["Archive-opc-CUTTACK_TIMESTAMP_EP"] = row["Archive-opc-CUTTACK_TIMESTAMP_EP_"+ str(val)]
						del row["Archive-opc-CUTTACK_TIMESTAMP_EP_"+ str(val)]
					if val not in [0, "0"]:
						row.setdefault("Actual_Qty_"+str(val), 0)
						row.setdefault("Set_Qty_"+str(val), 0)
						row.setdefault("Material_Type_"+str(val), 0)
				rowsToPrint.append(row)


		if rowsToPrint != []:
			print(str(len(rowsToPrint)) + " ready to push")
			print({'asofdate': self._date })
			x = myDb["aarti_steel_staging"].delete_many({'as_of_date': self._date })
			
			print(str(x.deleted_count) + " deleted...")
			# myDb["aarti_steel_staging"].remove({'asofdate': self._date })
			record_id = myDb["aarti_steel_staging"].insert_many(rowsToPrint)
			# print(record_id)

		# file = "trialText_refreshed.txt"

		# with open(file, 'w') as f:
		# 	w = csv.DictWriter(f, rowsToPrint[0].keys(), dialect = "excel-tab")
		# 	w.writeheader()
		# 	w.writerows(rowsToPrint)


if __name__ == "__main__":
	RunStage("2019-11-25")
	RunStage("2019-11-26")
	RunStage("2019-11-27")
	RunStage("2019-11-28")
	RunStage("2019-11-29")
	RunStage("2019-11-30")
