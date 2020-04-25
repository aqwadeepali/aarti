import os, json
from datetime import timedelta
from datetime import datetime
import time
from flask import jsonify, request, Response, json
from werkzeug.datastructures import Headers
import uuid
import csv
import os, json
import zipfile
import re
import ast
import copy
import xlsxwriter

from logging import getLogger, INFO, ERROR, WARNING, CRITICAL, NOTSET, DEBUG
from settings import Settings, Settings_File


FILE_PATH = str(os.path.realpath(__file__))
FILE_PATH = FILE_PATH.replace("services.pyc", "")
FILE_PATH = FILE_PATH.replace("services.pyo", "")
FILE_PATH = FILE_PATH.replace("services.py", "")
FILE_PATH = FILE_PATH.replace("classes", "")
FILE_PATH = FILE_PATH + "downloads/"

CONFIG_PATH = str(os.path.realpath(__file__))
CONFIG_PATH = CONFIG_PATH.replace("services.pyc", "")
CONFIG_PATH = CONFIG_PATH.replace("services.pyo", "")
CONFIG_PATH = CONFIG_PATH.replace("services.py", "")
CONFIG_PATH = CONFIG_PATH + "config/"
CUSTOMER_PATH = CONFIG_PATH + "customer/"

def register_services(app, WSGI_PATH_PREFIX):
    BaseServices(app, WSGI_PATH_PREFIX)

class BaseServices:
    # print('                    Registered Services')
    # print('-------------------------------------------------------------------')
    def __init__(self, app, API):
        self.app = app
        self.API = API
        self.dManagers = self.app.config["Managers"]["DataManager"]
        self.mongoManger = self.dManagers.get_connection()

        self.mongo = self.mongoManger["analyse_db"]
        self.log = getLogger()
        self.all_settings = {}

        self.app.add_url_rule(API + '/services/sendfresponse', 'sendfresponse', self.sendfresponse, methods=['GET','POST'])

        # # Home API
        self.app.add_url_rule(API + '/services/getConfig', 'getConfig', self.get_config, methods=['POST'])
        self.app.add_url_rule(API + '/services/getCustomer', 'getCustomer', self.get_customer, methods=['POST'])
        self.app.add_url_rule(API + '/services/getSummary', 'getSummary', self.get_summary, methods=['POST'])
        self.app.add_url_rule(API + '/services/downloadSummary', 'downloadSummary', self.download_summary, methods=['POST'])


    def get_params(self, request):
        return request.json if (request.method == 'POST') else request.args

    def sendfresponse(self):
        try:
            params = self.get_params(request)
            if params != False:
                fname = params.get('name')

                fPath = FILE_PATH + fname

                file = open(fPath, 'rb').read()
                response = Response()
                response.data = file
                response.status_code = 200
                response.headers = Headers()
                response.headers['Pragma'] = 'public'
                response.headers['Expires'] = '0'
                response.headers['Cache-Control'] = 'public'
                response.headers['Content-Description'] = 'File Transfer'
                response.headers['Content-Type'] = 'application/octet-stream'#'text/plain' #'application/vnd.ms-excel' #'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                response.headers['Content-Disposition'] = 'attachment; filename='+fname
                response.headers['Content-Transfer-Encoding'] = 'binary'
                # response.headers['X-Key'] = self.KEY

                os.remove(fPath)

                return response
            else:
                return False
        except Exception as e:
            self.log.exception("sendfresponse Exception....")
            self.log.exception(e)

    def get_config(self):
        params = self.get_params(request)
        result = {}

        files = []

        for r, d, f in os.walk(CONFIG_PATH):
            for file in f:
                if '.ini' in file:
                    if file != "stage_config.ini":
                        files.append(file)

        # for f in files:
            # print(f);

        self.all_settings = Settings(files)

        result = {
            "files": files,
            "count": len(files),
            "config": self.all_settings.config
        }

        return jsonify(Results=result)


    def get_customer(self):
        params = self.get_params(request)

        customer_data = self.all_settings.config.customer

        result = { "name": customer_data["name"], "logo": customer_data["logo"] }

        return jsonify(Results=result)

    def get_summary_data(self):
        params = self.get_params(request)
        filters = params.get("filters", {})

        fromDate = filters.get("fromDate", "")
        toDate = filters.get("toDate", "")
        cfile = filters.get("file", "")

        settings = Settings_File(cfile)

        # print(fromDate,'----- fromdate')
        # print(toDate,'----- todate')

        all_columns = settings.columns
        all_sequnce = settings.sequence
        all_merged = settings.mergedtitle
        mode = settings.type



        db = self.mongo[settings.collection]
        
        ls_date = toDate
        gt_date = fromDate
        lt_tm = time.mktime(datetime.strptime(ls_date,'%Y-%m-%d %H:%M').timetuple())
        gt_tm = time.mktime(datetime.strptime(gt_date,'%Y-%m-%d %H:%M').timetuple())
        to_find = {"$gte": gt_tm, "$lte": lt_tm }

        # print({ all_columns["timestampep"] : to_find },'---', { all_columns["timestampep"] : 1 })
        dbresult = db.find( { all_columns["timestampep"] : to_find } ).sort( [( all_columns["timestampep"] , 1 ) ])
        
        # item_count = db.count_documents({all_columns["timestampep"]:to_find})
        # print(item_count)

        rowspanheaders = []
        colspanheaders = []
        columns = []
        for seq in all_sequnce:
            if '_' in all_sequnce[seq]:
                colsplit = all_sequnce[seq].split("_")
                colnum = colsplit[1]
                colproperty = ast.literal_eval(all_merged[colnum])
                colspan = int(colproperty["colspan"])
                eachwidth = int(colproperty["width"]) / colspan
                colspanheaders.append(all_sequnce[seq])
                columns.append( {"field": all_sequnce[str(seq)], "header": all_columns[all_sequnce[seq]], "width": str(eachwidth) + "px"} )
            else:
                rowspanheaders.append({"header":all_columns[all_sequnce[seq]]})
                columns.append( {"field": all_sequnce[str(seq)], "header": all_columns[all_sequnce[seq]], "width": "125px"} )

            colspanheader_1 = []
        colspanheader_2 = []

        colCount = []
        for col in colspanheaders:
            colsplit = col.split("_")
            colname = int(colsplit[1])
            if colname not in colCount:
                colCount.append(colname)

        for cnt in sorted(colCount):
            colproperty = ast.literal_eval(all_merged[str(cnt)])
            colspanheader_1.append({"header": colproperty["title"], "colspan": colproperty["colspan"], "width":colproperty["width"]+"px"})

        seqlist = [int(x) for x in sorted(all_sequnce.keys())]
        for seq in sorted(seqlist):
            if all_sequnce[str(seq)] in colspanheaders:
                colspanheader_2.append({ "header" : all_columns[all_sequnce[str(seq)]] })

        all_data = []
        if mode == "daily":
            for node in dbresult:
                eachrow = {}
                for seq in sorted(seqlist):
                    eachrow.setdefault(all_sequnce[str(seq)], node.get(all_columns[all_sequnce[str(seq)]], ""))
                eachrow["date"] = datetime.strftime(eachrow["date"], "%Y-%m-%d %H:%M:%S")
                # eachrow["time"] = datetime.strftime(eachrow["time"], "%Y-%m-%d %H:%M:%S")
                all_data.append(eachrow)

        if mode == "monthly":
            all_rows = {}
            for node in dbresult:
                node_date = node[all_columns["date"]].strftime("%Y-%m-%d")
                node_furnace = node[all_columns["furnace"]]

                #################### Uncomment below line if Customer don't want to see records where Furnace is "--". Provide 1 tab to lines from 216-239
                # if node_furnace not in [None, "--"]:       

                if node_date not in all_rows.keys():
                    all_rows.setdefault(node_date, {})
                if node_furnace not in all_rows[node_date]:
                    all_rows[node_date].setdefault(node_furnace, {})

                if node_date not in all_rows.keys():
                    all_rows.setdefault(node_date, {})
                    if node_furnace not in all_rows[node_date]:
                        all_rows[node_date].setdefault(node_furnace, {})
                    for seq in sorted(seqlist):
                        if all_sequnce[str(seq)] not in ["date", "furnace"] and not all_sequnce[str(seq)].startswith("materialtype"):
                            all_rows[node_date][node_furnace].setdefault(all_sequnce[str(seq)], float(node.get(all_columns[all_sequnce[str(seq)]], 0)))
                        else:
                            all_rows[node_date][node_furnace].setdefault(all_sequnce[str(seq)], node.get(all_columns[all_sequnce[str(seq)]]))
                else:
                    if node_furnace not in all_rows[node_date]:
                        all_rows[node_date].setdefault(node_furnace, {})
                    for seq in sorted(seqlist):
                        if all_sequnce[str(seq)] not in ["date", "furnace"] and not all_sequnce[str(seq)].startswith("materialtype"):
                            all_rows[node_date][node_furnace][all_sequnce[str(seq)]] = round(float(all_rows[node_date][node_furnace].get(all_sequnce[str(seq)],0)) + float(node.get(all_columns[all_sequnce[str(seq)]], 0)),2)
                        else:
                            all_rows[node_date][node_furnace]["date"] = node_date
                            all_rows[node_date][node_furnace]["furnace"] = node_furnace
                            all_rows[node_date][node_furnace][all_sequnce[str(seq)]] = node.get(all_columns[all_sequnce[str(seq)]], "")

            for dt in all_rows:
                for furnace in all_rows[dt]:
                    eachrow = {}
                    for seq in sorted(seqlist):
                        eachrow.setdefault(all_sequnce[str(seq)], all_rows[dt][furnace].get(all_sequnce[str(seq)], ""))
                    all_data.append(eachrow)
        # print(all_data)
        result = {
            "rowspancount": "2",
            "colspancount": "3",
            "columns": columns,
            "rowspan": rowspanheaders,
            "colspan1": colspanheader_1,
            "colspan2": colspanheader_2,
            "data": all_data
        }

        return result

    def get_summary(self):
        
        result = self.get_summary_data()

        return jsonify(Results=result)

    def download_summary(self):
        params = self.get_params(request)

        filters = params.get("filters", {})
        cfile = filters.get("file", "")

        settings = Settings_File(cfile)

        all_columns = settings.columns
        all_sequnce = settings.sequence
        all_merged = settings.mergedtitle
        customer_data = settings.customer

        result = self.get_summary_data()
        all_data = result["data"]
        rowheader = result["rowspan"]
        rowspan = result["rowspancount"]
        colheader = result["colspan1"]
        colsubheader = result["colspan2"]

        excelname = "Result.xlsx"
        filename = FILE_PATH + excelname
        if os.path.exists(filename):
            os.remove(filename)

        workbook = xlsxwriter.Workbook( filename )
        worksheet = workbook.add_worksheet()

        merge_format = workbook.add_format({
                        'bold': 1,
                        'border': 1,
                        'font_size': 12,
                        'align': 'center',
                        'valign': 'vcenter',
                        'fg_color': '#C0C0C0'})

        heading_format = workbook.add_format({
                        'bold': 1,
                        'border': 1,
                        'font_size': 18,
                        'align': 'center',
                        'valign': 'vcenter',
                        'fg_color': 'white'})

        subheading_format = workbook.add_format({
                        'bold': 1,
                        'border': 0,
                        'font_size': 14,
                        'align': 'center',
                        'valign': 'vcenter',
                        'fg_color': 'white'})


        #add logo
        # worksheet.insert_image('B2', CUSTOMER_PATH + customer_data["logo"])

        start_row = 10

        r = start_row 
        c = 0
        for row in rowheader:
            worksheet.merge_range(r, c, r + 1, c, row["header"], merge_format) # first row, first col, last row, last col
            c += 1
        
        r = start_row
        for row in colheader:
            colspan = int(row["colspan"]) - 1
            worksheet.merge_range(r, c, r, c + colspan, row["header"], merge_format)
            c = c + colspan
            c += 1

        r = start_row + 1
        c = len(rowheader)
        for row in colsubheader:
            worksheet.write(r, c, row["header"], merge_format)
            c += 1

        r = start_row + 2
        for node in all_data:
            c = 0
            for seq in all_sequnce:
                worksheet.write(r, c, node[all_sequnce[seq]])
                c += 1
            r += 1


        worksheet.insert_image(0, 0, CUSTOMER_PATH + customer_data["logo"])
        worksheet.merge_range(0, 0, start_row - 5, c-1, customer_data["name"], heading_format)
        worksheet.merge_range(start_row - 4, 0, start_row - 3, c-1, "#Title 2", subheading_format)
        worksheet.merge_range(start_row - 2, 0, start_row - 1, c-1, "Daily Batching Report", subheading_format)

        workbook.close()

        return jsonify(Results=excelname)


