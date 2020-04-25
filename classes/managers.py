# from datetime import datetime, timedelta
# from xml.etree import ElementTree

from data import DataManager
# from compression import CompressionManager

def register_managers(app, WSGI_PATH_PREFIX):
    return Managers(app, WSGI_PATH_PREFIX)

class Managers:
    def __init__(self, app, WSGI_PATH_PREFIX):
        self.app = app
        # self.cmpmgr = CompressionManager( app, compresslevel=9 )
        self.DataManager = DataManager()
        mngrs = {
            # 'Compression': self.cmpmgr,
            'DataManager': self.DataManager
        }
        self.app.config.setdefault('Managers', mngrs)
        self.app.config['Managers'] = mngrs
        print('             Registered Application Managers')
        print('------------------------------------------------------------------')

