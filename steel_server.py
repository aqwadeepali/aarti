import os, sys, flask

base_path = os.path.dirname(__file__)
class_path = os.path.join(base_path, "classes/")
managers_path = os.path.join(base_path, "classes/managers")
download_path = os.path.join(base_path, "downloads/")
settings_path = os.path.join(base_path, "settings/")

sys.path.insert(0, base_path)
sys.path.insert(1, class_path)
sys.path.insert(2, managers_path)
sys.path.insert(3, download_path)
sys.path.insert(4, settings_path)

WSGI_PATH_PREFIX = ''

from flask import redirect, url_for
import managers, logging_managers, services

class Config:
    def __init__(self, app, WSGI_PATH_PREFIX):
        self.app = app
        # register managers and then services
        print(' * WSGIPrefix set to %s'%WSGI_PATH_PREFIX)
        self.logging_mgrs = logging_managers.register_managers(app, WSGI_PATH_PREFIX)
        self.mgrs = managers.register_managers(app, WSGI_PATH_PREFIX)
        self.srvs = services.register_services(app, WSGI_PATH_PREFIX)

    def reload(self):
        self.mgrs.reload()

# create application
application = flask.Flask(__name__)
cfg = None

@application.route(WSGI_PATH_PREFIX + '/')
def root():
    return redirect(url_for('page'))

@application.route(WSGI_PATH_PREFIX + '/home')
def page():
    return application.send_static_file('index.html')

@application.route(WSGI_PATH_PREFIX + '/reload')
def reload():
    cfg.reload()
    return redirect(url_for('ct_page'))

def set_wsgi_prefix(prefix='/'):
    WSGI_PATH_PREFIX = prefix
    cfg = Config(application, WSGI_PATH_PREFIX)

if __name__ == '__main__':
    WSGI_PATH_PREFIX = '/api_steel'
    cfg = Config(application, WSGI_PATH_PREFIX)
    dport = int(sys.argv[1]) if len(sys.argv) > 1 else 9000
    application.run(host='0.0.0.0',port=dport,debug=True,use_reloader=True,processes=1,static_files={'/':'dist'})
