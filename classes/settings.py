from configparser import SafeConfigParser
import os

base_path = os.path.dirname(__file__)
COMMON_PATH = os.path.join(base_path, "config/")

class Settings:
    def __init__( self, files=[] ):
        path = os.path.dirname(__file__)
        config = SafeConfigParser()
        self.config = {}
        for f in files:
            if f not in ["stage_config.ini"]:
                cfile = f
                config.read(COMMON_PATH + cfile)
                customer = dict(config.items("customer", {}))
                # collection = str(config.get("collection", "name"))
                # columns = dict(config.items("columns", {}))
                # sequence = dict(config.items("sequence", {}))
                # mergedtitle = dict(config.items("mergedtitle", {}))

                self.config.setdefault(cfile, {}).setdefault("customer", customer)
                # self.config.setdefault(cfile, {}).setdefault("collection", collection)
                # self.config.setdefault(cfile, {}).setdefault("columns", columns)
                # self.config.setdefault(cfile, {}).setdefault("sequence", sequence)
                # self.config.setdefault(cfile, {}).setdefault("mergedtitle", mergedtitle)


class Settings_File:
    def __init__( self, file ):
        path = os.path.dirname(__file__)
        config = SafeConfigParser()
        cfile = file
        config.read(COMMON_PATH + cfile)

        self.customer = dict(config.items("customer", {}))
        self.collection = str(config.get("collection", "name"))
        self.type = str(config.get("type", "mode"))
        self.columns = dict(config.items("columns", {}))
        self.sequence = dict(config.items("sequence", {}))
        self.mergedtitle = dict(config.items("mergedtitle", {}))

    def get_settings(self):
    	return { "type": self.type, "collection": self.collection, "columns": self.columns, "sequence": self.sequence, "mergedtitle":self.mergedtitle }
    
    
class Settings_Stage:
    def __init__(self, file):
        config = SafeConfigParser()
        self.config = {}
        
        config.read(COMMON_PATH + file)

        materials = dict(config.items("materialType", {}))
        furnaces = dict(config.items("furnace", {}))
        collection = str(config.get("collection", "name"))
        # modetype = str(config.get("type", "mode"))

        self.config.setdefault("collection", collection)
        # self.config.setdefault("type", modetype)
        self.config.setdefault("materials", materials)
        self.config.setdefault("furnaces", furnaces)


