import json
import os
from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import load, dump

def file_exists(file_path):
    return os.path.exists(file_path)

class SfConfig():

    def __init__(self, filename, type='json'):
        if type == 'json':
            home_dir   = os.path.join(os.path.expanduser("~"), '.snowflake')
            cur_dir    = os.getcwd()

            config = {}
        
            # Create initial configuration from the script_dir
            home_config_file = os.path.join(home_dir, filename)
            cur_config_file = os.path.join(cur_dir, filename)
        
            if (file_exists(home_config_file)):
                config = json.loads(open(str(home_config_file)).read())
                self.filename = home_config_file
                if (file_exists(cur_config_file) and cur_config_file != home_config_file):
                    # don't overwrite
                    tmp_config = json.loads(open(str(cur_config_file)).read())
                    self.filename = self.filename + ':' + cur_config_file
                    for config_key in tmp_config:
                        config[config_key] = tmp_config[config_key]
            elif (file_exists(cur_config_file) and cur_config_file != home_config_file):
                config = json.loads(open(str(cur_config_file)).read())
                self.filename = cur_config_file
            self.config = config
        if type == 'yaml':
            stream = open(filename, 'r')
            yaml_config = load(stream, Loader)
            self.config = yaml_config