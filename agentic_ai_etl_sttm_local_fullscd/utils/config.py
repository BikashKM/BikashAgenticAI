import os, yaml
from dotenv import load_dotenv
def load_config():
    if os.path.exists('.env'): load_dotenv('.env')
    cfg = {}
    if os.path.exists('config.yaml'):
        with open('config.yaml','r',encoding='utf-8') as f:
            cfg = yaml.safe_load(f) or {}
    return cfg or {}
