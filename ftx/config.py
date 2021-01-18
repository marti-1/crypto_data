import json

def load(filename='config.json'):
    with open(filename) as f:
        data = json.load(f)
    return data