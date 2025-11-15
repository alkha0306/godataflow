# frontend/utils/api.py
import requests

def api_get(url, params=None):
    try:
        r = requests.get(url, params=params)
        return r.json() if r.ok else None
    except:
        return None

def api_post(url, json=None):
    try:
        r = requests.post(url, json=json)
        return r.json() if r.ok else None
    except:
        return None

def api_put(url, json=None):
    try:
        r = requests.put(url, json=json)
        return r.json() if r.ok else None
    except:
        return None

def api_delete(url):
    try:
        r = requests.delete(url)
        return r.json() if r.ok else None
    except:
        return None
