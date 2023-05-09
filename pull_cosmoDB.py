#!/bin/python3

import configparser
import sys
from azure.cosmos import exceptions, CosmosClient
from pprint import pprint
import json


def normalize_id(i):
    if "_" in i:
        print(f"normalizing {i}")
        i = i.replace("_", "-")
    return i.lower()


def infer_type(i):
    i = normalize_id(i)
    if i.startswith("s"):
        return "Scenario"
    elif i.startswith("o"):
        return "Organization"
    elif i.startswith("sr"):
        return "ScenarioRun"
    elif i.startswith("sol"):
        return "Solution"
    elif i.startswith("u"):
        return "User"
    elif i.startswith("d"):
        return "Dataset"
    elif i.startswith("w"):
        return "Workspace"
    elif i.startswith("c"):
        return "Connector"
    else:
        return "Unknown"


def item_clean(_i) -> (str, dict):
    if 'type' in _i:
        t = _i.pop('type')
    else:
        t = infer_type(_i['id'])

    i = _i.copy()
    for k in _i.keys():
        if k.startswith("_"):
            i.pop(k)
    return (t, i)


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read(sys.argv[1])
    conf = config['DEFAULT']
    client = CosmosClient(conf['url'], conf['key'])

    unique = set()

    p_client = client.get_database_client(conf['database'])
    for c in p_client.list_containers():
        c_client = p_client.get_container_client(c['id'])
        for i in c_client.read_all_items():
            item_type, item = item_clean(i)
            print(f"Item {item['id']} of type {item_type}")

            with open(f"out/{item_type}_{item['id']}.json", "w") as f:
                f.write(json.dumps(item, indent=4))
