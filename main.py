#!/bin/python3

import logging
import os
import sys

from azure.cosmos import CosmosClient
from datetime import datetime
from cosmotech_api import ApiClient
from cosmotech_api import Configuration
from cosmotech_api.api import connector_api
from cosmotech_api.api import solution_api
from cosmotech_api.api import dataset_api
from cosmotech_api.api import organization_api
from cosmotech_api.api import scenario_api
from cosmotech_api.api import scenariorun_api
from cosmotech_api.api import workspace_api
import json
from azure.identity import DefaultAzureCredential

COSMOSDB_URL = "COSMOSDB_URL"
COSMOSDB_KEY = "COSMOSDB_KEY"
COSMOSDB_DATABASE_NAME = "COSMOSDB_DATABASE_NAME"
REDIS_API_URL = "REDIS_API_URL"
REDIS_API_SCOPE = "REDIS_API_SCOPE"

env_var_required = [COSMOSDB_URL, COSMOSDB_KEY , COSMOSDB_DATABASE_NAME, REDIS_API_URL, REDIS_API_SCOPE]
missing_env_vars = []
output_folder = '/tmp/out'


def normalize_id(i):
    if "_" in i:
        print(f"normalizing {i}")
        i = i.replace("_", "-")
    return i.lower()


def infer_type(i):
    i = normalize_id(i)
    if i.startswith("o"):
        return "Organization"
    elif i.startswith("sr"):
        return "Scenariorun"
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
    elif i.startswith("s"):
        return "Scenario"
    else:
        return "Unknown"


def convert_to_millis(date_string):
    dates = date_string.split(".", 1)
    millis = dates[1][0:6]
    date_string = dates[0] + "." + millis
    if not date_string.endswith("Z"):
        date_string = date_string + "Z"
    date = datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S.%fZ')
    return int(round(datetime.timestamp(date) * 1000))


def update(api_client, item):
    i = item['id']
    if i.lower().startswith("o"):
        api = organization_api.OrganizationApi(api_client)
        api.import_organization(item)
    elif i.lower().startswith("sr"):
        api = scenariorun_api.ScenariorunApi(api_client)
        api.import_scenario_run(
            item["organizationId"],
            item["workspaceId"],
            item["scenarioId"],
            item
        )
    elif i.lower().startswith("sol"):
        api = solution_api.SolutionApi(api_client)
        api.import_solution(
            item["organizationId"],
            item)
    elif i.lower().startswith("d"):
        api = dataset_api.DatasetApi(api_client)
        api.import_dataset(
            item["organizationId"],
            item
        )
    elif i.lower().startswith("w"):
        api = workspace_api.WorkspaceApi(api_client)
        api.import_workspace(
            item["organizationId"],
            item
        )
    elif i.lower().startswith("c"):
        api = connector_api.ConnectorApi(api_client)
        api.import_connector(item)
    elif i.lower().startswith("s"):
        api = scenario_api.ScenarioApi(api_client)
        api.import_scenario(
            item["organizationId"],
            item["workspaceId"],
            item)
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


def get_cosmosdb():
    client = CosmosClient(cosmosdb_url, cosmosdb_key, logging_enable=True)

    p_client = client.get_database_client(cosmosdb_database_name)
    for container in p_client.list_containers():
        organization_id = container['id'].split("_")[0]
        c_client = p_client.get_container_client(container['id'])
        for i in c_client.read_all_items():
            item_type, item = item_clean(i)
            if organization_id != 'connectors' and organization_id != 'users':
                item['organizationId'] = organization_id
            if 'creationDate' in item:
                item["creationDate"] = convert_to_millis(item["creationDate"])
            if 'lastUpdate' in item:
                item["lastUpdate"] = convert_to_millis(item["lastUpdate"])
            if 'ioTypes' in item:
                item["io_types"] = item["ioTypes"]
                del item["ioTypes"]
            if 'compatibility' in item and item['compatibility'] is not None:
                for compta in item['compatibility']:
                    if 'solutionKey' in compta:
                        compta["solution_key"] = compta["solutionKey"]
                        del compta["solutionKey"]
            logger.info("Item %s of type %s", item['id'], item_type)

            with open(f"{output_folder}/{item_type}_{item['id']}.json", "w") as file:
                file.write(json.dumps(item, indent=4))


def get_redis_api():
    credential = DefaultAzureCredential()
    token = credential.get_token(redis_api_scope)
    configuration = Configuration(host=redis_api_url,
                                  discard_unknown_keys=True,
                                  access_token=token.token)
    return ApiClient(configuration)


def put_cosmosdb_to_redis():
    with get_redis_api() as api:
        for file in os.listdir(output_folder):
            if file.endswith(".json"):
                with open(f"{output_folder}/{file}", "r") as file:
                    item = json.load(file)
                    logger.info("Migrating Item %s", item['id'])
                    update(api, item)


def check_env_var():
    """
    Check if all required environment variables are specified
    """
    for env_var in env_var_required:
        if env_var not in os.environ:
            missing_env_vars.append(env_var)


if __name__ == "__main__":

    log_level_name = os.getenv("LOG_LEVEL") if "LOG_LEVEL" in os.environ else "INFO"
    log_level = logging.getLevelName(log_level_name)
    logging.basicConfig(stream=sys.stdout, level=log_level,
                        format='%(levelname)s(%(name)s) - %(asctime)s - %(message)s',
                        datefmt='%d-%m-%y %H:%M:%S')
    logger = logging.getLogger(__name__)

    check_env_var()
    if not missing_env_vars:
        cosmosdb_url = os.getenv(COSMOSDB_URL)
        cosmosdb_key = os.getenv(COSMOSDB_KEY)
        cosmosdb_database_name = os.getenv(COSMOSDB_DATABASE_NAME)
        redis_api_url = os.getenv(REDIS_API_URL)
        redis_api_scope = os.getenv(REDIS_API_SCOPE)
    else:
        raise Exception(f"Missing environment variables named {missing_env_vars}")

    output_folder_exists = os.path.exists(output_folder)
    if not output_folder_exists:
        os.makedirs(output_folder)

    get_cosmosdb()
    put_cosmosdb_to_redis()
