# Copyright (c) Cosmo Tech.
# Licensed under the MIT license.l
import csv
import logging
import sys
from dataclasses import dataclass

import cosmotech_api
import yaml
from azure.common.credentials import UserPassCredentials
from azure.graphrbac import GraphRbacManagementClient
from azure.identity import DefaultAzureCredential
from cosmotech_api import ApiClient
from cosmotech_api import Configuration
from cosmotech_api.api import connector_api
from cosmotech_api.api import dataset_api
from cosmotech_api.api import solution_api
from cosmotech_api.api import organization_api
from cosmotech_api.api import scenario_api
from cosmotech_api.api import scenariorun_api
from cosmotech_api.api import workspace_api
from cosmotech_api.model.organization import Organization
from cosmotech_api.model.scenario import Scenario
from cosmotech_api.model.workspace import Workspace

csv_file = open('cosmos_redis_migration_report.csv', 'w', encoding='UTF8')
header_csv = ['RESOURCE', 'ID', 'OWNER_ID', 'OWNER_MAIL', 'STATUS', 'USERS']
csv_writer = csv.writer(csv_file)
csv_writer.writerow(header_csv)

logger = logging.getLogger()
fileHandler = logging.FileHandler("application.log")
streamHandler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
streamHandler.setFormatter(formatter)
fileHandler.setFormatter(formatter)
logger.addHandler(streamHandler)
logger.addHandler(fileHandler)
logger.setLevel(logging.DEBUG)
TRACE_DOCUMENTS = False


def get_graphclient(config_file):
    if config_file['options']['fetch_from_azure_ad']:
        logger.info("logging in for graph API")
        print()
        credentials = UserPassCredentials(
            config_file['azure']['user'],
            'k3MP6zXqlJBMmrXqX6kw',
            # getpass.getpass(prompt='Please enter Azure account password: '),
            resource="https://graph.windows.net")
        tenant_id = config_file['azure']['tenant']
        graphrbac_client = GraphRbacManagementClient(credentials, tenant_id)
        return graphrbac_client
    else:
        logger.info(
            "Option to fetch users from Azure AD is disabled in config")
        return None


def get_apiclient(config_file):
    host = config_file['cosmos']['url']
    scope = config_file['cosmos']['scope']

    logger.debug("cosmos logging in")
    credential = DefaultAzureCredential()
    logger.debug("cosmos Getting token")
    token = credential.get_token(scope)

    configuration = Configuration(host=host,
                                  discard_unknown_keys=True,
                                  access_token=token.token)

    return ApiClient(configuration)


def get_redisclient(config_file):
    host = config_file['redis']['url']
    scope = config_file['redis']['scope']

    logger.debug("redis logging in")
    credential = DefaultAzureCredential()
    logger.debug("redis Getting token")
    token = credential.get_token(scope)

    configuration = Configuration(host=host,
                                  discard_unknown_keys=True,
                                  access_token=token.token)
    return ApiClient(configuration)


def migrate_connectors(config, context):
    logger.info("Migrating connectors")
    try:
        redis_connector = connector_api.ConnectorApi(config.redis_client)
        api_connector = connector_api.ConnectorApi(config.api_client)

        connectors = api_connector.find_all_connectors()
        logger.info("Found " + f"{len(connectors)}" + " connectors")
        for connector in connectors:
            new_connector = redis_connector.register_connector(connector)
            context.connectorDict[connector.id] = new_connector.id
            logger.info("Migrated connector " + f"{connector.id}")
    except cosmotech_api.ApiException as e:
        logger.error("Exception when migrating connectors " + f"{e}")


def migrate_organizations(config, ctx):
    logger.info("Migrating organizations")
    try:
        redis_organization = organization_api.OrganizationApi(config.redis_client)
        api_organization = organization_api.OrganizationApi(config.api_client)

        if ctx.organizationId is not None:
            organizations = [api_organization.find_organization_by_id(ctx.organizationId)]
        else:
            organizations = api_organization.find_all_organizations()
        logger.info("Found " + f"{len(organizations)}" + " organizations")
        for organization in organizations:
            new_organization = redis_organization.register_organization(organization)
            ctx.organizationId = organization.id
            ctx.organizationNewId = new_organization.id
            logger.info("Migrated organization " + f"{organization.id}")
            migrate_solutions(config, ctx)
            # migrate_datasets(config, ctx)
            migrate_workspaces(config, ctx)
    except cosmotech_api.ApiException as e:
        logger.error("Exception when migrating organizations " + f"{e}")


def migrate_solutions(config, ctx):
    logger.info("Migrating solutions for organization " + f"{ctx.organizationId}")
    try:
        redis_solution = solution_api.SolutionApi(config.redis_client)
        api_solution = solution_api.SolutionApi(config.api_client)

        solutions = api_solution.find_all_solutions(ctx.organizationId)
        logger.info("Found " + f"{len(solutions)}" + " solutions")
        for solution in solutions:
            solution.organization_id = ctx.organizationNewId
            new_solution = redis_solution.create_solution(ctx.organizationNewId, solution)
            ctx.solutionDict[solution.id] = new_solution.id
            logger.info("Migrated solution " + f"{solution.id}")
    except cosmotech_api.ApiException as e:
        logger.error("Exception when migrating solutions " + f"{e}")


def migrate_datasets(config, ctx):
    logger.info("Migrating datasets for organization " + f"{ctx.organizationId}")
    try:
        redis_dataset = dataset_api.DatasetApi(config.redis_client)
        api_dataset = dataset_api.DatasetApi(config.api_client)

        datasets = api_dataset.find_all_datasets(ctx.organizationId)
        logger.info("Found " + f"{len(datasets)}" + " datasets")
        for dataset in datasets:
            if dataset.connector is None:
                logger.warning("Skipping dataset without connector: " + f"{dataset.id}")
                continue
            dataset.organization_id = ctx.organizationNewId
            dataset.connector.id = ctx.connectorDict[dataset.connector.id]
            new_dataset = redis_dataset.create_dataset(ctx.organizationNewId, dataset)
            ctx.datasetDict[dataset.id] = new_dataset.id
            logger.info("Migrated dataset " + f"{dataset.id}")
    except cosmotech_api.ApiException as e:
        logger.error("Exception when migrating datasets " + f"{e}" + f"{dataset}")


def migrate_workspaces(config, ctx):
    logger.info("Migrating workspaces for organization " + f"{ctx.organizationId}")
    try:
        redis_workspace = workspace_api.WorkspaceApi(config.redis_client)
        api_workspace = workspace_api.WorkspaceApi(config.api_client)

        workspaces = api_workspace.find_all_workspaces(ctx.organizationId)
        logger.info("Found " + f"{len(workspaces)}" + " workspaces")
        for workspace in workspaces:
            if workspace.solution is None:
                logger.warning("Skipping workspace without solution: " + f"{workspace.id}")
                continue
            try:
                workspace.solution.solution_id = ctx.solutionDict[workspace.solution.solution_id]
            except KeyError:
                logger.warning("Solution not found: " + f"{workspace.solution.solution_id}")
                continue
            workspace.organization_id = ctx.organizationNewId
            new_workspace = redis_workspace.create_workspace(
                ctx.organizationNewId,
                workspace)
            ctx.workspaceId = workspace.id
            ctx.workspaceNewId = new_workspace.id
            logger.info("Migrated workspace " + f"{workspace.id}")
            migrate_scenarios(config, ctx)

    except cosmotech_api.ApiException as e:
        logger.error("Exception when calling " +
                     "workspace_api->find_all_workspaces: " + f"{e}")


def migrate_scenarios(config, ctx):
    logger.info("Migrating scenarios for organization:" + f"{ctx.organizationId}" + ", workspace " + f"{ctx.workspaceId}")
    try:
        redis_scenario = scenario_api.ScenarioApi(config.redis_client)
        api_scenario = scenario_api.ScenarioApi(config.api_client)

        scenarios = api_scenario.find_all_scenarios(ctx.organizationId,
                                                    ctx.workspaceId)
        logger.info("Found " + f"{len(scenarios)}" + " scenarios")
        for scenario in scenarios:
            scenario.organization_id = ctx.organizationNewId
            scenario.workspace_id = ctx.workspaceNewId
            try:
                scenario.solution_id = ctx.solutionDict[scenario.solution_id]
            except KeyError:
                logger.warning("Solution not found: " + f"{scenario.solution_id}")
                continue

            scenario = redis_scenario.create_scenario(
                ctx.organizationNewId,
                ctx.workspaceNewId,
                scenario)
            ctx.scenarioId = scenario.id
            ctx.scenarioNewId = scenario.id
            logger.info("Migrated scenario " + f"{scenario.id}")
            # migrate_scenarioruns(config, ctx)

    except cosmotech_api.ApiException as e:
        logger.error("Exception when migrating scenarios: " + f"{e}")


def migrate_scenarioruns(config, ctx):
    logger.info("Migrating scenarioruns for organization:" + f"{ctx.organizationId}" + ", workspace " + f"{ctx.workspaceId}" + ", scenario " + f"{ctx.scenarioId}")
    try:
        redis_scenariorun = scenariorun_api.ScenariorunApi(config.redis_client)
        api_scenariorun = scenariorun_api.ScenariorunApi(config.api_client)

        scenarioruns = api_scenariorun.find_all_scenarioruns(ctx.organizationId,
                                                            ctx.workspaceId,
                                                        ctx.scenarioId)
        logger.info("Found " + f"{len(scenarioruns)}" + " scenarioruns")
        for scenariorun in scenarioruns:
            scenariorun.organization_id = ctx.organizationNewId
            scenariorun.workspace_id = ctx.workspaceNewId
            scenariorun.scenario_id = ctx.scenarioNewId
            try:
                scenariorun.solution_id = ctx.solutionDict[scenariorun.solution_id]
            except KeyError:
                logger.warning("Solution not found: " + f"{scenariorun.solution_id}")
                continue

            scenariorun = redis_scenariorun.create_scenariorun(
                ctx.organizationNewId,
                ctx.workspaceNewId,
                ctx.scenarioNewId,
                scenariorun)
            logger.info("Migrated scenariorun " + f"{scenariorun.id}")
    except cosmotech_api.ApiException as e:
        logger.error("Exception when migrating scenarioruns: " + f"{e}")


def get_config():
    with open('config.yaml', 'r') as config_file:
        return yaml.safe_load(config_file)


def build_config(api_client, graph_client, redis_client, config_file):
    mapping = {}
    if 'mapping' not in config_file:
        config_file['mapping'] = {}
    if config_file['mapping'] is not None:
        mapping = config_file['mapping']
    return Config(api_client=api_client,
                  graph_client=graph_client,
                  redis_client=redis_client,
                  config_file=config_file,
                  mapping=mapping)


def migrate():
    """Migrate COSMOSDB to REDIS"""
    logging.info("Migration start")
    config_file = get_config()

    with get_apiclient(config_file) as api_client, get_redisclient(config_file) as redis_client:
        graph_client = get_graphclient(config_file)
        config = build_config(api_client, graph_client, redis_client, config_file)
        ctx = Context()
        migrate_connectors(config, ctx)
        if 'organizationId' in config_file['options']:
            ctx.organizationId = config_file['options']['organizationId']
        migrate_organizations(config, ctx)
    csv_file.close()


@dataclass
class Config(object):
    api_client: str
    graph_client: str
    redis_client: str
    config_file: str
    mapping: dict


class Context:
    connectorDict: dict = {}
    organizationDict: dict = {}
    workspaceDict: dict = {}
    datasetDict: dict = {}
    solutionDict: dict = {}
    organizationId: str
    organizationNewId: str
    workspaceId: str
    workspaceNewId: str
    scenarioId: str
    scenarioNewId: str


if __name__ == "__main__":
    migrate()
