## Migration Cosmotech Tool from CosmosDB to Redis.

### Description:
Python script needs to be connected to 2 APIs:
* Cosmotech API with CosmosDB (since version V2)
* Cosmotech API with Redis

The script migrates all objects in order as shown in the table below:

| Migration                         | by Organization | by Workspace |
|-----------------------------------|-----------------|--------------|
| Azure connection made with az cli |
| Connectors                        |
| Organization(s)                   | &#8628;         
|                                   | Solutions       |
|                                   | Datasets        |
|                                   | Workspaces      | &#8628;      
|                                   |                 | Scenarios    
|                                   |                 | ScenarioRuns  

### During the migration:
* All dates are converted to Unix timestamp
* Lost objects aren’t migrated (like ScenarioRun without Scenario)
* If object already exists in Redis, it will be updated
* Script provides logs that can be found in application.log file


### Installation
Login with az cli
``` powershell
az login
```

## Run
Copy and complete [config.yaml.template](config.yaml.template) to config.yaml.
Provide the URL and scope of the CosmosDB and Redis APIs.
``` yaml
# Url and scope of the CosmosDB API to migrate from
cosmosdb:
  url: [URL]
  scope: [SCOPE]

# Url and scope of the Redis API to migrate to
redis:
  url: [URL]
  scope: [SCOPE]
```

Then run the script
``` bash
pipenv shell
pipenv install
python main.py
```
 