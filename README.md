## Migration Cosmotech Tool from CosmosDB to Redis.


Python script needs to be connected to API & to Redis directly with CosmosDB credentials and Redis password that can be 
found in Kubernetes Cosmotechredis Secret

The script migrates all objects from CosmosDB to RedisJSON:
* Connectors
* Organizations
* Solutions
* Datasets
* Workspaces
* Scenarios
* ScenarioRuns

During the migration all dates are converted to Unix timestamp

The script can be run in 2 modes:
* As python script : 
``` bash
pipenv shell
pipenv install
python main.py
```
* As pod with bound env variables during upgrade installation with `upgrade.sh`


In both cases Env variables have to been defined:

``` bash
COSMOSDB_DATABASE_NAME=my_cosmosdb_name
COSMOSDB_KEY=my_cosmodb_password
COSMOSDB_URL=https://my_cosmosdb_url
REDIS_SERVER=localhost
REDIS_PASSWORD=my_redis_password
```
