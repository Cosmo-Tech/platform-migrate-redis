## Summary

* This Python script migrates CosmosDB to Redis.
* Logs can be found in **application.log**


### Script workflow:


### Constraints:
* This script works since API v2.
* If object already exists in Redis, it will be updated.

### Installation
Login with az cli
``` powershell
az login
```

## Run
Copy and complete [config.yaml.template](config.yaml.template) to config.yaml.
``` bash
pipenv shell
pipenv install
python main.py
```
