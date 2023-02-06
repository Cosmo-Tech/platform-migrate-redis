## Summary

* This Python script migrates CosmosDB to Redis.
* Logs can be found in **application.log**


### Script workflow:


### Constraints:
* This script works since API v2.

### Installation
## Log in with az cli
[Install Powershell](https://learn.microsoft.com/en-us/powershell/scripting/install/install-debian?view=powershell-7.2)
[Install Powershell AZ module](https://learn.microsoft.com/en-us/powershell/azure/install-az-ps?view=azps-8.3.0)
``` powershell
Install-Module -Name Az -Scope CurrentUser -Repository PSGallery -Force
```
Connect in powershell
``` powershell
Connect-AzAccount
```

## Run
Copy and complete [config.yaml.template](config.yaml.template) to config.yaml.
``` bash
pipenv shell
pipenv install
python main.py
```

## Report example format
