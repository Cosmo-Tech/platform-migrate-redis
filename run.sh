# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.

set -x

docker run \
--network="host" \
-e COSMOSDB_URL="$(printenv COSMOSDB_URL)" \
-e COSMOSDB_KEY="$(printenv COSMOSDB_KEY)" \
-e COSMOSDB_DATABASE_NAME="$(printenv COSMOSDB_DATABASE_NAME)" \
-e REDIS_API_URL="$(printenv REDIS_API_URL)" \
-e REDIS_API_SCOPE="$(printenv REDIS_API_SCOPE)" \
-e LOG_LEVEL="INFO" \
platform-migrate-redis