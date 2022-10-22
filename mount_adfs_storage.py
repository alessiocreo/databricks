# Databricks notebook source
storage_account_name = "datameshposte"
client_id = dbutils.secrets.get(scope="poste",key="client-id")
tenant_id = dbutils.secrets.get(scope="poste",key="tenant-id")
client_secret = dbutils.secrets.get(scope="poste",key="client-secret")

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type":"OAuth",
    "fs.azure.account.oauth.provider.type":"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id":f'{client_id}',
    "fs.azure.account.oauth2.client.secret":f'{client_secret}',
    "fs.azure.account.oauth2.client.endpoint":f'https://login.microsoftonline.com/{tenant_id}/oauth2/token'
}

# COMMAND ----------

dbutils.fs.mount(
    source = f'abfss://raw@{storage_account_name}.dfs.core.windows.net/',
    mount_point = f'/mnt/{storage_account_name}/raw',
    extra_configs = configs
)

# COMMAND ----------

dbutils.fs.ls("/mnt/datameshposte")

# COMMAND ----------

dbutils.fs.mount(
    source = f'abfss://processed@{storage_account_name}.dfs.core.windows.net/',
    mount_point = f'/mnt/{storage_account_name}/processed',
    extra_configs = configs
)
