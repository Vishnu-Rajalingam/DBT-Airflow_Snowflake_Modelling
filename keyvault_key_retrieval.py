from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# Replace with your Azure Key Vault URL and the name of the secret you want to retrieve
key_vault_url = "https://vrrgkv5.vault.azure.net"
secret_name = "snowflake-account"

# Create a SecretClient
credential = DefaultAzureCredential()
secret_client = SecretClient(vault_url=key_vault_url, credential=credential)

# Retrieve the secret
try:
    secret = secret_client.get_secret(secret_name)
    print(f"Secret name: {secret.name}")
    print(f"Secret value: {secret.value}")
except Exception as e:
    print(f"Failed to retrieve the secret: {e}")
