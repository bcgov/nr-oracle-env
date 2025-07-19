import msal
import os
import requests

client_id = os.getenv("MSAL_CLIENT_ID")
tenant_id = os.getenv("MSAL_TENANT_ID")

authority = f"https://login.microsoftonline.com/{tenant_id}"
scopes = ["https://graph.microsoft.com/.default"]

app = msal.PublicClientApplication(client_id, authority=authority)

# Start the device code flow
flow = app.initiate_device_flow(scopes=scopes)
print(flow["message"])  # Go to URL and enter the user code shown here

result = app.acquire_token_by_device_flow(flow)

if "access_token" in result:
    access_token = result["access_token"]
    print("ACCESS TOKEN:", access_token)

    # Test Graph call
    headers = {"Authorization": f"Bearer {access_token}"}
    r = requests.get("https://graph.microsoft.com/v1.0/me", headers=headers)
    print(r.json())
else:
    print(result.get("error"))
