from azure.identity import AzureCliCredential, ManagedIdentityCredential, DefaultAzureCredential, ChainedTokenCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.core.exceptions import HttpResponseError
import pandas as pd
from time import sleep

credential = ChainedTokenCredential(AzureCliCredential(), DefaultAzureCredential(), ManagedIdentityCredential())

#LASER
subscription_id = "7bf8fea8-fa06-4796-a265-a90a3de4dc10"

resource_client = ResourceManagementClient(credential, subscription_id)

def resourceGroups():
    # Retrieve the list of resource groups
    # Potential for infinite loop mitigated by 10 minute max timeout of Consumption Function App
    while True:
        try:
            group_list = resource_client.resource_groups.list()
            df_rg = pd.DataFrame({})
            for group in list(group_list):
                if group.tags is not None:
                    budget_code = group.tags.get('Budget Code', 'No budget Code')
                    project_id = group.tags.get('Project ID', 'No Project ID')
                    project_name = group.tags.get('Project Name', 'No Project Name')
                    project_vre = group.tags.get('Project VRE', 'No Project VRE')
                else:
                    budget_code = 'No tags in Azure'
                    project_id = 'No tags in Azure'
                    project_name = 'No tags in Azure'
                    project_vre = 'No tags in Azure'
                df = pd.DataFrame({'ResourceGroup': [group.name], 'Budget Code': [budget_code], 'Project ID': [project_id]
                                , 'Project Name': [project_name], 'Project VRE': [project_vre]})
                df_rg = pd.concat([df_rg, df], ignore_index=True)
        # HttpResponseError Code: 429, Message: Too many requests. Please retry. 
        # If received then wait 15 seconds and try again (within the While loop)
        except HttpResponseError as e:
            if e.status_code == 429:
                sleep(15)
                continue
        # Break out of the While loop
        break
    return df_rg

def resources(rg_list):
    df_r = pd.DataFrame({})
    for resource_group in rg_list:
        # Retrieve the list of resources in group
        # The expand argument includes additional properties in the output.
        # Potential for infinite loop mitigated by 10 minute max timeout of Consumption Function App
        while True:
            try:
                #https://learn.microsoft.com/en-us/rest/api/resources/resources/list-by-resource-group
                resource_list = resource_client.resources.list_by_resource_group(
                    resource_group, expand = "createdTime,changedTime")
                for resource in list(resource_list):
                    if resource.tags is not None:
                        budget_code = resource.tags.get('Budget Code', 'No Budget Code')
                        project_id = resource.tags.get('Project ID', 'No Project ID')
                        project_name = resource.tags.get('Project Name', 'No Project Name')
                        project_vre = resource.tags.get('Project VRE', 'No Project VRE')
                    else:
                        budget_code = 'No tags in Azure'
                        project_id = 'No tags in Azure'
                        project_name = 'No tags in Azure'
                        project_vre = 'No tags in Azure'
                    df = pd.DataFrame({'ResourceGroup': [resource_group], 'Resource': [resource.name]
                                       , 'ResourceId': [resource.id], 'ResourceKind': [resource.kind]
                                       , 'ResourceType': [resource.type]
                                       , 'BudgetCode': [budget_code], 'Project ID': [project_id]
                                       , 'Project Name': [project_name], 'Project VRE': [project_vre]
                                       , 'CreatedDate': [resource.created_time]})
                    df['CreatedDate'] = df['CreatedDate'].apply(lambda a: pd.to_datetime(a).date()) 
                    df_r = pd.concat([df_r, df], ignore_index=True)
            # HttpResponseError Code: 429, Message: Too many requests. Please retry. 
            # If received then wait 15 seconds and try again (within the While loop)
            except HttpResponseError as e:
                if e.status_code == 429:
                    sleep(15)
                    continue
            # Break out of the While loop
            break
    return df_r

####################################################################
####################################################################
