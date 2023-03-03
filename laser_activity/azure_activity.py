from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.monitor import MonitorManagementClient
from azure.mgmt.costmanagement import CostManagementClient
import pandas as pd
from datetime import datetime, timedelta #, date, timezone
import os

starttime = datetime.now()
print("Started: " + str(starttime))

credential = AzureCliCredential()

##VSE
#subscription_id = "0ad3b8e0-8cb5-473b-bd46-82c19f2d66ea"
#LASER
subscription_id = "7bf8fea8-fa06-4796-a265-a90a3de4dc10"

resource_client = ResourceManagementClient(credential, subscription_id)
compute_client = ComputeManagementClient(credential, subscription_id)
storage_client = StorageManagementClient(credential, subscription_id)
monitor_client = MonitorManagementClient(credential, subscription_id)
costmanagement_client = CostManagementClient(credential)

def resourceGroups():
    # Retrieve the list of resource groups
    group_list = resource_client.resource_groups.list()
    rg_df = pd.DataFrame({})
    for group in list(group_list):
        if group.tags is not None:
            budget_code = group.tags.get('Budget Code', 'No budget code')
        else:
            budget_code = 'No tags in Azure'
        df = pd.DataFrame({'Resource Group': [group.name], 'Budget Code': [budget_code]})
        rg_df = pd.concat([rg_df, df], ignore_index=True)
    return rg_df

def resources():
    # Retrieve the list of resource groups
    group_list = resource_client.resource_groups.list()
    r_df = pd.DataFrame({})
    for group in group_list:
        resource_group = group.name
        # Retrieve the list of resources in group
        # The expand argument includes additional properties in the output.
        resource_list = resource_client.resources.list_by_resource_group(
            resource_group, expand = "createdTime,changedTime,tags")
        for resource in list(resource_list):
            if resource.tags is not None:
                budget_code = resource.tags.get('Budget Code', 'No budget code in Azure')
            else:
                budget_code = 'No tags in Azure'
            df = pd.DataFrame({'ResourceGroup': [resource_group], 'Resource': [resource.name], 'ResourceId': [resource.id], 'ResourceKind': [resource.kind], 
                'ResourceType': [resource.type], 'BudgetCode': [budget_code], 'CreatedDate': [resource.created_time]})
            df['CreatedDate'] = df['CreatedDate'].apply(lambda a: pd.to_datetime(a).date()) 
            r_df = pd.concat([r_df, df], ignore_index=True)
    return r_df

def virtualMachines():
    # Retrieve the list of resource groups
    group_list = resource_client.resource_groups.list()
    v_df = pd.DataFrame({})
    for group in group_list:
        vm_list = compute_client.virtual_machines.list(group.name)
        for vm in vm_list:
            df = pd.DataFrame({'ResourceGroup': [group.name], 'vm_hardware_profile': [vm.hardware_profile.vm_size], 'vmName': [vm.name]}) 
            v_df = pd.concat([v_df, df], ignore_index=True)
    return v_df

def storage():
    # Retrieve the list of resource groups
    group_list = resource_client.resource_groups.list()
    s_df = pd.DataFrame({})
    for group in group_list:
        s_list = storage_client.storage_accounts.list_by_resource_group(group.name)
        for s in s_list:
            count_used_storage = 0
            metrics_data = monitor_client.metrics.list(s.id)
            for item in metrics_data.value:
                for timeserie in item.timeseries:
                    for data in timeserie.data:
                        try:
                            count_used_storage = count_used_storage + data.average
                        except:
                            pass
            df = pd.DataFrame({'ResourceGroup': [group.name], 'StorageAccount': [s.name], 'AverageStorageUsed(GB)': [count_used_storage/1000000000], 'Id': [s.id]})
            s_df = pd.concat([s_df, df], ignore_index=True)
    return s_df

def costs(datefrom, dateto):
    group_list = resource_client.resource_groups.list()
    c_df = pd.DataFrame({})
    for group in group_list:
        resource_group = group.name
        resource_cost = costmanagement_client.query.usage(
            # uri parameter (https://learn.microsoft.com/en-us/rest/api/cost-management/query/usage?tabs=HTTP#uri-parameters)
            scope = f"subscriptions/{subscription_id}/resourceGroups/{resource_group}",
            # request body (https://learn.microsoft.com/en-us/rest/api/cost-management/query/usage?tabs=HTTP#request-body)
            parameters={
                "dataset": {
                    "aggregation": {"totalCost": {"function": "Sum", "name": "PreTaxCost"}},
                    "granularity": "Daily",
                    "grouping": [{"name": "ResourceId", "type": "Dimension"}, {"name": "Budget Code", "type": "TagKey"}],
                },
                "TimePeriod": {"from": datefrom, "to": dateto},
                "timeframe": "Custom",
                "type": "ActualCost",
            },
        )
        c_df = pd.concat([c_df, pd.DataFrame(resource_cost.rows)], ignore_index=True)
    c_df.columns = ['PreTaxCost', 'UsageDate', 'ResourceId', 'TagKey', 'TagValue', 'Currency']
    return c_df

def writeToExcel(datefrom, dateto):
    with pd.ExcelWriter('Resources & Costs v5.xlsx') as excel:
        resourceGroups().to_excel(excel, sheet_name='resourceGroups', index=False)
        resources().to_excel(excel, sheet_name='resources', index=False)
        virtualMachines().to_excel(excel, sheet_name='virtualMachines', index=False)
        storage().to_excel(excel, sheet_name='storage', index=False)
        costs(datefrom, dateto).to_excel(excel, sheet_name='costs', index=False)

def writeToCsv_Yesterday():
    yesterday = datetime.now() - timedelta(1)
    year = yesterday.year
    month = str('00' + str(yesterday.month))[-2:]
    day = str('00' + str(yesterday.day))[-2:]
    
    directory = f"activity/{year}/{month}"
    if not os.path.exists(directory):
        os.makedirs(directory)

    resourceGroups().to_csv(f"{directory}/{year}-{month}-{day} resourceGroups.csv", index=False)
    resources().to_csv(f"{directory}/{year}-{month}-{day} resources.csv", index=False)
    virtualMachines().to_csv(f"{directory}/{year}-{month}-{day} virtualMachines.csv", index=False)
    storage().to_csv(f"{directory}/{year}-{month}-{day} storage.csv", index=False)
    costs(yesterday, yesterday).to_csv(f"{directory}/{year}-{month}-{day} costs.csv", index=False)

def writeToCsv_MonthToDate():
    today = datetime.now()
    year = today.year
    month = str('00' + str(today.month))[-2:]
    monthstart = pd.to_datetime(f"{year}-{month}-01")

    directory = f"activity/{year}/{month}"
    if not os.path.exists(directory):
        os.makedirs(directory)

    resourceGroups().to_csv(f"{directory}/{year}-{month} resourceGroups.csv", index=False)
    resources().to_csv(f"{directory}/{year}-{month} resources.csv", index=False)
    virtualMachines().to_csv(f"{directory}/{year}-{month} virtualMachines.csv", index=False)
    storage().to_csv(f"{directory}/{year}-{month} storage.csv", index=False)
    costs(monthstart, today).to_csv(f"{directory}/{year}-{month} costs.csv", index=False)


#costs().to_csv('output.csv', index=False)


#df = virtualMachines()
#print(df.groupby(['ResourceGroup', 'vm_hardware_profile']).size())

#print(df[['ResourceType']].drop_duplicates())
#print(df[df['ResourceType'].str.contains('virtualMachines')])

#year_ago = date.today() - timedelta(days=365.25)

#rg = resources()
#rg_created = rg.loc[rg.groupby('ResourceGroup')['CreatedDate'].idxmin()]
#rg_thisyear = rg_created.loc[rg_created['CreatedDate'] <= str(year_ago)]
#rg_project = rg_thisyear[rg_thisyear['ResourceGroup'].str.contains('LRDP-p|LRDP-s0')].reset_index(drop=True)

#print(rg_project[['ResourceGroup', 'CreatedDate']])




endtime = datetime.now()
Timetaken = endtime - starttime
print(f"Completed: {str(endtime)} \nTime taken: {str(Timetaken)}")