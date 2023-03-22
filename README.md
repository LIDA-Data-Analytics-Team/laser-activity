# LASER Activity

Azure Function App used to run Python scripts that insert LASER usage costs into SQL Database.

Scheduled to run once every a day at 0700.

## What it does

There are several functions within the function app, each responsible for fetching specific activity data and populating a database. 

SubscriptionId, SQL Server & Database are all hard coded rather than parameterised. This was a design choice made because we'll only be using this to collect activity data from LASER into Prism.  

- [LASER Costs](#laser-costs) : costs accrued by each resource in LASER
- [LASER Resources](#laser-resources) : Resource Groups and Resources, along with useful tags for each
- [LASER VMs](#laser-vms) : VM sizes and Start/Stop event times, along with who initiated them
- LASER Users
- LASER Budgets

### LASER Costs

[Cost Management and Usage Data Updates & Retention](https://learn.microsoft.com/en-us/azure/cost-management-billing/costs/understand-cost-mgt-data#cost-and-usage-data-updates-and-retention)  

Important points from the above link:  

> Azure finalizes or closes the current billing period up to 72 hours (three calendar days) after the billing period ends.

> During the open month (uninvoiced) period, cost management data should be considered an estimate only. In some cases, charges may be latent in arriving to the system after the usage actually occurred.

Because costs are mutable until three days after the monthly billing period has closed, the function:
- iterates through the last 35 days, pulling data from the Cost Management API one day at a time at subscription scope
- compares each record with those already present in the database
	- matches records using [UsageDate], [ResourceGroup], [ResourceId], [Meter], [MeterSubCategory], [MeterCategory], [TagKey], [TagValue] 
- inserts any not present direct to [dbo].[tblUsageCosts]
- updates any records already present but with a different [PreTaxCost] 
	- truncates staging table
	- inserts to staging table [stg].[UsageCostsUpdate]
	- updates records in [dbo].[tblUsageCosts] from [stg].[UsageCostsUpdate] on SQL database using primary key [UsageCostsId] of existing record  

### LASER Resources

First compares Resource Groups and then Resources returned by Azure SDK (azure.mgmt.resource) with those already present in the SQL database.  
- [ResourceGroup] as a unique identifier for Resource Groups  
    - inserted to [dbo].[tblLaserResources]
- [ResourceID] as a unique identifier for Resources  
    - inserted to [dbo].[tblLaserResourceGroups]

Unfortunately I can't see that Azure Resource Management maintains a historic record of Resource Groups and Resources, but by treating Resource Groups and Resources as Type 2 Slowly Changing Dimensions in the database we can maintain a history of a VRE.  

```mermaid
graph TD
    A[Resource Group/Resource] --> |Present in database <br>and not in Azure| B(Logically delete database record)
    A --> |Present in database <br>and in Azure| C{Changed values?}
    A --> |Present in Azure <br>and not in database| D(Insert new record)
    C --> |Yes| G(Logically delete existing record <br>and insert new record)   
    C -->|No| H(Do nothing)
```

### LASER VMs 

Similar to Resoures, Azure doesn't appear to maintain historic records of Virtual Machines, so we pull their hardware profile size (eg 'Standard_D4s_v4' etc.) each day from Azure and update the database as a Type 2 Slowly Changing Dimension (same logic as Resources & Resource Groups above).  
- inserted to [db].[tblLaserVmSizes]

Then for each VM ResourceID, gets yesterday's Start and Stop activity data:
- Checks against records that may be already present in the database for the same event date and event id
- Writes to the database all records not already present  
- inserted to [db].[tblLaserVmActivity]  

Microsoft only retain activity data in Azure for 90 days:  
[Activity log retention period](https://learn.microsoft.com/en-us/azure/azure-monitor/essentials/activity-log?tabs=powershell#retention-period)


## Permissions

The Azure Logic App uses System Managed Identity to authenticate against the resources it interacts with. 

It requires membership to the following roles:  
|Scope|Role|
|---|---|
|Subscription|Reader|
|Azure SQL Database|db_datareader <br>db_datawriter <br>db_ddladmin|

