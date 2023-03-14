Azure Function App used to run Python scripts that insert LASER usage costs into SQL Database.

Scheduled to run once every a day at 0700.

https://learn.microsoft.com/en-us/azure/cost-management-billing/costs/understand-cost-mgt-data#cost-and-usage-data-updates-and-retention
Important points:
- Azure finalizes or closes the current billing period up to 72 hours (three calendar days) after the billing period ends.
- During the open month (uninvoiced) period, cost management data should be considered an estimate only. In some cases, charges may be latent in arriving to the system after the usage actually occurred.

For this reason the script:
- pulls data from the Cost Management API for the last 35 days
- compares each record with those already present in the database
	- matches records using [UsageDate], [ResourceGroup], [ResourceId], [Meter], [MeterSubCategory], [MeterCategory], [TagKey], [TagValue] 
- inserts any not present direct to [dbo].[tblUsageCosts]
- updates any records already present but with a different [PreTaxCost] 
	- truncates staging table
	- inserts to staging table [stg].[UsageCostsUpdate]
	- using primary key [UsageCostsId] of existing record 
	- updates [dbo].[tblUsageCosts] from [stg].[UsageCostsUpdate]

