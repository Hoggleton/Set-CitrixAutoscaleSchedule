# Set-CitrixAutoscaleSchedule

A PowerShell script that automatically configures Citrix DaaS Autoscale schedules based on real historical usage data — no manual schedule tuning required.

## Overview

Citrix DaaS Autoscale can save significant Azure compute costs, but configuring accurate schedules manually is time-consuming and often based on guesswork. This script closes that loop by pulling the actual hourly machine usage data that Citrix already collects, averaging it into a typical week profile per Delivery Group, and writing it directly back as the Autoscale schedule — all via the Citrix DaaS REST API.

**No Delivery Controller required. No Broker snap-in. No CSV exports. Just run it.**

## How It Works

For each matched Delivery Group the script:

1. Pulls hourly machine usage from `DeliveryGroups/{id}/Usage` — the same data source used by Autoscale Insights in Citrix Studio
2. Averages usage by day-of-week and hour to build a typical week profile
3. Classifies hours at or above a configurable threshold as **Peak**
4. Reduces the pool size by the buffer percentage so that `pool + buffer = actual demand` (rather than over-provisioning on top of demand)
5. Converts the hourly arrays into `PoolSizeSchedule` and `PeakTimeRanges` time range format as required by the API
6. PATCHes the Weekdays and Weekend Power Time Schemes for each DG in a single pass

## Requirements

- PowerShell 5.1 or later
- A Citrix Cloud API client with **read-write** access to Delivery Groups
  - Create one at: **Citrix Cloud → Identity & Access Management → API Access**
  - The API client needs at minimum Full Administrator or a custom role with Delivery Group edit permissions
- Power Time Schemes (Weekdays / Weekend) must already exist for each Delivery Group in Citrix Studio — the script updates them, it does not create them

## Parameters

| Parameter | Default | Description |
|---|---|---|
| `CustomerId` | *(required)* | Citrix Cloud customer ID — found on the IAM page |
| `ClientId` | *(required)* | API client ID |
| `ClientSecret` | *(required)* | API client secret |
| `Filter` | `*` | Wildcard filter on DG name. Omit to process all DGs |
| `PeakThresholdPct` | `70` | Hours at or above this % of the DG's peak average are classified as Peak |
| `MinMachines` | `2` | Minimum machines to keep powered on during off-peak |
| `PeakBuffer` | `10` | Peak capacity buffer % applied on top of pool size |
| `OffPeakBuffer` | `5` | Off-peak capacity buffer % applied on top of pool size |
| `WhatIf` | `false` | Preview what would be applied without making any changes |

## Usage

### Preview changes (always run this first)

```powershell
.\Set-CitrixAutoscaleSchedule.ps1 `
  -CustomerId "your-customer-id" `
  -ClientId   "your-client-id" `
  -ClientSecret "your-client-secret" `
  -Filter "*Windows11MS*" `
  -WhatIf
```

### Apply to all Windows 11 Multi-Session DGs

```powershell
.\Set-CitrixAutoscaleSchedule.ps1 `
  -CustomerId "your-customer-id" `
  -ClientId   "your-client-id" `
  -ClientSecret "your-client-secret" `
  -Filter "*Windows11MS*"
```

### Apply to a specific region only

```powershell
.\Set-CitrixAutoscaleSchedule.ps1 `
  -CustomerId "your-customer-id" `
  -ClientId   "your-client-id" `
  -ClientSecret "your-client-secret" `
  -Filter "*AzureEUW*Windows11MS*"
```

### Apply to all DGs with a custom threshold

```powershell
.\Set-CitrixAutoscaleSchedule.ps1 `
  -CustomerId "your-customer-id" `
  -ClientId   "your-client-id" `
  -ClientSecret "your-client-secret" `
  -PeakThresholdPct 75 `
  -MinMachines 3
```

## Understanding the Output

```
━━ DG-AzureEUW-Windows11MS-LD-Standard ━━
  167 hourly records retrieved.
  Peak avg: 126 machines | Threshold: 89 machines (70%)
  Weekdays   Pool: 2-99 machines | Peak: 9:00-17:00
  Weekend    Pool: 2-2 machines  | Peak: No peak hours
  Autoscale enabled.
  Weekdays: applied.
  Weekend: applied.
```

- **Peak avg** — highest average machine usage across all hours in the dataset
- **Threshold** — machines at or above this value are classified as Peak hours
- **Pool** — min and max machines scheduled across the day (already reduced by buffer %)
- **Peak** — hours classified as Peak based on the threshold

## Buffer Calculation

The buffer percentage is subtracted from the pool size rather than added on top, so that:

```
Pool Size (scheduled) = Average Usage × (1 - Buffer%)
Autoscale Buffer      = Buffer% on top of pool
Total Capacity        ≈ Average Usage
```

For example, with a peak average of 100 machines and a 10% peak buffer:
- Pool size scheduled = `100 × 0.90 = 90 machines`
- Autoscale adds 10% buffer = 9 machines
- Total capacity = 99 machines ≈ actual demand

## Automating with Azure Automation

To keep schedules aligned with evolving usage patterns, run this script on a weekly schedule via Azure Automation:

1. Create an Azure Automation account
2. Create a PowerShell Runbook and paste the script contents
3. Store `CustomerId`, `ClientId`, and `ClientSecret` as encrypted Automation Variables or use a Credential asset
4. Schedule the runbook to run weekly (e.g. Sunday night before the working week)

## Notes

- The `/Usage` endpoint returns a rolling window of approximately 7 days of hourly data. This is the same data Citrix Studio displays in the **Autoscale Insights** tab.
- Each Delivery Group is processed independently — DGs in different regions or with different personas will naturally produce different schedules reflecting their actual usage patterns.
- DGs with very low usage (peak average of 2 machines) will be scheduled at `MinMachines` throughout. Consider whether Autoscale is necessary for these groups.
- Test DGs and lightly used groups will be included if they match the filter — use a more specific filter or review the WhatIf output before applying broadly.
- The script requires **read-write** API credentials to apply changes. Consider using a separate read-only client for auditing and the read-write client only when applying.

## API Endpoints Used

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `/cctrustoauth2/{customerId}/tokens/clients` | Authenticate |
| `GET` | `/cvad/manage/me` | Resolve Site ID |
| `GET` | `/cvad/manage/DeliveryGroups` | Enumerate DGs |
| `GET` | `/cvad/manage/DeliveryGroups/{id}/Usage` | Fetch hourly usage |
| `GET` | `/cvad/manage/DeliveryGroups/{id}/PowerTimeSchemes` | Get existing schemes |
| `PATCH` | `/cvad/manage/DeliveryGroups/{id}` | Enable Autoscale + set buffers |
| `PATCH` | `/cvad/manage/DeliveryGroups/{id}/PowerTimeSchemes/{id}` | Apply schedule |

## License

MIT
