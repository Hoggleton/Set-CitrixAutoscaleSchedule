<#
.SYNOPSIS
  Pulls Citrix DaaS Usage data and applies it directly as Autoscale schedules.
.PARAMETER CustomerId      Citrix Cloud customer ID
.PARAMETER ClientId        API client ID (read-write)
.PARAMETER ClientSecret    API client secret
.PARAMETER Filter          Wildcard DG name filter (default: * = all)
.PARAMETER PeakThresholdPct  Hours at or above this % of peak are classified as PEAK (default: 70)
.PARAMETER MinMachines     Minimum machines in off-peak (default: 2)
.PARAMETER PeakBuffer      Peak capacity buffer % (default: 10)
.PARAMETER OffPeakBuffer   Off-peak capacity buffer % (default: 5)
.PARAMETER WhatIf          Preview changes without applying
#>
param(
  [Parameter(Mandatory)][string]$CustomerId,
  [Parameter(Mandatory)][string]$ClientId,
  [Parameter(Mandatory)][string]$ClientSecret,
  [string]$Filter          = "*",
  [int]$PeakThresholdPct   = 70,
  [int]$MinMachines        = 2,
  [int]$PeakBuffer         = 10,
  [int]$OffPeakBuffer      = 5,
  [switch]$WhatIf
)
$ErrorActionPreference = "Stop"

# ── Authenticate ──────────────────────────────────────────────────────────────
Write-Host "`nAuthenticating to Citrix Cloud..." -ForegroundColor Cyan
$tok = (Invoke-RestMethod -Method Post `
  -Uri "https://api.cloud.com/cctrustoauth2/$CustomerId/tokens/clients" `
  -Body @{ grant_type="client_credentials"; client_id=$ClientId; client_secret=$ClientSecret }
).access_token

# ── Resolve Site ID ───────────────────────────────────────────────────────────
$h0     = @{ Authorization="CwsAuth Bearer=$tok"; "Citrix-CustomerId"=$CustomerId }
$me     = Invoke-RestMethod -Uri "https://api.cloud.com/cvad/manage/me" -Headers $h0
$siteId = ($me.Customers | Where-Object { $_.Id -eq $CustomerId }).Sites[0].Id
if (-not $siteId) { throw "Could not resolve Site ID for customer: $CustomerId" }
Write-Host "  Site ID: $siteId"

$hdrs = @{
  Authorization        = "CwsAuth Bearer=$tok"
  "Citrix-CustomerId"  = $CustomerId
  "Citrix-InstanceId"  = $siteId
  "Content-Type"       = "application/json"
}
$base = "https://api.cloud.com/cvad/manage"

# ── Enumerate Delivery Groups ─────────────────────────────────────────────────
Write-Host "Discovering Delivery Groups (filter: $Filter)..." -ForegroundColor Cyan
$allDGs = [Collections.Generic.List[object]]::new()
$dgUri  = "$base/DeliveryGroups?limit=100"
do {
  $pg = Invoke-RestMethod -Uri $dgUri -Headers $hdrs
  if ($pg.Items) { $allDGs.AddRange([object[]]$pg.Items) }
  if ($pg.ContinuationToken) {
    $dgUri = "$base/DeliveryGroups?limit=100&continuationToken=$($pg.ContinuationToken)"
  } else { $dgUri = $null }
} while ($dgUri)

$targetDGs = $allDGs | Where-Object { $_.Name -like $Filter }
Write-Host "  Found $($allDGs.Count) total DGs, $($targetDGs.Count) match filter." -ForegroundColor Green
if ($targetDGs.Count -eq 0) { Write-Warning "No DGs matched filter [$Filter]. Exiting."; exit 1 }

if ($WhatIf) {
  Write-Host "`n  [WHATIF MODE] No changes will be applied.`n" -ForegroundColor Yellow
}

$summary = [Collections.Generic.List[object]]::new()

# ── Process each DG ──────────────────────────────────────────────────────────
foreach ($dg in $targetDGs) {
  Write-Host "`n━━ $($dg.Name) ━━" -ForegroundColor Cyan

  # Pull Usage data
  try {
    $usage = (Invoke-RestMethod -Uri "$base/DeliveryGroups/$($dg.Id)/Usage" -Headers $hdrs).Items
  } catch {
    Write-Warning "  Could not retrieve Usage data: $($_.Exception.Message)"
    continue
  }

  if (-not $usage -or $usage.Count -eq 0) {
    Write-Host "  No usage data available - skipping." -ForegroundColor DarkGray
    continue
  }
  Write-Host "  $($usage.Count) hourly records retrieved."

  # ── Average usage by DayOfWeek + Hour ────────────────────────────────────
  $sums   = @{}
  $counts = @{}
  foreach ($entry in $usage) {
    $ts  = [datetime]$entry.Time
    $key = "$($ts.DayOfWeek.value__)_$($ts.Hour)"
    if (-not $sums[$key])   { $sums[$key]   = 0 }
    if (-not $counts[$key]) { $counts[$key] = 0 }
    $sums[$key]   += [int]$entry.Usage
    $counts[$key] += 1
  }

  $avg = @{}
  for ($d = 0; $d -le 6; $d++) {
    $avg[$d] = @{}
    for ($h = 0; $h -le 23; $h++) {
      $key = "${d}_${h}"
      if ($counts[$key]) {
        $avg[$d][$h] = [math]::Ceiling($sums[$key] / $counts[$key])
      } else {
        $avg[$d][$h] = $MinMachines
      }
    }
  }

  $allAvgs   = for ($d=0;$d -le 6;$d++) { for ($h=0;$h -le 23;$h++) { $avg[$d][$h] } }
  $maxAvg    = ($allAvgs | Measure-Object -Maximum).Maximum
  $threshold = [math]::Ceiling($maxAvg * $PeakThresholdPct / 100)
  Write-Host "  Peak avg: $maxAvg machines | Threshold: $threshold machines ($PeakThresholdPct%)"

  # ── Build scheme arrays ───────────────────────────────────────────────────
  # Weekdays = Mon-Fri averaged, Weekend = Sat+Sun averaged
  $schemes = @(
    @{ Label='Weekdays'; DayNums=@(1,2,3,4,5); MatchDay='Monday'   },
    @{ Label='Weekend';  DayNums=@(6,0);        MatchDay='Saturday' }
  )

  foreach ($scheme in $schemes) {
    $peakHours = [bool[]]::new(24)
    $poolSize  = [int[]]::new(24)
    for ($h = 0; $h -le 23; $h++) {
      $vals    = $scheme.DayNums | ForEach-Object { $avg[$_][$h] }
      $slotAvg = [math]::Ceiling(($vals | Measure-Object -Average).Average)
      $bufPct  = if ($slotAvg -ge $threshold) { $PeakBuffer } else { $OffPeakBuffer }
      $pool    = [math]::Max($MinMachines, [math]::Floor($slotAvg * (1 - $bufPct / 100)))
      $poolSize[$h]  = $pool
      $peakHours[$h] = ($slotAvg -ge $threshold)
    }
    $scheme.PeakHours = $peakHours
    $scheme.PoolSize  = $poolSize

    $peakRanges = @(); $inPeak = $false; $peakStart = 0
    for ($h = 0; $h -le 24; $h++) {
      if ($h -lt 24 -and $peakHours[$h] -and -not $inPeak) { $inPeak=$true; $peakStart=$h }
      elseif (($h -eq 24 -or -not $peakHours[$h]) -and $inPeak) {
        $peakRanges += "$($peakStart):00-$($h):00"; $inPeak=$false
      }
    }
    $peakStr = if ($peakRanges) { $peakRanges -join ', ' } else { 'No peak hours' }
    $poolMin = ($poolSize | Measure-Object -Minimum).Minimum
    $poolMax = ($poolSize | Measure-Object -Maximum).Maximum
    Write-Host "  $($scheme.Label.PadRight(10)) Pool: $poolMin-$poolMax machines | Peak: $peakStr"
  }

  # ── Helper: convert hourly array to time range schedule ──────────────────
  function ConvertTo-TimeRanges {
    param([array]$HourlyValues, [bool]$IsBool)
    $ranges = [Collections.Generic.List[object]]::new()
    $start  = 0
    $cur    = $HourlyValues[0]
    for ($h = 1; $h -le 24; $h++) {
      $next = if ($h -lt 24) { $HourlyValues[$h] } else { $null }
      if ($h -eq 24 -or $next -ne $cur) {
        $startStr = "{0:D2}:00" -f $start
        $endStr   = if ($h -eq 24) { "00:00" } else { "{0:D2}:00" -f $h }
        if ($IsBool) {
          if ($cur) { $ranges.Add(@{ TimeRange="$startStr-$endStr" }) }
        } else {
          $ranges.Add(@{ TimeRange="$startStr-$endStr"; PoolSize=[int]$cur })
        }
        $start = $h
        $cur   = $next
      }
    }
    return $ranges.ToArray()
  }

  $pts = (Invoke-RestMethod -Uri "$base/DeliveryGroups/$($dg.Id)/PowerTimeSchemes" -Headers $hdrs).Items
  if (-not $pts) {
    Write-Warning "  No PowerTimeSchemes found — create Weekdays/Weekend in Studio first."
    continue
  }

  if ($WhatIf) {
    Write-Host "  [WHATIF] Would enable Autoscale (PeakBuffer: $PeakBuffer%, OffPeakBuffer: $OffPeakBuffer%)" -ForegroundColor Yellow
    foreach ($scheme in $schemes) {
      $match = $pts | Where-Object { $_.DaysOfWeek -contains $scheme.MatchDay }
      if ($match) {
        Write-Host "  [WHATIF] Would update $($scheme.Label) scheme (ID: $($match.Id))" -ForegroundColor Yellow
      } else {
        Write-Host "  [WHATIF] No scheme found containing $($scheme.MatchDay) — would skip" -ForegroundColor DarkYellow
      }
    }
    $summary.Add([PSCustomObject]@{ DG=$dg.Name; PeakMachines=$maxAvg; Threshold=$threshold; Status='WhatIf' })
    continue
  }

  # ── Enable Autoscale + buffers ────────────────────────────────────────────
  $dgBody = @{
    AutoscalingEnabled       = $true
    PeakBufferSizePercent    = $PeakBuffer
    OffPeakBufferSizePercent = $OffPeakBuffer
  } | ConvertTo-Json
  Invoke-RestMethod -Method Patch -Uri "$base/DeliveryGroups/$($dg.Id)" -Headers $hdrs -Body $dgBody | Out-Null
  Write-Host "  Autoscale enabled." -ForegroundColor Green

  # ── Apply each scheme ─────────────────────────────────────────────────────
  $applied = 0
  foreach ($scheme in $schemes) {
    $match = $pts | Where-Object { $_.DaysOfWeek -contains $scheme.MatchDay }
    if (-not $match) {
      Write-Warning "  No PowerTimeScheme found containing $($scheme.MatchDay) — skipping."
      continue
    }

    $schemeBody = @{
      PeakTimeRanges    = ConvertTo-TimeRanges -HourlyValues $scheme.PeakHours -IsBool $true
      PoolSizeSchedule  = ConvertTo-TimeRanges -HourlyValues $scheme.PoolSize  -IsBool $false
    } | ConvertTo-Json -Depth 5

    Write-Host "  Sending $($scheme.Label) (ID: $($match.Id)):" -ForegroundColor DarkGray
    Write-Host "    $schemeBody" -ForegroundColor DarkGray

    Invoke-RestMethod -Method Patch `
      -Uri "$base/DeliveryGroups/$($dg.Id)/PowerTimeSchemes/$($match.Id)" `
      -Headers $hdrs -Body $schemeBody | Out-Null
    Write-Host "  $($scheme.Label): applied." -ForegroundColor Green
    $applied++
  }

  $summary.Add([PSCustomObject]@{
    DG             = $dg.Name
    PeakMachines   = $maxAvg
    Threshold      = $threshold
    SchemesApplied = $applied
    Status         = 'Applied'
  })
}

# ── Summary ───────────────────────────────────────────────────────────────────
Write-Host "`n━━ Summary ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow
$summary | Format-Table -AutoSize
if (-not $WhatIf) {
  Write-Host "Done. Review schedules in Citrix Studio > Delivery Groups > Manage Autoscale." -ForegroundColor Green
}