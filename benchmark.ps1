$ErrorActionPreference = "Continue"

# benchmark.ps1 - Automated benchmark suite for the DKV cluster.
# Starts a 3-node cluster, runs PUT@10/50/100, GET@50, and a
# leader-failover test, then writes benchmark_results.md.

$PROJECT_ROOT = (Get-Item -Path ".\").FullName
$DATA_DIR     = Join-Path $PROJECT_ROOT "cluster-data"
$SERVER_BIN   = Join-Path $PROJECT_ROOT "modules\server-node\build\install\server-node\bin\server-node.bat"
$RESULTS_FILE = Join-Path $PROJECT_ROOT "benchmark_results.md"
$DURATION_SEC = 30

function Start-DkvCluster {
    Write-Host "`n=== Cleaning data directories ==="
    if (Test-Path $DATA_DIR) { Remove-Item $DATA_DIR -Recurse -Force }
    New-Item -ItemType Directory -Force -Path "$DATA_DIR\node1" | Out-Null
    New-Item -ItemType Directory -Force -Path "$DATA_DIR\node2" | Out-Null
    New-Item -ItemType Directory -Force -Path "$DATA_DIR\node3" | Out-Null

    Write-Host "=== Starting 3-node cluster ==="
    $script:p1 = Start-Process -FilePath $SERVER_BIN `
        -ArgumentList "node1","8081","$DATA_DIR\node1","node2:localhost:8082,node3:localhost:8083" `
        -WindowStyle Hidden -PassThru
    $script:p1.Id | Out-File "$DATA_DIR\node1.pid"

    $script:p2 = Start-Process -FilePath $SERVER_BIN `
        -ArgumentList "node2","8082","$DATA_DIR\node2","node1:localhost:8081,node3:localhost:8083" `
        -WindowStyle Hidden -PassThru
    $script:p2.Id | Out-File "$DATA_DIR\node2.pid"

    $script:p3 = Start-Process -FilePath $SERVER_BIN `
        -ArgumentList "node3","8083","$DATA_DIR\node3","node1:localhost:8081,node2:localhost:8082" `
        -WindowStyle Hidden -PassThru
    $script:p3.Id | Out-File "$DATA_DIR\node3.pid"

    Write-Host "Waiting for cluster to elect a leader..."
    Start-Sleep -Seconds 5
}

function Stop-DkvCluster {
    Write-Host "`n=== Stopping cluster ==="
    foreach ($node in @("node1","node2","node3")) {
        $pidFile = "$DATA_DIR\$node.pid"
        if (Test-Path $pidFile) {
            $pidToKill = Get-Content $pidFile
            try { Stop-Process -Id $pidToKill -Force -ErrorAction SilentlyContinue
                  Write-Host "  Killed $node (PID=$pidToKill)"
            } catch {}
            Remove-Item $pidFile -Force
        }
    }
    Start-Sleep -Seconds 2
}

function Get-LeaderInfo {
    for ($attempt = 0; $attempt -lt 10; $attempt++) {
        foreach ($port in @(8081,8082,8083)) {
            try {
                $metricsPort = $port + 1000
                $resp = Invoke-RestMethod -Uri "http://localhost:${metricsPort}/metrics" `
                         -TimeoutSec 2 -ErrorAction SilentlyContinue
                if ($resp.state -eq "LEADER") {
                    return @{ Port = $port; NodeId = $resp.nodeId; Term = $resp.term }
                }
            } catch {}
        }
        Start-Sleep -Seconds 1
    }
    return $null
}

function Invoke-Benchmark {
    param(
        [string]$Operation,
        [int]$Threads,
        [int]$Duration = $DURATION_SEC,
        [int]$ValueSize = 100
    )

    $seeds = "localhost:8081,localhost:8082,localhost:8083"
    Write-Host "`n>>> Benchmark: $Operation @ $Threads threads, ${Duration}s"

    $output = & "$PROJECT_ROOT\gradlew.bat" -q ":modules:client-java:benchmark" `
        --args="$seeds $Operation $Threads $Duration $ValueSize" 2>&1 |
        ForEach-Object { if ($_ -is [System.Management.Automation.ErrorRecord]) { $_.ToString() } else { $_ } } |
        Out-String

    Write-Host $output

    $csvLine = ($output -split "`n") | Where-Object { $_ -match "^BENCHMARK_CSV," } | Select-Object -First 1
    if (-not $csvLine) {
        Write-Host "WARNING: No BENCHMARK_CSV line found"
        return $null
    }

    $parts = $csvLine.Trim() -split ","
    return @{
        Operation  = $parts[1]
        Threads    = $parts[2]
        TotalOps   = $parts[3]
        Throughput  = $parts[4]
        P50        = $parts[5]
        P95        = $parts[6]
        P99        = $parts[7]
        Min        = $parts[8]
        Max        = $parts[9]
        Errors     = $parts[10]
    }
}

function Stop-LeaderNode {
    param([int]$LeaderPort)
    $nodeNum = $LeaderPort - 8080
    $node = "node$nodeNum"
    $pidFile = "$DATA_DIR\$node.pid"
    if (Test-Path $pidFile) {
        $pidToKill = Get-Content $pidFile
        try {
            Stop-Process -Id $pidToKill -Force -ErrorAction SilentlyContinue
            Write-Host "  Killed leader $node (PID=$pidToKill, port=$LeaderPort)"
        } catch {}
        Remove-Item $pidFile -Force
    }
}

function Get-MachineSpecs {
    try {
        $cpu = (Get-CimInstance Win32_Processor | Select-Object -First 1).Name.Trim()
        $ramBytes = (Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory
        $ramGB = [math]::Round($ramBytes / 1GB, 0)
        return "$cpu, ${ramGB} GB RAM"
    } catch {
        return "(could not detect hardware)"
    }
}

function Format-ResultRow {
    param($r)
    $p = [char]0x7C  # pipe character
    return "$p $($r.Operation) $p $($r.Threads) $p $($r.Throughput) $p $($r.P50) $p $($r.P95) $p $($r.P99) $p $($r.Min) $p $($r.Max) $p $($r.Errors) $p"
}

function Get-TableHeader {
    $p = [char]0x7C  # pipe character
    $d = [string]::new('-', 11)
    $header = "$p Operation $p Concurrency $p Throughput (ops/sec) $p p50 (ms) $p p95 (ms) $p p99 (ms) $p Min (ms) $p Max (ms) $p Errors $p"
    $sep = "$p$d$p$([string]::new('-',13))$p$([string]::new('-',22))$p$([string]::new('-',10))$p$([string]::new('-',10))$p$([string]::new('-',10))$p$([string]::new('-',10))$p$([string]::new('-',10))$p$([string]::new('-',8))$p"
    return @($header, $sep)
}

# ===== MAIN =====

Write-Host "=== DKV CLUSTER BENCHMARK SUITE ==="

# 1. Build
Write-Host "`n=== Building project ==="
& "$PROJECT_ROOT\gradlew.bat" ":modules:server-node:installDist" ":modules:client-java:installDist" --quiet
if ($LASTEXITCODE -ne 0) {
    Write-Host "BUILD FAILED" -ForegroundColor Red
    exit 1
}

$allResults = @()

# 2. Start cluster
Start-DkvCluster

$leader = Get-LeaderInfo
if (-not $leader) {
    Write-Host "ERROR: No leader elected after 10 seconds" -ForegroundColor Red
    Stop-DkvCluster
    exit 1
}
Write-Host "Leader detected: $($leader.NodeId) on port $($leader.Port), term=$($leader.Term)"

# 3. Steady-state benchmarks
Write-Host "`n=========================================="
Write-Host "  STEADY-STATE BENCHMARKS"
Write-Host "=========================================="

$putResult10  = Invoke-Benchmark -Operation "PUT" -Threads 10
$putResult50  = Invoke-Benchmark -Operation "PUT" -Threads 50
$putResult100 = Invoke-Benchmark -Operation "PUT" -Threads 100

# 4. GET benchmark at 50 threads
$getResult50  = Invoke-Benchmark -Operation "GET" -Threads 50

$allResults += $putResult10
$allResults += $putResult50
$allResults += $putResult100
$allResults += $getResult50

# 5. Leader failover benchmark
Write-Host "`n=========================================="
Write-Host "  LEADER FAILOVER BENCHMARK"
Write-Host "=========================================="

Stop-DkvCluster
Start-DkvCluster

$leader = Get-LeaderInfo
if (-not $leader) {
    Write-Host "ERROR: No leader elected" -ForegroundColor Red
    Stop-DkvCluster
    exit 1
}
Write-Host "Leader for failover test: $($leader.NodeId) on port $($leader.Port)"

$steadyP99 = if ($putResult50) { $putResult50.P99 } else { "N/A" }

$seeds = "localhost:8081,localhost:8082,localhost:8083"
$benchJob = Start-Job -ScriptBlock {
    param($ProjectRoot, $Seeds, $Duration)
    Set-Location $ProjectRoot
    & "$ProjectRoot\gradlew.bat" -q ":modules:client-java:benchmark" `
        --args="$Seeds PUT 50 $Duration 100" 2>&1
} -ArgumentList $PROJECT_ROOT, $seeds, $DURATION_SEC

$halfDuration = [math]::Floor($DURATION_SEC / 2)
Write-Host "Waiting $halfDuration seconds before killing leader..."
Start-Sleep -Seconds $halfDuration

Write-Host ">>> Killing leader to trigger failover"
Stop-LeaderNode -LeaderPort $leader.Port

Write-Host "Waiting for benchmark to complete..."
$benchOutput = Receive-Job -Job $benchJob -Wait | Out-String
Remove-Job $benchJob -Force -ErrorAction SilentlyContinue

Write-Host $benchOutput

$csvLine = ($benchOutput -split "`n") | Where-Object { $_ -match "^BENCHMARK_CSV," } | Select-Object -First 1
$failoverResult = $null
if ($csvLine) {
    $parts = $csvLine.Trim() -split ","
    $failoverResult = @{
        Operation  = "PUT*"
        Threads    = $parts[2]
        TotalOps   = $parts[3]
        Throughput  = $parts[4]
        P50        = $parts[5]
        P95        = $parts[6]
        P99        = $parts[7]
        Min        = $parts[8]
        Max        = $parts[9]
        Errors     = $parts[10]
    }
}

# 6. Collect machine specs
$machineSpecs = Get-MachineSpecs
Write-Host "`nMachine: $machineSpecs"

# 7. Write benchmark_results.md
Write-Host "`n=== Writing benchmark_results.md ==="

$tblHdr = Get-TableHeader
$lines = [System.Collections.ArrayList]::new()

[void]$lines.Add('# Benchmark Results')
[void]$lines.Add('')
[void]$lines.Add("**Date:** $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')")
[void]$lines.Add("**Machine:** $machineSpecs")
[void]$lines.Add('**Configuration:** 3-node cluster, all on localhost')
[void]$lines.Add("**Duration:** ${DURATION_SEC}s per benchmark run")
[void]$lines.Add('**Value size:** 100 bytes')
[void]$lines.Add('')
[void]$lines.Add('---')
[void]$lines.Add('')
[void]$lines.Add('## Steady-State Performance')
[void]$lines.Add('')
[void]$lines.Add($tblHdr[0])
[void]$lines.Add($tblHdr[1])

foreach ($r in $allResults) {
    if ($r) {
        [void]$lines.Add((Format-ResultRow -r $r))
    }
}

[void]$lines.Add('')
[void]$lines.Add('## Leader Failover')
[void]$lines.Add('')
[void]$lines.Add("Leader was killed at the ${halfDuration}-second mark of a ${DURATION_SEC}s PUT benchmark at 50 threads.")
[void]$lines.Add('')

if ($failoverResult) {
    [void]$lines.Add($tblHdr[0])
    [void]$lines.Add($tblHdr[1])
    [void]$lines.Add((Format-ResultRow -r $failoverResult))
    [void]$lines.Add('')
    [void]$lines.Add('*\* = leader killed mid-run*')
    [void]$lines.Add('')
    [void]$lines.Add("**Steady-state p99:** ${steadyP99} ms")
    [void]$lines.Add("**Failover p99:** $($failoverResult.P99) ms")
} else {
    [void]$lines.Add('Failover benchmark data was not captured.')
}

[void]$lines.Add('')
[void]$lines.Add('---')
[void]$lines.Add('')
$gt = [char]0x3E  # > character
[void]$lines.Add("$gt **Disclaimer:** Tested on $machineSpecs with all 3 nodes running on localhost.")
[void]$lines.Add("$gt Numbers measure protocol overhead (Raft consensus + LSM storage), not network")
[void]$lines.Add("$gt latency. Real-world performance across a network will be dominated by round-trip")
[void]$lines.Add("$gt time rather than these protocol costs.")

$lines -join "`r`n" | Out-File -FilePath $RESULTS_FILE -Encoding utf8
Write-Host "Results written to $RESULTS_FILE"

# 8. Cleanup
Stop-DkvCluster

Write-Host "`n=== BENCHMARK SUITE COMPLETE ==="
Write-Host "Results: $RESULTS_FILE"
exit 0
