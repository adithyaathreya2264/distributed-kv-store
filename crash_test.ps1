$ErrorActionPreference = "Stop"

$PROJECT_ROOT = (Get-Item -Path ".\").FullName
$DATA_DIR = Join-Path -Path $PROJECT_ROOT -ChildPath "cluster-data"
$SERVER_BIN = Join-Path -Path $PROJECT_ROOT -ChildPath "modules\server-node\build\install\server-node\bin\server-node.bat"

Write-Host "=== Building project ==="
Set-Location -Path $PROJECT_ROOT
.\gradlew.bat :modules:server-node:installDist :modules:client-java:installDist --quiet

Write-Host "=== Cleaning data directories ==="
if (Test-Path -Path $DATA_DIR) {
    Remove-Item -Path $DATA_DIR -Recurse -Force
}
New-Item -ItemType Directory -Force -Path (Join-Path -Path $DATA_DIR -ChildPath "node1") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path -Path $DATA_DIR -ChildPath "node2") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path -Path $DATA_DIR -ChildPath "node3") | Out-Null

function Start-Cluster {
    Write-Host "=== Starting cluster ==="
    
    $p1 = Start-Process -FilePath $SERVER_BIN -ArgumentList "node1", "8081", "$DATA_DIR\node1", "node2:localhost:8082,node3:localhost:8083" -WindowStyle Hidden -PassThru
    $p1.Id | Out-File -FilePath "$DATA_DIR\node1.pid"
    
    $p2 = Start-Process -FilePath $SERVER_BIN -ArgumentList "node2", "8082", "$DATA_DIR\node2", "node1:localhost:8081,node3:localhost:8083" -WindowStyle Hidden -PassThru
    $p2.Id | Out-File -FilePath "$DATA_DIR\node2.pid"
    
    $p3 = Start-Process -FilePath $SERVER_BIN -ArgumentList "node3", "8083", "$DATA_DIR\node3", "node1:localhost:8081,node2:localhost:8082" -WindowStyle Hidden -PassThru
    $p3.Id | Out-File -FilePath "$DATA_DIR\node3.pid"
    
    Write-Host "Waiting for cluster to elect a leader..."
    Start-Sleep -Seconds 3
}

function Kill-Cluster {
    Write-Host "=== Hard killing all nodes ==="
    foreach ($node in @("node1", "node2", "node3")) {
        $pidFile = "$DATA_DIR\$node.pid"
        if (Test-Path -Path $pidFile) {
            $pidToKill = Get-Content -Path $pidFile
            try {
                Stop-Process -Id $pidToKill -Force -ErrorAction SilentlyContinue
                Write-Host "  Killed $node (PID=$pidToKill)"
            } catch {
                # Ignore
            }
            Remove-Item -Path $pidFile -Force
        }
    }
}

# ---- Phase 1: Start, Write, Kill ----
Start-Cluster

Write-Host ""
Write-Host "=== Writing 10 keys ==="
$FAILED = 0
for ($i = 1; $i -le 10; $i++) {
    Write-Host "  PUT key$i = value$i"
    $success = $false
    
    # Try node1
    try {
        .\gradlew.bat -q :modules:client-java:run --args="localhost 8081 put key$i value$i" *>$null
        $success = $true
    } catch {}
    
    if (-not $success) {
        # Try node2
        try {
            .\gradlew.bat -q :modules:client-java:run --args="localhost 8082 put key$i value$i" *>$null
            $success = $true
        } catch {}
    }
    
    if (-not $success) {
        # Try node3
        try {
            .\gradlew.bat -q :modules:client-java:run --args="localhost 8083 put key$i value$i" *>$null
            $success = $true
        } catch {}
    }
    
    if (-not $success) {
        Write-Host "  FAILED to write key$i"
        $FAILED++
    }
}

Write-Host ""
Kill-Cluster

Write-Host ""
Write-Host "=== Waiting 2 seconds before restart ==="
Start-Sleep -Seconds 2

# ---- Phase 2: Restart and Verify ----
Start-Cluster

Write-Host ""
Write-Host "=== Reading back 10 keys ==="
$PASS = 0
$FAIL = 0
for ($i = 1; $i -le 10; $i++) {
    $result = ""
    
    foreach ($port in @(8081, 8082, 8083)) {
        try {
            # Capture output
            $result = .\gradlew.bat -q :modules:client-java:run --args="localhost $port get key$i"
            if ($LASTEXITCODE -eq 0 -and $result -ne $null) {
                break
            }
        } catch {}
    }

    if ($result -match "value$i") {
        Write-Host "  key$i = value$i ✓"
        $PASS++
    } else {
        Write-Host "  key$i MISSING or WRONG ✗ (got: $result)"
        $FAIL++
    }
}

Write-Host ""
Write-Host "=== Results: $PASS passed, $FAIL failed ==="

# Cleanup
Kill-Cluster

if ($FAIL -eq 0) {
    Write-Host "✓ CRASH RECOVERY TEST PASSED"
    exit 0
} else {
    Write-Host "✗ CRASH RECOVERY TEST FAILED"
    exit 1
}
