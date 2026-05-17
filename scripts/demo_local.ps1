param(
    [string]$Python = "python",
    [string]$Key = "demo",
    [string]$Payload = "hello world"
)

$ErrorActionPreference = "Stop"

$root = (Resolve-Path (Join-Path $PSScriptRoot ".."))
Set-Location $root

$logsDir = Join-Path $root "demo-logs"
if (Test-Path $logsDir) {
    Remove-Item -Recurse -Force $logsDir
}
New-Item -ItemType Directory -Path $logsDir | Out-Null

function Start-Node {
    param(
        [string]$NodeId,
        [int]$Port,
        [string[]]$Seeds,
        [string]$KeyCommand
    )

    $args = @(
        "-m", "dmq", "node",
        "--node-id", $NodeId,
        "--bind-host", "127.0.0.1",
        "--bind-port", "$Port",
        "--advertise-host", "127.0.0.1",
        "--advertise-port", "$Port",
        "--key-command", $KeyCommand,
        "--log-level", "INFO"
    )

    foreach ($seed in $Seeds) {
        $args += "--seed"
        $args += $seed
    }

    $outLog = Join-Path $logsDir "$NodeId.out.log"
    $errLog = Join-Path $logsDir "$NodeId.err.log"

    Write-Host "Starting $NodeId on port $Port ..."
    return Start-Process -FilePath $Python -ArgumentList $args -PassThru -RedirectStandardOutput $outLog -RedirectStandardError $errLog
}

function Run-ClientCmd {
    param([string[]]$ClientArgs)

    Write-Host ("dmq " + ($ClientArgs -join " "))
    & $Python @("-m", "dmq") @ClientArgs
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code $LASTEXITCODE"
    }
}

$node1 = $null
$node2 = $null
$node3 = $null

function Wait-ForPort {
    param(
        [int]$Port,
        [int]$TimeoutSeconds = 5
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $client = New-Object System.Net.Sockets.TcpClient
            $async = $client.BeginConnect("127.0.0.1", $Port, $null, $null)
            if ($async.AsyncWaitHandle.WaitOne(200)) {
                $client.EndConnect($async)
                $client.Close()
                return
            }
            $client.Close()
        }
        catch {
        }
        Start-Sleep -Milliseconds 150
    }

    throw "Port $Port did not open within $TimeoutSeconds seconds"
}

function Stop-NodeByPort {
    param([int]$Port)

    try {
        $conn = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction Stop | Select-Object -First 1
        if ($null -ne $conn) {
            try { Stop-Process -Id $conn.OwningProcess -Force -ErrorAction Stop } catch { }
        }
    }
    catch {
        # Get-NetTCPConnection might be unavailable; ignore cleanup failures.
    }
}

try {
    # Clean start: ensure no leftover nodes are listening.
    Stop-NodeByPort -Port 5001
    Stop-NodeByPort -Port 5002
    Stop-NodeByPort -Port 5003

    $node1 = Start-Node -NodeId "node1" -Port 5001 -Seeds @() -KeyCommand "$Key=upper"
    Wait-ForPort -Port 5001 -TimeoutSeconds 5
    $node2 = Start-Node -NodeId "node2" -Port 5002 -Seeds @("node1@127.0.0.1:5001") -KeyCommand "$Key=reverse"
    Wait-ForPort -Port 5002 -TimeoutSeconds 5
    $node3 = Start-Node -NodeId "node3" -Port 5003 -Seeds @("node2@127.0.0.1:5002", "node1@127.0.0.1:5001") -KeyCommand "$Key=length"
    Wait-ForPort -Port 5003 -TimeoutSeconds 5

    Write-Host "`n--- STEP 1: Subscribe node2 + node3 ---"
    Run-ClientCmd -ClientArgs @("subscribe", "--target", "node1@127.0.0.1:5001", "--subscriber", "node2@127.0.0.1:5002", "--key", $Key)
    Run-ClientCmd -ClientArgs @("subscribe", "--target", "node1@127.0.0.1:5001", "--subscriber", "node3@127.0.0.1:5003", "--key", $Key)

    Start-Sleep -Milliseconds 250

    Write-Host "`n--- STEP 2: Publish payload (expect delivered=2) ---"
    Run-ClientCmd -ClientArgs @("publish", "--target", "node1@127.0.0.1:5001", "--key", $Key, "--payload", $Payload)

    Start-Sleep -Seconds 1

    Write-Host "`n--- STEP 3: Unsubscribe node3 ---"
    Run-ClientCmd -ClientArgs @("unsubscribe", "--target", "node1@127.0.0.1:5001", "--subscriber", "node3@127.0.0.1:5003", "--key", $Key)

    Write-Host "`n--- STEP 4: Publish again (expect delivered=1) ---"
    Run-ClientCmd -ClientArgs @("publish", "--target", "node1@127.0.0.1:5001", "--key", $Key, "--payload", $Payload)

    Start-Sleep -Seconds 1

    Write-Host "`n--- STEP 5: Simulate node2 crash ---"
    Stop-NodeByPort -Port 5002
    $node2 = $null

    Start-Sleep -Milliseconds 400

    Write-Host "`n--- STEP 6: Publish after crash (expect delivered=0 + cleanup) ---"
    Run-ClientCmd -ClientArgs @("publish", "--target", "node1@127.0.0.1:5001", "--key", $Key, "--payload", $Payload)

    Start-Sleep -Seconds 1

    Write-Host "`n--- LOG TAILS (Processed deliver lines) ---"
    foreach ($n in @("node1", "node2", "node3")) {
        $path = Join-Path $logsDir "$n.err.log"
        if (Test-Path $path) {
            Write-Host "`n[$n] $path"
            Get-Content $path -Tail 40
        }
    }

    Write-Host "`nLogs are in: $logsDir"
}
finally {
    Write-Host "`nStopping nodes..."
    Stop-NodeByPort -Port 5001
    Stop-NodeByPort -Port 5002
    Stop-NodeByPort -Port 5003
}
