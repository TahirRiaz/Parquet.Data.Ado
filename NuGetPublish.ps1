#Requires -Version 5.0
<#
.SYNOPSIS
    Builds and publishes a .NET project to NuGet.org
.DESCRIPTION
    This script automates the process of building, packing, and publishing a .NET project to NuGet.org.
    It includes error handling, logging, and parameter validation.
.PARAMETER ProjectPath
    Path to the project to build and publish
.PARAMETER Configuration
    Build configuration (Debug/Release)
.PARAMETER NuGetSource
    NuGet repository URL
.PARAMETER ApiKeyName
    Name of the environment variable containing the NuGet API key
.PARAMETER SkipBuild
    Skip the build process and only pack and publish
.EXAMPLE
    .\publish-nuget.ps1 -ProjectPath "C:\Projects\MyProject"
.NOTES
    Requires PowerShell 5+ and .NET SDK installed
#>

[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateNotNullOrEmpty()]
    [string]$ProjectPath = "B:\Github\Parquet.Data.Ado\Parquet.Data.Ado",
    
    [Parameter(Position = 1)]
    [ValidateSet("Debug", "Release")]
    [string]$Configuration = "Release",
    
    [Parameter(Position = 2)]
    [ValidateNotNullOrEmpty()]
    [string]$NuGetSource = "https://api.nuget.org/v3/index.json",
    
    [Parameter(Position = 3)]
    [string]$ApiKeyName = "NugetKey",
    
    [Parameter(Position = 4)]
    [switch]$SkipBuild
    
    # Removed the Verbose parameter as it's a common parameter
)

# Function to log messages with timestamps
function Write-Log {
    param (
        [Parameter(Mandatory = $true)]
        [string]$Message,
        
        [Parameter(Mandatory = $false)]
        [ValidateSet("Info", "Warning", "Error", "Success")]
        [string]$Level = "Info"
    )
    
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $icon = switch ($Level) {
        "Info"    { "ℹ️" }
        "Warning" { "⚠️" }
        "Error"   { "❌" }
        "Success" { "✅" }
        default   { "ℹ️" }
    }
    
    $color = switch ($Level) {
        "Info"    { "White" }
        "Warning" { "Yellow" }
        "Error"   { "Red" }
        "Success" { "Green" }
        default   { "White" }
    }
    
    Write-Host "[$timestamp] $icon " -NoNewline
    Write-Host $Message -ForegroundColor $color
}

# Function to validate project path
function Test-ProjectPath {
    param (
        [string]$Path
    )
    
    if (-not (Test-Path $Path)) {
        Write-Log "Project path does not exist: $Path" -Level "Error"
        return $false
    }
    
    $csprojFile = Get-ChildItem -Path $Path -Filter "*.csproj" | Select-Object -First 1
    if (-not $csprojFile) {
        Write-Log "No .csproj file found in directory: $Path" -Level "Error"
        return $false
    }
    
    return $true
}

# Banner function
function Show-Banner {
    $bannerText = @"
 _   _      _____       _     _ _     _     
| \ | |_   |  __ \     | |   | (_)   | |    
|  \| | | | | |  | | __ | |   | |_ ___| |__  
| . ` | | | | |  | |/ _  | |   | | / __| '_ \ 
| |\  | |_| | |__| | (_| | |___| | \__ \ | | |
|_| \_|\__,_|_____/ \__,_|_____|_|_|___/_| |_|
                                             
"@
    Write-Host $bannerText -ForegroundColor Cyan
    Write-Host "NuGet Package Publisher v1.1" -ForegroundColor Cyan
    Write-Host "===================================" -ForegroundColor Cyan
    Write-Host ""
}

# Main script execution
try {
    # Show banner
    Show-Banner
    
    # Start timing
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    
    # Validate project path
    Write-Log "Validating project path: $ProjectPath" -Level "Info"
    if (-not (Test-ProjectPath $ProjectPath)) {
        exit 1
    }
    
    # Get project name from .csproj file
    $csprojFile = Get-ChildItem -Path $ProjectPath -Filter "*.csproj" | Select-Object -First 1
    $projectName = [System.IO.Path]::GetFileNameWithoutExtension($csprojFile.Name)
    Write-Log "Project: $projectName" -Level "Info"
    
    # Step 0: Read API key from environment or prompt
    Write-Log "Getting API key from environment variable '$ApiKeyName'..." -Level "Info"
    $apiKey = [Environment]::GetEnvironmentVariable($ApiKeyName, "User")
    
    # Check Machine level if not found at User level
    if (-not $apiKey) {
        $apiKey = [Environment]::GetEnvironmentVariable($ApiKeyName, "Machine")
    }
    
    # Prompt for API key if not found in environment variables
    if (-not $apiKey) {
        Write-Log "Environment variable '$ApiKeyName' not found. Please enter your NuGet API key:" -Level "Warning"
        $apiKey = Read-Host -Prompt "Enter NuGet API key" -AsSecureString
        $BSTR = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($apiKey)
        $apiKey = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($BSTR)
        [System.Runtime.InteropServices.Marshal]::ZeroFreeBSTR($BSTR)
        
        if (-not $apiKey) {
            Write-Log "No API key provided. Cannot continue." -Level "Error"
            exit 1
        }
    }
    
    Write-Log "API key successfully retrieved" -Level "Success"
    
    # Ask for confirmation
    $confirmation = Read-Host -Prompt "Do you want to continue with publishing $projectName to NuGet.org? (Y/N)"
    if ($confirmation -ne 'Y' -and $confirmation -ne 'y') {
        Write-Log "Operation cancelled by user" -Level "Warning"
        exit 0
    }
    
    # Step 1: Clean
    if (-not $SkipBuild) {
        Write-Log "Cleaning project..." -Level "Info"
        $cleanOutput = dotnet clean $ProjectPath --configuration $Configuration --verbosity minimal
        if ($LASTEXITCODE -ne 0) {
            Write-Log "Clean failed with exit code $LASTEXITCODE" -Level "Error"
            Write-Log $cleanOutput -Level "Error"
            exit 1
        }
        Write-Log "Clean completed successfully" -Level "Success"
        
        # Step 2: Restore
        Write-Log "Restoring NuGet packages..." -Level "Info"
        $restoreOutput = dotnet restore $ProjectPath --verbosity minimal
        if ($LASTEXITCODE -ne 0) {
            Write-Log "Restore failed with exit code $LASTEXITCODE" -Level "Error"
            Write-Log $restoreOutput -Level "Error"
            exit 1
        }
        Write-Log "Restore completed successfully" -Level "Success"
        
        # Step 3: Build
        Write-Log "Building project with configuration: $Configuration..." -Level "Info"
        $buildOutput = dotnet build $ProjectPath --configuration $Configuration --no-restore
        if ($LASTEXITCODE -ne 0) {
            Write-Log "Build failed with exit code $LASTEXITCODE" -Level "Error"
            Write-Log $buildOutput -Level "Error"
            exit 1
        }
        Write-Log "Build completed successfully" -Level "Success"
    }
    else {
        Write-Log "Skipping build process (--skip-build flag provided)" -Level "Warning"
    }
    
    # Step 4: Pack
    Write-Log "Packing project..." -Level "Info"
    $noBuildFlag = if ($SkipBuild) { "" } else { "--no-build" }
    $packOutput = dotnet pack $ProjectPath --configuration $Configuration $noBuildFlag
    if ($LASTEXITCODE -ne 0) {
        Write-Log "Pack failed with exit code $LASTEXITCODE" -Level "Error"
        Write-Log $packOutput -Level "Error"
        exit 1
    }
    Write-Log "Pack completed successfully" -Level "Success"
    
    # Step 5: Locate .nupkg
    Write-Log "Locating NuGet package..." -Level "Info"
    $packagePath = "$ProjectPath\bin\$Configuration"
    $package = Get-ChildItem $packagePath -Filter "*.nupkg" |
        Where-Object { $_.Name -notlike "*.symbols.nupkg" } |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
    
    if (-not $package) {
        Write-Log "No .nupkg file found in $packagePath" -Level "Error"
        exit 1
    }
    
    # Get package version from filename
    $packageNamePattern = "^(.+?)\.(\d+\.\d+\.\d+(?:\.\d+)?(?:-[a-zA-Z0-9\.-]+)?)\.nupkg$"
    if ($package.Name -match $packageNamePattern) {
        $packageId = $matches[1]
        $packageVersion = $matches[2]
        Write-Log "Found package: $packageId (v$packageVersion)" -Level "Success"
    }
    else {
        Write-Log "Found package: $($package.FullName)" -Level "Success"
    }
    
    # Step 6: Ask for final confirmation
    $finalConfirmation = Read-Host -Prompt "Are you sure you want to publish this package to NuGet.org? (Y/N)"
    if ($finalConfirmation -ne 'Y' -and $finalConfirmation -ne 'y') {
        Write-Log "Publishing cancelled by user" -Level "Warning"
        exit 0
    }
    
    # Step 7: Push
    Write-Log "Pushing package to NuGet.org..." -Level "Info"
    $pushOutput = dotnet nuget push $package.FullName `
        --api-key $apiKey `
        --source $NuGetSource `
        --skip-duplicate `
        --no-symbols
    
    if ($LASTEXITCODE -ne 0) {
        Write-Log "Push failed with exit code $LASTEXITCODE" -Level "Error"
        Write-Log $pushOutput -Level "Error"
        exit 1
    }
    
    # Stop timing
    $stopwatch.Stop()
    $elapsedTime = $stopwatch.Elapsed
    $formattedTime = "{0:mm}m {0:ss}s" -f $elapsedTime
    
    Write-Log "Package successfully published to NuGet.org!" -Level "Success"
    Write-Log "Total time: $formattedTime" -Level "Info"
    
    # Optional: Open browser to package page
    if ($packageId) {
        $openBrowser = Read-Host -Prompt "Open package page in browser? (Y/N)"
        if ($openBrowser -eq 'Y' -or $openBrowser -eq 'y') {
            Start-Process "https://www.nuget.org/packages/$packageId"
        }
    }
}
catch {
    Write-Log "An unexpected error occurred: $_" -Level "Error"
    Write-Log $_.ScriptStackTrace -Level "Error"
    exit 1
}