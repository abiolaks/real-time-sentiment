# PowerShell script to sync local.settings.json to Azure Function App Application settings
# Usage: .\p.sh <FunctionAppName> <ResourceGroupName>

param(
    [string]$functionAppName,
    [string]$resourceGroup
)

# Read local.settings.json
$json = Get-Content ./local.settings.json | Out-String | ConvertFrom-Json
$settings = $json.Values.PSObject.Properties

foreach ($setting in $settings) {
    $key = $setting.Name
    $value = $setting.Value
    Write-Host "Setting $key..."
    az functionapp config appsettings set `
        --name $functionAppName `
        --resource-group $resourceGroup `
        --settings "$key=$value"
}
Write-Host "All settings applied."
