param(
  [string]$Name = "CSV Cleaner",
  [switch]$NoInstall
)

# Optional install of dependencies
if (-not $NoInstall) {
  pip install --upgrade pip
  pip install PySide6 pandas pyinstaller
}

pyinstaller --noconfirm --clean --onefile --windowed `
  --name "$Name" `
  gui_app.py

Write-Host "Build complete. See dist\$Name.exe"

