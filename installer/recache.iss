#define MyAppName "Recache"
#define MyAppVersion "1.0.0"
#define MyAppPublisher "Recache"
#define MyAppExeName "recache.exe"
#define MyServiceName "Recache"
#define PublishDir "..\recache\publish"

[Setup]
AppId={{6C1B1F0E-6E8B-4B7B-9C39-2E7B7B9D9A11}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
DefaultDirName={autopf}\{#MyAppName}
DefaultGroupName={#MyAppName}
DisableProgramGroupPage=yes
OutputDir=Output
OutputBaseFilename=RecacheSetup
Compression=lzma2
SolidCompression=yes
PrivilegesRequired=admin
UninstallDisplayIcon={app}\{#MyAppExeName}
WizardStyle=modern

[Files]
Source: "{#PublishDir}\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "service-install.ps1"; DestDir: "{app}"; Flags: ignoreversion
Source: "service-uninstall.ps1"; DestDir: "{app}"; Flags: ignoreversion

[Run]
Filename: "powershell.exe"; Parameters: "-NoProfile -ExecutionPolicy Bypass -File ""{app}\service-install.ps1"""; Flags: runhidden waituntilterminated; StatusMsg: "Installing and starting the Recache service..."

[UninstallRun]
Filename: "powershell.exe"; Parameters: "-NoProfile -ExecutionPolicy Bypass -File ""{app}\service-uninstall.ps1"""; Flags: runhidden waituntilterminated; RunOnceId: "StopRecacheService"
