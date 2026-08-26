#define MyAppName "Recache"
#define MyAppVersion "1.0.0"
#define MyAppPublisher "Recache"
#define MyAppExeName "recache.exe"
#define MyServiceName "Recache"
#define MyServiceDisplayName "Recache Cache Service"
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

; Service registration is done with direct sc.exe calls rather than
; shelling out to "powershell.exe -ExecutionPolicy Bypass -File ..."
; — that exact invocation pattern (interpreter + execution-policy
; bypass + script file, spawned from an installer) is also what
; malware droppers commonly do, and is a much more likely trigger
; for AV/Defender heuristics than the lack of a trusted signature.
[Run]
Filename: "{sys}\sc.exe"; Parameters: "stop {#MyServiceName}"; Flags: runhidden; StatusMsg: "Stopping any existing Recache service..."
Filename: "{sys}\sc.exe"; Parameters: "delete {#MyServiceName}"; Flags: runhidden; StatusMsg: "Removing any existing Recache service..."
Filename: "{sys}\sc.exe"; Parameters: "create {#MyServiceName} binPath= ""{app}\{#MyAppExeName}"" start= auto DisplayName= ""{#MyServiceDisplayName}"""; Flags: runhidden; StatusMsg: "Registering the Recache service..."
Filename: "{sys}\sc.exe"; Parameters: "description {#MyServiceName} ""In-memory Redis-compatible cache server (recache).""" ; Flags: runhidden
Filename: "{sys}\sc.exe"; Parameters: "failure {#MyServiceName} reset= 86400 actions= restart/5000/restart/5000/restart/5000"; Flags: runhidden
Filename: "{sys}\sc.exe"; Parameters: "start {#MyServiceName}"; Flags: runhidden; StatusMsg: "Starting the Recache service..."

[UninstallRun]
Filename: "{sys}\sc.exe"; Parameters: "stop {#MyServiceName}"; Flags: runhidden; RunOnceId: "StopRecacheService"
Filename: "{sys}\sc.exe"; Parameters: "delete {#MyServiceName}"; Flags: runhidden; RunOnceId: "DeleteRecacheService"
