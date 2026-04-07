# -*- mode: python ; coding: utf-8 -*-

workspace = r"D:\xyclaw"
icon_path = r"D:\xyclaw\assets\apple_icon.ico"
datas = [
    (r"D:\xyclaw\assets\apple_icon.png", "assets"),
    (r"D:\xyclaw\assets\apple_icon.ico", "assets"),
]


a = Analysis(
    [r"D:\xyclaw\app\desktop_main.pyw"],
    pathex=[workspace],
    binaries=[],
    datas=datas,
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="OKX桌面程序",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=icon_path,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="OKX桌面程序",
)
