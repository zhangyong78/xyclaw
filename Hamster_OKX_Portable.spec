# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['D:\\xyclaw\\app\\desktop_main.pyw'],
    pathex=[],
    binaries=[],
    datas=[('D:\\xyclaw\\assets\\apple_icon.png', 'assets'), ('D:\\xyclaw\\assets\\apple_icon.ico', 'assets')],
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
    a.binaries,
    a.datas,
    [],
    name='Hamster_OKX_Portable',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['D:\\xyclaw\\assets\\apple_icon.ico'],
)
