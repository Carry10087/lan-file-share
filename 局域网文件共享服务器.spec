# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['局域网文件共享服务器.py'],
    pathex=[],
    binaries=[],
    datas=[('static', 'static')],
    hiddenimports=['werkzeug', 'flask', 'waitress', 'configparser'],
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
    name='局域网文件共享服务器',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='NONE',
)
