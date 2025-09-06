# -*- mode: python ; coding: utf-8 -*-
import os
from PyInstaller.utils.hooks import collect_all

# -----------------------------
# Collect pandas, boto3, botocore fully
# -----------------------------
pandas_datas, pandas_binaries, pandas_hiddenimports = collect_all('pandas')
boto3_datas, boto3_binaries, boto3_hiddenimports = collect_all('boto3')
botocore_datas, botocore_binaries, botocore_hiddenimports = collect_all('botocore')

# -----------------------------
# Hidden imports for dynamic modules
# -----------------------------
hiddenimports = [
    'normal',
    'filmhub_csv',
    's3_utils',
] + pandas_hiddenimports + boto3_hiddenimports + botocore_hiddenimports

# -----------------------------
# Datas & binaries
# -----------------------------
datas = pandas_datas + boto3_datas + botocore_datas
binaries = pandas_binaries + boto3_binaries + botocore_binaries

# -----------------------------
# Main Analysis
# -----------------------------
a = Analysis(
    ['PNM-S3Sync.py'],
    pathex=[os.getcwd()],   # use current working directory to avoid __file__ issue
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    runtime_hooks=[],
    excludes=['pandas.tests', 'pytest'],  # exclude unnecessary test modules
    noarchive=False,
    debug=False,
    optimize=0,
)

# -----------------------------
# PYZ: Python bytecode archive
# -----------------------------
pyz = PYZ(a.pure, a.zipped_data, cipher=None)

# -----------------------------
# EXE: The macOS executable
# -----------------------------
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='PNM-S3Sync',
    debug=False,
    strip=False,
    upx=True,
    console=False,       # Windowed GUI
    bootloader_ignore_signals=False,
)

# -----------------------------
# APP bundle for macOS
# -----------------------------
app = BUNDLE(
    exe,
    name='PNM-S3Sync.app',
    icon=None,
    bundle_identifier=None,
)
