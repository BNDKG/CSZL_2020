# -*- mode: python ; coding: utf-8 -*-
import sys
sys.setrecursionlimit(5000)

block_cipher = None


a = Analysis(['CSZL_Framwork2020.py'],
             pathex=['C:\\Users\\ZMC_home01\\source\\repos\\CSZL_2020\\CSZL_Framwork2020'],
             binaries=[],
             datas=[],
             hiddenimports=['pandas','pylab','scipy','scipy.signal','cython', 'sklearn','sklearn.impute','sklearn.preprocessing','sklearn.decomposition','sklearn.cluster','sklearn.metrics','sklearn.metrics.get_scorer','sklearn.tree','sklearn.ensemble','sklearn.neighbors.typedefs','sklearn.neighbors.quad_tree','sklearn.tree._utils'],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          [],
          name='CSZL_Framwork2020',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          upx_exclude=[],
          runtime_tmpdir=None,
          console=True )
