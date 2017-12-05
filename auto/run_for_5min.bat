REM 导出从通达信官网下载的历史5分钟数据(只做一次即可)
REM python.exe %~dp0..\demo_stock\B_5min\B01_export_5.py
REM 导出本地的5分钟数据
python.exe %~dp0..\demo_stock\B_5min\A01_export_lc5.py
REM 合并新旧数据
python.exe %~dp0..\demo_stock\B_5min\A02_concat_h5.py
REM 合并所有股票生成矩阵，5分钟数据太多了，可能得用64位
python.exe %~dp0..\demo_stock\B_5min\A03_merge.py

pause