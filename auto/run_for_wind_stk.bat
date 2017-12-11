REM 以下的内容请根据自己的需求进行调整

REM 下载行业分类
python.exe %~dp0..\demo_stock\A_1day\B04_download_sectors_st.py
REM 合并行业分类
python.exe %~dp0..\demo_stock\A_1day\B05_merge_sector_daily.py
REM 更新股票ipo信息
python.exe %~dp0..\demo_stock\A_1day\C01_ipo_date.py
REM 更新、合并总股本等信息
python.exe %~dp0..\demo_stock\A_1day\D01_download_total_shares.py
python.exe %~dp0..\demo_stock\A_1day\D02_merge_total_shares.py
REM 下载报告期数据
REM python.exe %~dp0..\demo_stock\A_1day\D03_download_report.py
REM python.exe %~dp0..\demo_stock\A_1day\D04_download_pb_lf.py

pause