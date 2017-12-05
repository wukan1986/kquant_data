REM 收盘后运行
cd %~d0
cd %~dp0

REM 先打开通达信，下载日线和5分钟数据
REM https://www.autoitscript.com/site/autoit/downloads/
REM https://www.autoitscript.com/cgi-bin/getfile.pl?autoit3/autoit-v3-setup.exe
REM 由于最新版通达信不能在虚拟机中运行，只能安装2017年6月份以前版本
"D:\Program Files (x86)\AutoIt3\autoit3.exe" /ErrorStdOut "%~dp0\run_tdx.au3" 


REM 下载除权数据
python.exe %~dp0..\demo_stock\A_1day\B01_download_pwr.py
REM 导出日线数据
python.exe %~dp0..\demo_stock\A_1day\B02_export.py
REM 合并日线数据
python.exe %~dp0..\demo_stock\A_1day\B03_merge.py

REM 请根据自己的需求选择
start run_for_wind.bat
start run_for_5min.bat

pause