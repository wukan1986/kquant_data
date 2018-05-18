REM 收盘后运行
cd %~d0
cd %~dp0
REM 以下的内容请根据自己的需求进行调整

"D:\Program Files (x86)\AutoIt3\autoit3.exe" /ErrorStdOut "%~dp0\run_tdx_opt.au3"

pause

REM 下载50期权清单
python.exe %~dp0..\demo_option\A01_download_option_info.py
REM 生成put call ratio
python.exe %~dp0..\demo_option\B01_50etf_put_call_ratio.py

pause