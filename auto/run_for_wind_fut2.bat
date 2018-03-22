REM 收盘后运行
cd %~d0
cd %~dp0

REM 如果装了多个版本的Python,需要指定路径
PATH d:\Users\Kan\Anaconda3;d:\Users\Kan\Anaconda3\Scripts;d:\Users\Kan\Anaconda3\Library\bin;%PATH%

python.exe --version

REM 下载品种持仓
python.exe %~dp0..\demo_future\A02_download_futureoir.py
REM 合并品种持仓
python.exe %~dp0..\demo_future\A03_merge_futureoir.py


REM pause