REM 收盘后运行
cd %~d0
cd %~dp0

REM 如果装了多个版本的Python,需要指定路径
PATH d:\Users\Kan\Anaconda3;d:\Users\Kan\Anaconda3\Scripts;d:\Users\Kan\Anaconda3\Library\bin;%PATH%

python.exe --version

REM 下载主力代码清单
python.exe %~dp0..\demo_future\B02_download_trade_hiscode.py
REM 下载交易所上市合约(read_csv)
python.exe %~dp0..\demo_future\C01_download_sectors.py
REM 更新合约合约名，上市退市日期(read_csv)
python.exe %~dp0..\demo_future\D01_download_ipo_last_trade_trading.py

REM pause