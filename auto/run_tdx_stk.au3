#Include "run_tdx_fun.au3"

$hMainWnd = RunMain()
ExitPop()
ExitPop2()
;PopOptionDlg($hMainWnd)
;WaitOptionDlg()
PopDownloadDlg($hMainWnd)
SetCheckDownloadDlg_STK()
ClickDownloadDlg()
WaitDownloadDlg()
ExitMain()

Exit(1)




