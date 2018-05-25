#AutoIt3Wrapper_Change2CUI=y
;#RequireAdmin,这会导致阻塞模式没了
; 在云服务器上运行时，远程桌面关闭会导致autoit3的鼠标和键盘操作失效，请改用RealVNC
#Include <GuiTab.au3>
#include <GuiButton.au3>
#include <GuiComboBox.au3>

Func RunMain()
   ; 请配置通达信软件的主程序
   Local $iPID = Run("D:\new_hbzq\TdxW.exe", "")
   ; 请配置通达信软件的标题
   Local $title = "[TITLE:华宝证券至富版V7.72; CLASS:#32770]"
   WinActivate($title)
   Local $hLoginWnd = WinWaitActive($title)

   ; 登录
   Sleep(500)
   ControlClick($hLoginWnd, "", "[CLASS:AfxWnd42; INSTANCE:3]")

   ; 找到主窗口
   Local $title = "[CLASS:TdxW_MainFrame_Class]"
   WinActivate($title)
   Local $hMainWnd = WinWaitActive($title)
   SendKeepActive($hMainWnd)
   WinMove($hMainWnd, "", 0, 0, 300, 400)
   Return $hMainWnd
EndFunc

Func PopOptionDlg($hMainWnd)
   Sleep(2000)
   ControlClick($hMainWnd, "", 1004)
   Send('{DOWN 1}{ENTER}')
EndFunc

Func PopDownloadDlg($hMainWnd)

   ;在本地居然不能用，不然会出错
   ;Sleep(2000)
   ;WinClose("[TITLE:即时播报; CLASS:#32770]")

   ; 点击到盘后数据下载
   ; 如果使用Mouse without Borders这个软件进行多台电脑会出错
   Sleep(2000)
   ControlClick($hMainWnd, "", 1000)
   Send('{DOWN 11}{ENTER}')
EndFunc

Func WaitOptionDlg()
   ; 这里要等一等，因为内容会被刷新
   Local $title = "[TITLE:衍生品行情; CLASS:#32770]"
   WinActivate($title)
   Local $hDlgWnd = WinWaitActive($title)
   Local $idBtn1 = ControlGetHandle($hDlgWnd,"","[TEXT:连接主站]")
   _GUICtrlButton_Click($idBtn1)
   WinWaitClose($hDlgWnd)
EndFunc

Func SetCheckDownloadDlg()
   ; 点击进行下载
   Local $title = "[TITLE:盘后数据下载; CLASS:#32770]"
   WinActivate($title)
   Local $hDlgWnd = WinWaitActive($title)


   Sleep(500)
   Local $idRdo1 = ControlGetHandle($hDlgWnd,"","[TEXT:日线和实时行情数据]")
   Local $idRdo2 = ControlGetHandle($hDlgWnd,"","[TEXT:5分钟线数据]")
   Local $idRdo3 = ControlGetHandle($hDlgWnd,"","[TEXT:1分钟线数据]")
   Local $idRdo4 = ControlGetHandle($hDlgWnd,"","[TEXT:日线数据]")

   If False Then
	  ; 将第一页的日线数据选上
	  ;_GUICtrlButton_SetCheck($idRdo1)
	  ; 将第二页的5分钟数据选上
	  ;_GUICtrlButton_SetCheck($idRdo2)
   EndIf

   ; 将第四页的日线选上
   _GUICtrlButton_SetCheck($idRdo4)

   ; 激活各页，需要设置的时间进行切换
   Local $idTab = ControlGetHandle($hDlgWnd,"","[CLASS:SysTabControl32; INSTANCE:1]")
   _GUICtrlTab_SetCurFocus($idTab, 3)

   Sleep(1000)

   _GUICtrlTab_SetCurFocus($idTab, 1)
   _GUICtrlTab_SetCurFocus($idTab, 0)
   _GUICtrlTab_SetCurFocus($idTab, 3)

   Sleep(1000)

   Local $hDate = ControlGetHandle($hDlgWnd,"","[ID:1237]")
   ;$sTime = GUICtrlRead($hDate)
   ;ConsoleWrite($sTime)
   ;Local $sText = WinGetText($hDate)
   ;ConsoleWrite($sText)
   ;GUICtrlSetData($hDate, "2018-01-01 00:00:00")

   ; 将第四页的日线选上
   Local $idRdo5 = ControlGetHandle($hDlgWnd,"","[TEXT:下载所有国内期货日线数据]")
   ; 只是选中，并不会导致出现下拉框
   _GUICtrlButton_Click($idRdo5)
   _GUICtrlButton_SetCheck($idRdo5)
   ; 这个ID会不会变？
   Local $idRdo6 = ControlGetHandle($hDlgWnd,"","[ID:2087]")

   _GUICtrlComboBox_ShowDropDown($idRdo6, True)
   _GUICtrlComboBox_SelectString($idRdo6,"期权")

EndFunc

Func ClickDownloadDlg()
   Local $title = "[TITLE:盘后数据下载; CLASS:#32770]"
   WinActivate($title)
   Local $hDlgWnd = WinWaitActive($title)

   ; 开始下载数据
   Sleep(500)
   ControlClick($hDlgWnd, "", "[TEXT:开始下载]")
EndFunc


Func WaitDownloadDlg()
   ; 开始下载数据
   Local $title = "[TITLE:盘后数据下载; CLASS:#32770]"
   WinActivate($title)
   Local $hDlgWnd = WinWaitActive($title)

   Local $idtext = ''
   Do
	  Sleep(2000)
	  $idtext = ControlGetText($hDlgWnd,"","[CLASS:Static; INSTANCE:2]")
   Until '下载完毕.' = $idtext
   ;Until '下载被取消.' = $idtext
   ; 找到下载完毕.
   MsgBox($MB_SYSTEMMODAL, "Title", "此消息框5秒倒计时后自动关闭", 5)
EndFunc

Func ExitMain()
   ; 需要退出下载对话框，否则程序没有完全退出
   Local $title = "[TITLE:盘后数据下载; CLASS:#32770]"
   WinActivate($title)
   Local $hDlgWnd = WinWaitActive($title)
   WinClose($hDlgWnd)
   WinWaitClose($hDlgWnd)

   ; 关闭主窗口
   Local $title = "[CLASS:TdxW_MainFrame_Class]"
   WinActivate($title)
   Local $hMainWnd = WinWaitActive($title)
   WinClose($hMainWnd)

   ; 确认退出
   Local $hMainWnd = WinWaitActive("[TITLE:退出确认; CLASS:#32770]")
   ControlClick($hMainWnd, "", "[TEXT:退出]")
EndFunc

Func ExitPop()
   ; 需要退出下载对话框，否则程序没有完全退出
   Local $title = "[TITLE:华宝证券; CLASS:#32770]"
   WinActivate($title)
   Local $hDlgWnd = WinWaitActive($title)
   WinClose($hDlgWnd)
   WinWaitClose($hDlgWnd)
EndFunc


$hMainWnd = RunMain()
ExitPop()
PopOptionDlg($hMainWnd)
WaitOptionDlg()
PopDownloadDlg($hMainWnd)
SetCheckDownloadDlg()

;ClickDownloadDlg()
;WaitDownloadDlg()
;ExitMain()

Exit(1)


