@echo off

set location=%1
set destination=%2

xcopy /s %location%\build\native\include %destination%\include\
xcopy /s %location%\build\native\lib %destination%\lib\