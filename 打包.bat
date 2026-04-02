@echo off
chcp 65001 >nul
echo ========================================
echo    局域网文件共享服务器 - 打包工具
echo ========================================
echo.

python build_exe.py

echo.
echo 按任意键退出...
pause >nul

