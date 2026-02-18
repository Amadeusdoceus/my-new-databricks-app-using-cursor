@echo off
echo ============================================
echo   LOGS - Databricks App: demo-app
echo ============================================
echo.
echo Obtendo logs do aplicativo...
echo.
databricks apps get-logs demo-app
echo.
echo ============================================
echo   COMANDO PARA COPIAR:
echo ============================================
echo.
echo   databricks apps get-logs demo-app
echo.
pause
