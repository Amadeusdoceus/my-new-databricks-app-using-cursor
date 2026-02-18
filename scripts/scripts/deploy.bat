@echo off
echo ============================================
echo   DEPLOY - Databricks App: demo-app
echo ============================================
echo.
echo Enviando para producao...
echo.
databricks apps deploy demo-app --source-code-path /Workspace/Users/amadeu.ludwig@edenred.com/demo-app
echo.
if %ERRORLEVEL% EQU 0 (
    echo [OK] Deploy realizado com sucesso!
) else (
    echo [ERRO] Falha no deploy. Verifique os logs com logs.bat
)
echo.
echo ============================================
echo   COMANDO PARA COPIAR:
echo ============================================
echo.
echo   databricks apps deploy demo-app --source-code-path /Workspace/Users/amadeu.ludwig@edenred.com/demo-app
echo.
pause
