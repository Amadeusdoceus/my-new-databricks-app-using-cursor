@echo off
echo ============================================
echo   INSTALL - Databricks CLI e SDK
echo ============================================
echo.
echo Instalando/atualizando Databricks CLI e SDK...
echo.
pip install --upgrade databricks-cli databricks-sdk
echo.
echo Verificando versoes instaladas:
echo.
databricks --version
pip show databricks-sdk | findstr /i "Version"
echo.
if %ERRORLEVEL% EQU 0 (
    echo [OK] Instalacao concluida com sucesso!
) else (
    echo [ERRO] Falha na instalacao. Verifique seu ambiente Python.
)
echo.
echo ============================================
echo   COMANDO PARA COPIAR:
echo ============================================
echo.
echo   pip install --upgrade databricks-cli databricks-sdk
echo.
pause
