@echo off
echo ============================================
echo   SYNC - Sincronizar arquivos com Databricks
echo ============================================
echo.
echo Sincronizando arquivos de origem...
echo Pressione Ctrl+C para parar a sincronizacao.
echo.
echo ============================================
echo   COMANDO PARA COPIAR:
echo ============================================
echo.
echo   .\databricks_cli\databricks.exe sync --watch --exclude *.zip .. /Workspace/Users/amadeu.ludwig@edenred.com/demo-app
echo.
echo ============================================

.\databricks_cli\databricks.exe sync --watch --exclude *.zip .. /Workspace/Users/amadeu.ludwig@edenred.com/demo-app