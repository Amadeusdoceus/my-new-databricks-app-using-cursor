"""
Script para verificar se a tabela gold.fact.maintenanceservices existe e está acessível
via Databricks CLI.
"""
import subprocess
import sys
from pathlib import Path

# Tenta encontrar o executável do Databricks CLI
cli_paths = [
    Path(r"C:\Databricks\scripts\databricks_cli\databricks.exe"),
    Path(r"C:\Databricks\scripts\scripts\databricks_cli\databricks.exe"),
    Path("databricks"),  # Se estiver no PATH
]

cli_exe = None
for path in cli_paths:
    if isinstance(path, str):
        cli_exe = path
        break
    elif path.exists():
        cli_exe = str(path)
        break

if not cli_exe:
    print("❌ Databricks CLI não encontrado. Verifique se está instalado.")
    sys.exit(1)

print(f"✅ Usando Databricks CLI: {cli_exe}\n")

# Tabelas para verificar
tables_to_check = [
    "gold.fact.maintenanceservices",
    "gold.fact_maintenanceservices",
]

print("=" * 60)
print("VERIFICAÇÃO DE TABELAS VIA DATABRICKS CLI")
print("=" * 60)

for table_name in tables_to_check:
    print(f"\n🔍 Verificando tabela: {table_name}")
    print("-" * 60)
    
    # Tenta executar: databricks sql execute --query "DESCRIBE TABLE gold.fact.maintenanceservices"
    try:
        cmd = [
            cli_exe,
            "sql",
            "execute",
            "--query",
            f'DESCRIBE TABLE {table_name}',
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
        )
        
        if result.returncode == 0:
            print(f"✅ Tabela '{table_name}' encontrada!")
            print("\nColunas encontradas:")
            print(result.stdout)
            
            # Verifica se tem as colunas necessárias
            output_lower = result.stdout.lower()
            has_timestamp = "transaction_timestamp" in output_lower
            has_orderservice = "orderservice" in output_lower
            
            if has_timestamp and has_orderservice:
                print("\n✅ Colunas obrigatórias encontradas:")
                print("   - transaction_timestamp")
                print("   - orderservice")
            else:
                print("\n⚠️ Algumas colunas obrigatórias podem estar faltando:")
                if not has_timestamp:
                    print("   ❌ transaction_timestamp")
                if not has_orderservice:
                    print("   ❌ orderservice")
        else:
            print(f"❌ Erro ao acessar tabela '{table_name}':")
            print(f"   Exit code: {result.returncode}")
            print(f"   Stderr: {result.stderr}")
            
    except subprocess.TimeoutExpired:
        print(f"⏱️ Timeout ao verificar '{table_name}'")
    except Exception as e:
        print(f"❌ Erro ao executar comando: {e}")

print("\n" + "=" * 60)
print("VERIFICAÇÃO CONCLUÍDA")
print("=" * 60)

# Tenta listar todas as tabelas no schema gold.fact
print("\n📋 Tentando listar todas as tabelas em 'gold.fact':")
print("-" * 60)

try:
    cmd = [
        cli_exe,
        "sql",
        "execute",
        "--query",
        "SHOW TABLES IN gold.fact",
    ]
    
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=30,
    )
    
    if result.returncode == 0:
        print("✅ Tabelas encontradas em gold.fact:")
        print(result.stdout)
    else:
        print(f"⚠️ Não foi possível listar tabelas: {result.stderr}")
        
except Exception as e:
    print(f"❌ Erro: {e}")
