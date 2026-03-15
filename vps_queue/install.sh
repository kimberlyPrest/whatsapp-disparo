#!/usr/bin/env bash
# =================================================================
# VPS Worker — Setup e execução
# Execute UMA VEZ para configurar, depois use apenas: python worker.py
# =================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== 1. Instalando dependências Python ==="
pip install -r requirements.txt

echo ""
echo "=== 2. Carregando variáveis de ambiente ==="
if [ -f "../.env" ]; then
    set -a
    source "../.env"
    set +a
    echo "   .env carregado de: $(realpath ../.env)"
else
    echo "   AVISO: ../.env não encontrado. Exporte as variáveis manualmente."
fi

echo ""
echo "=== 3. Verificando conexão com PostgreSQL da VPS ==="
python3 - <<'PYEOF'
import os, psycopg2

raw = os.environ.get("POSTGRES_HOST", "")
if ":" in raw:
    host, port = raw.rsplit(":", 1)
    port = int(port)
else:
    host = raw
    port = int(os.environ.get("POSTGRES_PORTA", "5432").strip() or "5432")

try:
    conn = psycopg2.connect(
        host=host, port=port,
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
        connect_timeout=10,
    )
    cur = conn.cursor()
    cur.execute("SELECT version()")
    v = cur.fetchone()[0][:60]
    print(f"   Conectado: {v}")
    conn.close()
except Exception as e:
    print(f"   FALHA na conexão: {e}")
    print("   Verifique as variáveis POSTGRES_* no .env")
    exit(1)
PYEOF

echo ""
echo "=== 4. Aplicando migrations da fila (setup.sql) ==="
python3 - <<'PYEOF'
import os, psycopg2

raw = os.environ.get("POSTGRES_HOST", "")
host, port = (raw.rsplit(":", 1) if ":" in raw else (raw, "5432"))
port = int(port)

conn = psycopg2.connect(
    host=host, port=port,
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    dbname=os.environ["POSTGRES_DB"],
)
sql_path = os.path.join(os.path.dirname(__file__), "setup.sql")
with open(sql_path) as f:
    sql = f.read()

with conn:
    with conn.cursor() as cur:
        # Pula linhas de comentário que começam com --
        statements = [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]
        for stmt in statements:
            if stmt:
                cur.execute(stmt)
        print("   dispatch_queue criada/verificada com sucesso.")
conn.close()
PYEOF

echo ""
echo "=== Setup concluído! ==="
echo ""
echo "Para iniciar o worker:"
echo "   cd $(pwd)"
echo "   python worker.py"
echo ""
echo "Para rodar em background (produção):"
echo "   nohup python worker.py >> worker.log 2>&1 &"
echo "   echo \$! > worker.pid"
