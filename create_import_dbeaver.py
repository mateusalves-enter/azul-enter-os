import subprocess
import time
import os
import re
import unicodedata
import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# === CONFIGURAÇÕES ===
CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "report_elawReport_264_48229347209932635939549488.csv")
SCHEMA = "ai_deployment"
TABLE = "azul_enteros_cadastro_elaw"
BATCH_SIZE = 300
CSV_DELIMITER = ";"
CSV_ENCODING = "utf-8"

DB_HOST = "127.0.0.1"
DB_PORT = 5433
DB_NAME = "talisman-prod"
DB_USER = "noop"
DB_PASSWORD = "noop"


def normalize_column_name(name):
    """
    Normaliza nome de coluna para padrão snake_case sem acentos.
    Exemplos:
      'ID do Processo'        -> 'id_do_processo'
      'Somatória do Valor'    -> 'somatoria_do_valor'
      'Data/Hora (UTC)'       -> 'data_hora_utc'
      'Nº do Documento'       -> 'n_do_documento'
    """
    # Remove acentos
    nfkd = unicodedata.normalize("NFKD", str(name))
    ascii_str = "".join(c for c in nfkd if not unicodedata.combining(c))

    # Lowercase
    ascii_str = ascii_str.lower()

    # Substitui qualquer caractere que não seja letra/número por underscore
    ascii_str = re.sub(r"[^a-z0-9]+", "_", ascii_str)

    # Remove underscores no começo e fim
    ascii_str = ascii_str.strip("_")

    # Colapsa múltiplos underscores em um só
    ascii_str = re.sub(r"_+", "_", ascii_str)

    return ascii_str


def hoop_login():
    print("🔐 Executando hoop login...")
    subprocess.run(["hoop", "login"], check=True)
    print("✅ Login OK")


def start_hoop_tunnel(max_attempts=15, relogin_on_fail=True):
    """Abre o tunnel hoop. Se falhar e relogin_on_fail=True, faz hoop login e tenta de novo."""
    for login_attempt in range(2):  # no máximo 1 re-login
        process = subprocess.Popen(
            ["hoop", "connect", "ai-deployment-owner"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        for attempt in range(1, max_attempts + 1):
            time.sleep(2)
            try:
                test_conn = psycopg2.connect(
                    host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                    user=DB_USER, password=DB_PASSWORD, connect_timeout=3,
                )
                test_conn.close()
                return process
            except psycopg2.OperationalError:
                pass

        process.terminate()
        process.wait()

        if login_attempt == 0 and relogin_on_fail:
            print("⚠️  Tunnel não subiu. Tentando hoop login novamente...")
            hoop_login()
            time.sleep(2)
        else:
            break

    raise RuntimeError("Tunnel não subiu mesmo após re-login.")


def get_connection():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )
    conn.autocommit = False
    return conn


def table_exists(cursor, schema, table):
    """Verifica se a tabela existe no schema."""
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        )
    """, (schema, table))
    return cursor.fetchone()[0]


def build_create_table_ddl(columns, schema, table):
    """Gera DDL com todas as colunas como TEXT."""
    cols = [f"    {col}  TEXT" for col in columns]
    ddl = f"CREATE TABLE IF NOT EXISTS {schema}.{table} (\n"
    ddl += ",\n".join(cols)
    ddl += "\n);"
    return ddl


def get_column_limits(cursor, schema, table):
    cursor.execute("""
        SELECT column_name, character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (schema, table))
    return {row[0]: row[1] for row in cursor.fetchall()}


def truncate_data(df, column_limits):
    truncated_count = 0
    for col in df.columns:
        limit = column_limits.get(col)
        if limit and df[col].dtype == object:
            mask = df[col].astype(str).str.len() > limit
            count = mask.sum()
            if count > 0:
                df[col] = df[col].astype(str).str[:limit]
                truncated_count += count
                print(f"   ⚠️  Coluna '{col}': {count} valores truncados para {limit} caracteres")
    return df, truncated_count


def import_csv():
    hoop_login()

    print(f"\n📄 Lendo CSV: {CSV_PATH}")
    # Lê tudo como string para preservar dados e evitar inferência errada
    df = pd.read_csv(CSV_PATH, delimiter=CSV_DELIMITER, encoding=CSV_ENCODING, dtype=str)
    df = df[~df.iloc[:, 0].astype(str).str.startswith("Sistema")]
    df = df.reset_index(drop=True)

    # Normaliza nomes das colunas
    original_cols = list(df.columns)
    normalized_cols = [normalize_column_name(c) for c in original_cols]

    print("\n🔤 Normalização dos nomes das colunas:")
    for orig, norm in zip(original_cols, normalized_cols):
        if orig != norm:
            print(f"   '{orig}' → '{norm}'")
        else:
            print(f"   '{orig}' (sem mudança)")

    # Detecta colunas duplicadas após normalização
    if len(set(normalized_cols)) != len(normalized_cols):
        seen = {}
        duplicates = []
        for c in normalized_cols:
            seen[c] = seen.get(c, 0) + 1
            if seen[c] > 1:
                duplicates.append(c)
        raise ValueError(
            f"❌ Nomes duplicados após normalização: {set(duplicates)}.\n"
            f"   Renomeie as colunas originais no CSV antes de continuar."
        )

    df.columns = normalized_cols

    total_rows = len(df)
    print(f"\n   Linhas encontradas: {total_rows}")
    print(f"   Colunas: {list(df.columns)}")

    print("\n🔗 Abrindo tunnel...")
    hoop_process = start_hoop_tunnel()
    print("✅ Tunnel ativo")

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # === ETAPA 1: Verifica se a tabela existe; se não, cria ===
        print(f"\n🔍 Verificando se {SCHEMA}.{TABLE} existe...")
        if not table_exists(cursor, SCHEMA, TABLE):
            print(f"   ⚙️  Tabela não existe. Criando com todas as colunas TEXT...")
            ddl = build_create_table_ddl(list(df.columns), SCHEMA, TABLE)
            print(f"\n📝 DDL:\n{ddl}\n")
            cursor.execute(ddl)
            conn.commit()
            print(f"   ✅ Tabela {SCHEMA}.{TABLE} criada")
        else:
            print(f"   ✅ Tabela já existe")

        # === ETAPA 2: Limpa a tabela ===
        print(f"\n🗑️  Limpando tabela {SCHEMA}.{TABLE}...")
        cursor.execute(f"DELETE FROM {SCHEMA}.{TABLE}")
        deleted = cursor.rowcount
        conn.commit()
        print(f"   ✅ {deleted} linhas removidas")

        # === ETAPA 3: Verifica limites de colunas ===
        print("\n🔍 Verificando limites das colunas...")
        column_limits = get_column_limits(cursor, SCHEMA, TABLE)
        df, truncated = truncate_data(df, column_limits)
        if truncated == 0:
            print("   ✅ Todos os valores dentro do limite")

        cursor.close()
        conn.close()
    finally:
        hoop_process.terminate()
        hoop_process.wait()
        time.sleep(1)

    # === ETAPA 4: Importação em batches ===
    df = df.replace({np.nan: None})
    columns = list(df.columns)
    cols_str = ", ".join(columns)
    insert_query = f'INSERT INTO {SCHEMA}.{TABLE} ({cols_str}) VALUES %s'

    total_inserted = 0
    total_batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE
    consecutive_errors = 0
    MAX_ERRORS = 3
    MAX_RETRIES = 5

    print(f"\n🚀 Iniciando importação em {total_batches} batches de {BATCH_SIZE} linhas...\n")

    for i in range(0, total_rows, BATCH_SIZE):
        batch_num = (i // BATCH_SIZE) + 1
        batch = df.iloc[i : i + BATCH_SIZE]
        values = [tuple(row) for row in batch.to_numpy()]

        success = False
        for retry in range(1, MAX_RETRIES + 1):
            hoop_process = None
            try:
                hoop_process = start_hoop_tunnel()
                conn = get_connection()
                cursor = conn.cursor()

                execute_values(cursor, insert_query, values)
                conn.commit()
                total_inserted += len(values)
                consecutive_errors = 0
                success = True
                print(f"   ✅ Batch {batch_num}/{total_batches} — {total_inserted}/{total_rows} linhas inseridas")

                cursor.close()
                conn.close()

            except RuntimeError as e:
                # Tunnel não subiu nem com re-login
                print(f"   ⚠️  Batch {batch_num} — tunnel falhou (tentativa {retry}/{MAX_RETRIES}): {e}")
            except Exception as e:
                print(f"   ⚠️  Batch {batch_num} — erro (tentativa {retry}/{MAX_RETRIES}): {e}")

            finally:
                if hoop_process:
                    hoop_process.terminate()
                    hoop_process.wait()
                    time.sleep(2)

            if success:
                break

        if not success:
            consecutive_errors += 1
            print(f"   ❌ Batch {batch_num} falhou após {MAX_RETRIES} tentativas.")

        if consecutive_errors >= MAX_ERRORS:
            print(f"\n🛑 {MAX_ERRORS} erros consecutivos. Abortando.")
            break

    print(f"\n✅ Importação concluída! {total_inserted}/{total_rows} linhas inseridas.")


if __name__ == "__main__":
    import_csv()