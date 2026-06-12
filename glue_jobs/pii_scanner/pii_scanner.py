"""
PII Scanner — Glue PySpark Job (LGPD / AWS Lake Formation).

Detecta colunas com dados pessoais em todas as tabelas do Glue Data Catalog
para as zonas bronze e silver usando dois mecanismos complementares:

  1. Name hints  — mapeamento direto de nomes de colunas semanticamente
                   associados a dados pessoais (cpf, email, telefone…).
                   Confiança: ALTA — basta o nome bater.

  2. Regex scan  — amostra SAMPLE_ROWS linhas por coluna de tipo string.
                   Se a proporção de linhas que casam com o padrão for
                   ≥ REGEX_MATCH_THRESHOLD → coluna marcada como PII.
                   Confiança: MÉDIA — captura colunas renomeadas/mascaradas.

Para cada coluna PII detectada o job executa dois passos de catalogação:

  a) Glue Data Catalog  — atualiza o campo `Parameters` da coluna com:
       pii_type              ex: "Email"
       lgpd_classification   ex: "dado_pessoal"
       pii_confidence        ex: "high" | "medium"
       pii_detected_by       "cloudmart-pii-scanner"
       pii_tagged_at         ISO-8601 timestamp

  b) Lake Formation LF-Tags — aplica as tags governadas:
       PII                = <tipo>    ex: "Email"
       LGPD_Classification = <classe> ex: "dado_pessoal"

  Ao final grava um relatório JSON em:
    s3://<REPORT_BUCKET>/pii-reports/<YYYY>/<MM>/<DD>/pii_report.json

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
REFERÊNCIA LGPD
  Art. 5° — Dado pessoal: informação relacionada a pessoa natural
            identificada ou identificável.
  Art. 5°, II — Dado pessoal sensível: dado sobre origem racial ou
            étnica, convicção religiosa, saúde, vida sexual, dado
            genético ou biométrico, etc.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Parâmetros:
  --BRONZE_DB     nome do banco no Glue Catalog da bronze-zone
  --SILVER_DB     nome do banco no Glue Catalog da silver-zone
  --BRONZE_BUCKET bucket S3 da bronze-zone (leitura dos Parquets)
  --SILVER_BUCKET bucket S3 da silver-zone
  --REPORT_BUCKET bucket S3 onde o relatório JSON é gravado
"""

import json
import re
import sys
from datetime import datetime, timezone
from typing import Optional

import boto3
import pandas as pd
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# ── parâmetros do job ────────────────────────────────────────────────────────
args = getResolvedOptions(
    sys.argv,
    ["BRONZE_DB", "SILVER_DB", "BRONZE_BUCKET", "SILVER_BUCKET", "REPORT_BUCKET"],
)
BRONZE_DB = args["BRONZE_DB"]
SILVER_DB = args["SILVER_DB"]
BRONZE_BUCKET = args["BRONZE_BUCKET"]
SILVER_BUCKET = args["SILVER_BUCKET"]
REPORT_BUCKET = args["REPORT_BUCKET"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

glue_client = boto3.client("glue")
lf_client = boto3.client("lakeformation")

# ── hiperparâmetros de detecção ──────────────────────────────────────────────
SAMPLE_ROWS = 500           # linhas amostradas por coluna para regex
REGEX_MATCH_THRESHOLD = 0.20  # 20 % do sample → PII detectado por regex

# ── padrões regex PII (LGPD Art. 5°) ────────────────────────────────────────
# Cada entrada: pii_type → (compiled_regex, lgpd_classification)
PII_PATTERNS: dict[str, tuple[re.Pattern, str]] = {
    "CPF": (
        re.compile(r"\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b"),
        "dado_pessoal",
    ),
    "CNPJ": (
        re.compile(r"\b\d{2}\.?\d{3}\.?\d{3}[/\\]?\d{4}-?\d{2}\b"),
        "dado_pessoal",
    ),
    "Email": (
        re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", re.IGNORECASE),
        "dado_pessoal",
    ),
    "Telefone": (
        # Cobre: (11) 98765-4321 | +55 11 98765-4321 | 11987654321
        re.compile(r"(\+?55\s?)?(\(?[1-9]{2}\)?[\s.\-]?)(\d{4,5}[\s.\-]?\d{4})"),
        "dado_pessoal",
    ),
    "CEP": (
        re.compile(r"\b\d{5}-?\d{3}\b"),
        "dado_pessoal",
    ),
    "RG": (
        re.compile(r"\b\d{1,2}\.?\d{3}\.?\d{3}-?[\dxX]\b"),
        "dado_pessoal",
    ),
}

# ── mapeamento por nome de coluna (ALTA confiança) ────────────────────────────
# Chave: substring que pode aparecer no nome (lower). Valor: (pii_type, lgpd_class)
PII_NAME_HINTS: dict[str, tuple[str, str]] = {
    "cpf":              ("CPF",               "dado_pessoal"),
    "cnpj":             ("CNPJ",              "dado_pessoal"),
    "email":            ("Email",             "dado_pessoal"),
    "e_mail":           ("Email",             "dado_pessoal"),
    "telefone":         ("Telefone",          "dado_pessoal"),
    "celular":          ("Telefone",          "dado_pessoal"),
    "fone":             ("Telefone",          "dado_pessoal"),
    "nome_cliente":     ("NomePessoa",        "dado_pessoal"),
    "nome_usuario":     ("NomePessoa",        "dado_pessoal"),
    "nome_completo":    ("NomePessoa",        "dado_pessoal"),
    "data_nascimento":  ("DataNascimento",    "dado_pessoal"),
    "data_nasc":        ("DataNascimento",    "dado_pessoal"),
    "nascimento":       ("DataNascimento",    "dado_pessoal"),
    "rg":               ("RG",               "dado_pessoal"),
    "cep":              ("CEP",              "dado_pessoal"),
    "endereco":         ("Endereco",         "dado_pessoal"),
    "logradouro":       ("Endereco",         "dado_pessoal"),
    "bairro":           ("Endereco",         "dado_pessoal"),
    # Dados sensíveis — LGPD Art. 5°, II
    "senha":            ("Senha",            "dado_pessoal_sensivel"),
    "password":         ("Senha",            "dado_pessoal_sensivel"),
    "hash_senha":       ("Senha",            "dado_pessoal_sensivel"),
    "saude":            ("DadoSaude",        "dado_pessoal_sensivel"),
    "diagnostico":      ("DadoSaude",        "dado_pessoal_sensivel"),
    "doenca":           ("DadoSaude",        "dado_pessoal_sensivel"),
    "raca":             ("OrigemEtnica",     "dado_pessoal_sensivel"),
    "etnia":            ("OrigemEtnica",     "dado_pessoal_sensivel"),
    "religiao":         ("CrencaReligiosa",  "dado_pessoal_sensivel"),
    "biometria":        ("DadoBiometrico",   "dado_pessoal_sensivel"),
    "impressao_digital": ("DadoBiometrico",  "dado_pessoal_sensivel"),
}


# ─────────────────────────────────────────────────────────────────────────────
# 1. Detecção por name hints
# ─────────────────────────────────────────────────────────────────────────────

def detect_by_name(col_name: str) -> Optional[tuple[str, str, str]]:
    """
    Retorna (pii_type, lgpd_class, confidence='high') se o nome da coluna
    contiver alguma das chaves de PII_NAME_HINTS.
    """
    lower = col_name.lower()
    for hint, (pii_type, lgpd_class) in PII_NAME_HINTS.items():
        if hint in lower:
            return pii_type, lgpd_class, "high"
    return None


# ─────────────────────────────────────────────────────────────────────────────
# 2. Detecção por regex sobre amostra de dados
# ─────────────────────────────────────────────────────────────────────────────

def detect_by_regex(
    sample_df: pd.DataFrame, col_name: str
) -> Optional[tuple[str, str, str, float]]:
    """
    Testa padrões PII em uma Series pandas.
    Retorna (pii_type, lgpd_class, confidence='medium', match_rate) ou None.
    """
    series = sample_df[col_name].dropna().astype(str)
    if series.empty:
        return None

    for pii_type, (pattern, lgpd_class) in PII_PATTERNS.items():
        matches = series.str.contains(pattern, regex=True, na=False)
        rate = matches.sum() / len(series)
        if rate >= REGEX_MATCH_THRESHOLD:
            return pii_type, lgpd_class, "medium", float(rate)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# 3. Tagging no Glue Data Catalog (column Parameters)
# ─────────────────────────────────────────────────────────────────────────────

def tag_glue_column(
    database: str,
    table_name: str,
    col_name: str,
    pii_type: str,
    lgpd_class: str,
    confidence: str,
) -> bool:
    """
    Atualiza os parâmetros da coluna no Glue Catalog.
    Retorna True se bem-sucedido.
    """
    try:
        resp = glue_client.get_table(DatabaseName=database, Name=table_name)
        table = resp["Table"]

        # Campos que o Glue rejeita no TableInput
        _EXCLUDE = {
            "DatabaseName", "CreateTime", "UpdateTime", "CreatedBy",
            "IsRegisteredWithLakeFormation", "CatalogId", "VersionId",
            "IsMultiDialectView", "Status", "ViewDefinition",
        }
        table_input = {k: v for k, v in table.items() if k not in _EXCLUDE}

        tagged = False
        pii_params = {
            "pii_type": pii_type,
            "lgpd_classification": lgpd_class,
            "pii_confidence": confidence,
            "pii_detected_by": "cloudmart-pii-scanner",
            "pii_tagged_at": datetime.now(timezone.utc).isoformat(),
        }

        # Atualiza colunas normais no StorageDescriptor
        for col in table_input.get("StorageDescriptor", {}).get("Columns", []):
            if col["Name"] == col_name:
                col.setdefault("Parameters", {}).update(pii_params)
                tagged = True

        # Atualiza partition keys, se aplicável
        for col in table_input.get("PartitionKeys", []):
            if col["Name"] == col_name:
                col.setdefault("Parameters", {}).update(pii_params)
                tagged = True

        if tagged:
            glue_client.update_table(DatabaseName=database, TableInput=table_input)
            logger.info(
                f"[Glue Catalog] Tagueado: {database}.{table_name}.{col_name} "
                f"→ {pii_type} ({lgpd_class})"
            )
        return tagged

    except Exception as exc:
        logger.error(
            f"[Glue Catalog] Falha ao taguear {database}.{table_name}.{col_name}: {exc}"
        )
        return False


# ─────────────────────────────────────────────────────────────────────────────
# 4. Tagging no Lake Formation (LF-Tags)
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_lf_tag(tag_key: str, tag_values: list[str]):
    """Cria a LF-Tag se não existir; adiciona valores novos se já existir."""
    try:
        existing = lf_client.get_lf_tag(TagKey=tag_key)
        existing_vals = set(existing.get("TagValues", []))
        new_vals = [v for v in tag_values if v not in existing_vals]
        if new_vals:
            lf_client.update_lf_tag(
                TagKey=tag_key,
                TagValuesToAdd=new_vals,
            )
    except lf_client.exceptions.EntityNotFoundException:
        lf_client.create_lf_tag(TagKey=tag_key, TagValues=tag_values)


def tag_lf_column(
    database: str,
    table_name: str,
    col_name: str,
    pii_type: str,
    lgpd_class: str,
) -> bool:
    """
    Aplica as LF-Tags PII e LGPD_Classification na coluna via Lake Formation.
    Retorna True se bem-sucedido.
    """
    try:
        _ensure_lf_tag("PII", [
            "CPF", "CNPJ", "Email", "Telefone", "NomePessoa",
            "DataNascimento", "Endereco", "RG", "CEP",
            "Senha", "DadoSaude", "OrigemEtnica", "CrencaReligiosa", "DadoBiometrico",
        ])
        _ensure_lf_tag("LGPD_Classification", [
            "dado_pessoal", "dado_pessoal_sensivel", "nao_pessoal",
        ])

        lf_client.add_lf_tags_to_resource(
            Resource={
                "TableWithColumns": {
                    "DatabaseName": database,
                    "Name": table_name,
                    "ColumnNames": [col_name],
                }
            },
            LFTags=[
                {"TagKey": "PII",                  "TagValues": [pii_type]},
                {"TagKey": "LGPD_Classification",  "TagValues": [lgpd_class]},
            ],
        )
        logger.info(
            f"[LF-Tags] Aplicado: {database}.{table_name}.{col_name} "
            f"→ PII={pii_type}, LGPD={lgpd_class}"
        )
        return True

    except Exception as exc:
        logger.error(
            f"[LF-Tags] Falha ao taguear {database}.{table_name}.{col_name}: {exc}"
        )
        return False


# ─────────────────────────────────────────────────────────────────────────────
# 5. Varredura de uma tabela
# ─────────────────────────────────────────────────────────────────────────────

def scan_table(
    database: str,
    table_name: str,
    s3_path: str,
) -> list[dict]:
    """
    Lê uma amostra da tabela e retorna a lista de achados PII.
    Cada achado: dict com database, table, column, pii_type, lgpd_class, confidence.
    """
    findings: list[dict] = []

    try:
        df_spark = spark.read.parquet(s3_path).limit(SAMPLE_ROWS)
        schema = df_spark.schema

        # Colunas string → candidatas a regex; todas → checam name hints
        string_cols = [f.name for f in schema if str(f.dataType) == "StringType()"]

        # Converte amostra de string columns para pandas (custo único)
        if string_cols:
            sample_pd = df_spark.select(*string_cols).toPandas()
        else:
            sample_pd = pd.DataFrame()

    except Exception as exc:
        logger.error(f"Não foi possível ler {s3_path}: {exc}")
        return findings

    all_cols = [f.name for f in schema]

    for col_name in all_cols:
        pii_type = lgpd_class = confidence = None
        match_rate = None

        # --- name hints (prioridade alta) ---
        hint_result = detect_by_name(col_name)
        if hint_result:
            pii_type, lgpd_class, confidence = hint_result

        # --- regex (complementar, só para strings ainda não detectadas) ---
        if pii_type is None and col_name in string_cols and not sample_pd.empty:
            regex_result = detect_by_regex(sample_pd, col_name)
            if regex_result:
                pii_type, lgpd_class, confidence, match_rate = regex_result

        if pii_type is None:
            continue  # coluna limpa

        finding = {
            "database":    database,
            "table":       table_name,
            "column":      col_name,
            "pii_type":    pii_type,
            "lgpd_class":  lgpd_class,
            "confidence":  confidence,
            "match_rate":  match_rate,  # None para name hints
        }
        findings.append(finding)

        # --- aplica tags ---
        tag_glue_column(database, table_name, col_name, pii_type, lgpd_class, confidence)
        tag_lf_column(database, table_name, col_name, pii_type, lgpd_class)

    return findings


# ─────────────────────────────────────────────────────────────────────────────
# 6. Enumeração de tabelas no Glue Catalog
# ─────────────────────────────────────────────────────────────────────────────

def list_tables(database: str) -> list[dict]:
    """Retorna lista de {name, location} de todas as tabelas do banco."""
    tables = []
    paginator = glue_client.get_paginator("get_tables")
    for page in paginator.paginate(DatabaseName=database):
        for t in page["TableList"]:
            location = (
                t.get("StorageDescriptor", {}).get("Location", "")
            )
            tables.append({"name": t["Name"], "location": location})
    return tables


# ─────────────────────────────────────────────────────────────────────────────
# 7. Relatório JSON em S3
# ─────────────────────────────────────────────────────────────────────────────

def publish_report(all_findings: list[dict]):
    now = datetime.now(timezone.utc)
    report = {
        "executed_at":   now.isoformat(),
        "databases":     [BRONZE_DB, SILVER_DB],
        "total_pii_cols": len(all_findings),
        "findings":      all_findings,
    }
    prefix = now.strftime("%Y/%m/%d")
    key = f"pii-reports/{prefix}/pii_report.json"
    boto3.client("s3").put_object(
        Bucket=REPORT_BUCKET,
        Key=key,
        Body=json.dumps(report, ensure_ascii=False, indent=2),
        ContentType="application/json",
    )
    logger.info(f"Relatório PII publicado em s3://{REPORT_BUCKET}/{key}")


# ─────────────────────────────────────────────────────────────────────────────
# 8. Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=== PII Scanner iniciado ===")
    logger.info(
        f"Bancos: {BRONZE_DB} (s3://{BRONZE_BUCKET}), "
        f"{SILVER_DB} (s3://{SILVER_BUCKET})"
    )
    logger.info(
        f"Parâmetros: SAMPLE_ROWS={SAMPLE_ROWS}, "
        f"REGEX_MATCH_THRESHOLD={REGEX_MATCH_THRESHOLD:.0%}"
    )

    # Mapeamento database → bucket raiz
    scope = [
        (BRONZE_DB, BRONZE_BUCKET),
        (SILVER_DB, SILVER_BUCKET),
    ]

    all_findings: list[dict] = []

    for database, bucket in scope:
        try:
            tables = list_tables(database)
        except Exception as exc:
            logger.error(f"Não foi possível listar tabelas de {database}: {exc}")
            continue

        logger.info(f"Varrendo {len(tables)} tabela(s) em '{database}'…")
        for tbl in tables:
            # Prefere a localização registrada no Catalog; faz fallback para
            # o padrão do projeto se a localização não estiver registrada.
            s3_path = tbl["location"] or f"s3://{bucket}/{tbl['name']}/"
            logger.info(f"  → {database}.{tbl['name']}  ({s3_path})")
            findings = scan_table(database, tbl["name"], s3_path)
            all_findings.extend(findings)
            if findings:
                for f in findings:
                    logger.info(
                        f"     PII detectado: coluna='{f['column']}' "
                        f"tipo={f['pii_type']} lgpd={f['lgpd_class']} "
                        f"confiança={f['confidence']}"
                    )
            else:
                logger.info("     Nenhuma PII encontrada.")

    publish_report(all_findings)

    total = len(all_findings)
    logger.info(
        f"=== PII Scanner concluído — {total} coluna(s) PII identificada(s) e tagueada(s) ==="
    )


if __name__ == "__main__":
    main()
