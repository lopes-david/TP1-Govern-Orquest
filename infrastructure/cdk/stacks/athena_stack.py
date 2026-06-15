"""
Stack CDK: Athena Self-Service Analytics

Provisiona recursos para que analistas de negócio possam executar
consultas SQL ad hoc sem depender da equipe de engenharia:

  1. S3 Bucket — destino dos resultados de queries (athena-results-cloudmart-{account})
     Criptografia SSE-S3, bloqueio de acesso público.

  2. Athena Workgroup — cloudmart-analysts
     • Output fixo no bucket acima (enforce = True)
     • Limite de 1 GB de dados escaneados por query (controle de custo)
     • Métricas CloudWatch habilitadas

  3. Named Queries (Saved Queries) — 6 consultas padrão prontas para uso:
     ┌──────────────────────────────────────────────────────────────────────┐
     │  Nome                            │ Banco          │ Propósito         │
     ├──────────────────────────────────┼────────────────┼───────────────────┤
     │  receita_por_categoria           │ cloudmart_gold │ KPIs de receita   │
     │  top10_produtos_mais_vendidos    │ cloudmart_silver│ Ranking produtos  │
     │  receita_por_canal_e_regiao      │ cloudmart_gold │ Canal x região    │
     │  clientes_por_segmento           │ cloudmart_bronze│ Segmentação CRM   │
     │  vendas_canceladas_por_mes       │ cloudmart_silver│ Churn/cancelamento│
     │  ticket_medio_por_categoria      │ cloudmart_gold │ Ticket médio      │
     └──────────────────────────────────────────────────────────────────────┘

  O acesso dos analistas é governado via Lake Formation (AccessControlStack):
  a coluna `cpf` é automaticamente ocultada de qualquer query na tabela clientes.
"""

from aws_cdk import (
    RemovalPolicy,
    Stack,
    aws_athena as athena,
    aws_s3 as s3,
)
from constructs import Construct

# Nomes das bases de dados do Glue Catalog (criadas pelos Crawlers do pipeline)
_DB_BRONZE = "cloudmart_bronze"
_DB_SILVER = "cloudmart_silver"
_DB_GOLD   = "cloudmart_gold"

# Nome do workgroup exportado para outros stacks (ex.: AccessControlStack)
WORKGROUP_NAME = "cloudmart-analysts"


class AthenaStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        account_id = self.node.try_get_context("account_id") or self.account

        # ── 1. Bucket de resultados ───────────────────────────────────────────
        self.results_bucket = s3.Bucket(
            self, "AthenaResultsBucket",
            bucket_name=f"athena-results-cloudmart-{account_id}",
            removal_policy=RemovalPolicy.RETAIN,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            versioned=False,
        )

        output_location = f"s3://{self.results_bucket.bucket_name}/query-results/"

        # ── 2. Workgroup para analistas ───────────────────────────────────────
        self.workgroup = athena.CfnWorkGroup(
            self, "AnalystWorkgroup",
            name=WORKGROUP_NAME,
            description=(
                "Workgroup para analistas de negócio CloudMart. "
                "Queries ad hoc em bronze/silver/gold via Athena + Lake Formation."
            ),
            state="ENABLED",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=output_location,
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_S3",
                    ),
                ),
                # 1 GB de dados escaneados por query — proteção contra full scans acidentais
                bytes_scanned_cutoff_per_query=1_073_741_824,
                # Impede que analistas sobrescrevam o output_location via SDK
                enforce_work_group_configuration=True,
                publish_cloud_watch_metrics_enabled=True,
            ),
        )

        # ── 3. Named Queries (Saved Queries) ─────────────────────────────────
        self._create_named_queries()

    # ─────────────────────────────────────────────────────────────────────────

    def _create_named_queries(self):
        queries = [
            # ── Gold mart ────────────────────────────────────────────────────
            {
                "id": "ReceitaPorCategoria",
                "name": "receita_por_categoria",
                "description": (
                    "Receita bruta total, pedidos e ticket médio agrupados por "
                    "categoria de produto (gold mart – pré-agregado)."
                ),
                "database": _DB_GOLD,
                "query": """\
-- Receita por categoria de produto (gold mart)
-- Fonte: cloudmart_gold.relatorio_vendas — pré-agregado pelo job silver_to_gold
SELECT
    categoria,
    SUM(receita_bruta)    AS receita_bruta_total,
    SUM(total_pedidos)    AS total_pedidos,
    SUM(total_unidades)   AS total_unidades_vendidas,
    ROUND(AVG(ticket_medio), 2) AS ticket_medio_geral
FROM relatorio_vendas
GROUP BY  categoria
ORDER BY  receita_bruta_total DESC;""",
            },
            {
                "id": "ReceitaPorCanalERegiao",
                "name": "receita_por_canal_e_regiao",
                "description": (
                    "Receita bruta por canal de venda e região geográfica "
                    "(gold mart). Útil para decisões de alocação de verba."
                ),
                "database": _DB_GOLD,
                "query": """\
-- Receita por canal de venda e região geográfica (gold mart)
SELECT
    canal_venda,
    nome_regiao,
    SUM(receita_bruta)         AS receita_bruta_total,
    SUM(total_pedidos)         AS total_pedidos,
    ROUND(AVG(ticket_medio), 2) AS ticket_medio
FROM relatorio_vendas
GROUP BY  canal_venda, nome_regiao
ORDER BY  receita_bruta_total DESC;""",
            },
            {
                "id": "TicketMedioPorCategoria",
                "name": "ticket_medio_por_categoria_e_mes",
                "description": (
                    "Evolução mensal do ticket médio por categoria (gold mart). "
                    "Série temporal para identificar sazonalidade e tendências."
                ),
                "database": _DB_GOLD,
                "query": """\
-- Ticket médio mensal por categoria (gold mart)
SELECT
    ano,
    mes,
    categoria,
    ROUND(AVG(ticket_medio), 2) AS ticket_medio,
    SUM(total_pedidos)          AS total_pedidos
FROM relatorio_vendas
GROUP BY  ano, mes, categoria
ORDER BY  ano DESC, mes DESC, categoria;""",
            },
            # ── Silver ───────────────────────────────────────────────────────
            {
                "id": "Top10ProdutosMaisVendidos",
                "name": "top10_produtos_mais_vendidos",
                "description": (
                    "Top 10 produtos por volume de unidades vendidas em "
                    "transações concluídas (silver)."
                ),
                "database": _DB_SILVER,
                "query": """\
-- Top 10 produtos por unidades vendidas (silver – vendas concluídas)
SELECT
    nome_produto,
    categoria,
    SUM(quantidade)    AS total_unidades,
    SUM(valor_total)   AS receita_bruta,
    COUNT(id_venda)    AS total_pedidos
FROM vendas
WHERE status_venda = 'Concluída'
GROUP BY  nome_produto, categoria
ORDER BY  total_unidades DESC
LIMIT 10;""",
            },
            {
                "id": "VendasCanceladasPorMes",
                "name": "vendas_canceladas_por_mes",
                "description": (
                    "Volume e valor monetário de vendas canceladas agrupados "
                    "por ano e mês (silver). Suporte a análise de churn."
                ),
                "database": _DB_SILVER,
                "query": """\
-- Vendas canceladas por mês (silver)
SELECT
    ano,
    mes,
    COUNT(id_venda)        AS total_canceladas,
    SUM(valor_total)       AS valor_cancelado,
    ROUND(AVG(valor_total), 2) AS ticket_medio_cancelado
FROM vendas
WHERE status_venda = 'Cancelada'
GROUP BY  ano, mes
ORDER BY  ano DESC, mes DESC;""",
            },
            # ── Bronze ───────────────────────────────────────────────────────
            {
                "id": "ClientesPorSegmento",
                "name": "clientes_por_segmento",
                "description": (
                    "Distribuição de clientes por segmento e estado (bronze). "
                    "A coluna cpf é omitida automaticamente pelo Lake Formation "
                    "para perfis sem permissão (LGPD Art. 6°)."
                ),
                "database": _DB_BRONZE,
                "query": """\
-- Clientes por segmento e estado (bronze)
-- LGPD: a coluna `cpf` é filtrada pelo Lake Formation para MarketingAnalystRole
SELECT
    segmento,
    estado,
    COUNT(id_cliente) AS total_clientes
FROM clientes
GROUP BY  segmento, estado
ORDER BY  total_clientes DESC;""",
            },
        ]

        for q in queries:
            athena.CfnNamedQuery(
                self, q["id"],
                name=q["name"],
                description=q["description"],
                database=q["database"],
                query_string=q["query"],
                work_group=self.workgroup.name,
            )
