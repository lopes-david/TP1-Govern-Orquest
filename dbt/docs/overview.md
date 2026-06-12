{% docs cloudmart_project %}
# CloudMart Data Platform — Visão Geral

CloudMart é uma plataforma de e-commerce B2C/B2B que opera nos canais
**online, loja física, televendas e parceiros**. Este projeto dbt transforma
os dados transacionais em camadas analíticas seguindo a **arquitetura medallion**
(Bronze → Silver → Gold).

## Arquitetura de Dados

```
Raw Zone (CSV)
    │  Glue Job: raw_to_bronze
    ▼
Bronze Zone (Parquet)          ← fontes dbt (sources.yml)
    │  dbt models: stg_*
    ▼
Silver Zone (Parquet)          ← int_vendas_enriched
    │  dbt models: int_*
    ▼
Gold Zone (Parquet)            ← mart_relatorio_vendas
      dbt models: mart_*       ← CONSUMO RECOMENDADO para BI/relatórios
```

## Quem usa o quê

| Perfil                  | Modelo recomendado             | Acesso via        |
|-------------------------|--------------------------------|-------------------|
| Analista de BI          | `mart_relatorio_vendas`        | Athena / QuickSight |
| Engenheiro de Dados     | `int_vendas_enriched`          | Athena            |
| Analista de Marketing   | `stg_clientes_marketing`       | Athena (CPF oculto via Lake Formation) |
| Cientista de Dados      | `int_vendas_enriched`          | Athena / SageMaker |

## Governança e LGPD

Colunas com dados pessoais são marcadas com a tag `pii_type` no Glue Catalog
e com LF-Tags `PII` e `LGPD_Classification` no Lake Formation.
Veja [stg_clientes_marketing](#model.cloudmart_dbt.stg_clientes_marketing)
para o modelo com CPF mascarado destinado ao perfil de marketing.

## Pipeline de Atualização

O pipeline é orquestrado pelo AWS Step Functions (`cloudmart-data-pipeline`)
e executado diariamente. Os modelos dbt são materializados como tabelas Parquet
no S3 e catalogados no AWS Glue Data Catalog para consulta via Athena.
{% enddocs %}


{% docs col_ingestion_ts %}
Timestamp de quando o registro foi ingerido na bronze-zone pelo Glue Job
`raw_to_bronze.py`. Útil para auditoria de pipeline e rastreamento de latência.
Formato: `TIMESTAMP` (fuso UTC). Não representa a data do evento de negócio —
para isso, use `data_venda` ou `data_cadastro`.
{% enddocs %}


{% docs col_id_venda %}
Identificador único da venda na plataforma CloudMart.
Chave primária da tabela de fatos. Gerado pelo sistema transacional de origem
(inteiro sequencial). Nunca reutilizado, mesmo para vendas canceladas.
{% enddocs %}


{% docs col_id_cliente %}
Identificador único do cliente. Chave estrangeira para a dimensão `clientes`.
Um mesmo cliente pode ter múltiplas vendas (relação 1:N).
{% enddocs %}


{% docs col_id_produto %}
Identificador único do produto vendido. Chave estrangeira para `produtos`.
Representa o SKU no momento da venda — se o produto for descontinuado
ele ainda aparece nas vendas históricas.
{% enddocs %}


{% docs col_status_venda %}
Estado atual da venda no ciclo de vida transacional.

| Valor           | Significado                                      |
|-----------------|--------------------------------------------------|
| `Em andamento`  | Pedido recebido, aguardando pagamento/separação  |
| `Concluída`     | Entregue e confirmada — receita reconhecida      |
| `Cancelada`     | Revertida antes da entrega — não gera receita    |

**Importante:** métricas financeiras (receita, ticket médio) devem filtrar
apenas `Concluída`. O mart `mart_relatorio_vendas` já aplica esse filtro.
{% enddocs %}


{% docs col_canal_venda %}
Canal pelo qual a venda foi originada. Usado para análises de mix de canal
e atribuição de campanha.

Valores possíveis: `Online`, `Loja Física`, `Televendas`, `Parceiros`.
{% enddocs %}


{% docs col_valor_total %}
Valor monetário total da venda após aplicação de desconto.
Calculado como: `quantidade × valor_unitario × (1 − desconto)`.
Sempre ≥ 0 na camada silver (registros com valor negativo são descartados
no Glue Job `bronze_to_silver_vendas.py`).
Moeda: BRL (Real brasileiro). Precisão: DECIMAL(18,2).
{% enddocs %}


{% docs col_desconto %}
Percentual de desconto concedido sobre o valor unitário.
Formato: decimal entre 0.0000 e 1.0000 (ex: 0.1500 = 15% de desconto).
{% enddocs %}


{% docs col_cpf %}
CPF (Cadastro de Pessoas Físicas) do cliente.
**Dado pessoal — LGPD Art. 5°.** Acesso restrito via Lake Formation:

- Perfil `MarketingAnalystRole` recebe `NULL` nesta coluna (Data Cells Filter).
- Para uso legítimo consulte o responsável pela área de dados.
- Modelo com dado mascarado: `stg_clientes_marketing`.

Formato armazenado: string, podendo conter máscara `NNN.NNN.NNN-NN` ou
apenas dígitos `NNNNNNNNNNN` dependendo da origem.
{% enddocs %}


{% docs col_segmento %}
Segmento de mercado ao qual o cliente pertence, utilizado para segmentação
de campanhas e análises de comportamento de compra.

Valores possíveis: `Varejo`, `Corporativo`, `Governo`, `SMB`.
{% enddocs %}


{% docs col_is_alto_valor %}
Flag boolean que indica se a venda é classificada como "alto valor".
Critério: `valor_total > R$ 10.000`.
Usado para priorização logística, análise de clientes premium e
segmentação de ofertas personalizadas.
{% enddocs %}


{% docs col_receita_bruta %}
Soma do `valor_total` de todas as vendas **Concluídas** no período.
Representa a receita bruta confirmada (sem deduções de devoluções ou impostos).
Moeda: BRL. Precisão: DECIMAL(18,2).
{% enddocs %}


{% docs col_ticket_medio %}
Média do `valor_total` por venda concluída no período e agrupamento.
Indicador de saúde do negócio: ticket médio crescente indica upsell eficaz.
Calculado como: `receita_bruta / total_pedidos`.
{% enddocs %}
