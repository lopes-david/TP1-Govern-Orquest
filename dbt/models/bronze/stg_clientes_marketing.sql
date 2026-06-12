{{
  config(
    materialized = 'table',
    tags         = ['bronze', 'marketing', 'pii_masked'],
  )
}}

{#
  Visão de clientes para o perfil "Analista de Marketing".

  Por que cpf = NULL aqui?
  ─────────────────────────────────────────────────────────────────────────
  LGPD Art. 6°, III  — princípio da necessidade: o acesso deve se limitar
    ao mínimo necessário para a finalidade legítima da atividade.
  LGPD Art. 46        — medidas técnicas e administrativas de segurança
    aptas a proteger dados pessoais de acessos não autorizados.

  O Analista de Marketing precisa de dados demográficos (segmento, estado,
  canal_venda) e de contato (e-mail) para campanhas, mas NÃO tem necessidade
  legítima de acesso ao CPF. A substituição por NULL impede o vazamento mesmo
  que a query seja exportada ou copiada por engano.

  Mecanismo duplo de proteção:
    1. Este modelo dbt → materializa a visão mascarada no catálogo silver
    2. Lake Formation Data Cells Filter (access_control_stack.py) →
       exclui a coluna `cpf` diretamente no Glue Catalog para a IAM Role
       MarketingAnalystRole, impedindo acesso à tabela original bronze.clientes
       mesmo via Athena ou SDK direto.
#}
select
    id_cliente,
    nome_cliente,
    email,
    cast(null as varchar(14))  as cpf,
    segmento,
    estado,
    id_regiao,
    data_cadastro,
    ingestion_ts
from {{ ref('stg_clientes') }}
