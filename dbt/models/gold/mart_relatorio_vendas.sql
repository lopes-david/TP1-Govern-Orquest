-- Mart analítico: KPIs de vendas por categoria, região e canal
-- Fonte de verdade para dashboards e relatórios gerenciais
with base as (
    select * from {{ ref('int_vendas_enriched') }}
    where status_venda = 'Concluída'
)

select
    categoria,
    nome_regiao,
    canal_venda,
    ano,
    mes,
    count(id_venda)        as total_pedidos,
    sum(quantidade)        as total_unidades,
    sum(valor_total)       as receita_bruta,
    avg(valor_total)       as ticket_medio,
    max(valor_total)       as maior_venda,
    min(valor_total)       as menor_venda,
    current_timestamp      as _refreshed_at
from base
group by 1, 2, 3, 4, 5
order by receita_bruta desc
