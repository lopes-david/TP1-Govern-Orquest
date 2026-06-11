with vendas as (
    select * from {{ ref('stg_vendas') }}
    where quantidade > 0
      and valor_total >= 0
      and status_venda in ('Concluída', 'Cancelada', 'Em andamento')
),

clientes as (select * from {{ ref('stg_clientes') }}),
produtos as (select * from {{ ref('stg_produtos') }}),
regioes  as (select * from {{ ref('stg_regioes') }})

select
    v.id_venda,
    v.data_venda,
    v.ano,
    v.mes,
    v.id_produto,
    p.nome_produto,
    p.categoria,
    v.id_cliente,
    c.nome_cliente,
    c.segmento,
    c.estado,
    v.id_regiao,
    r.nome_regiao,
    v.canal_venda,
    v.status_venda,
    v.quantidade,
    v.valor_unitario,
    v.desconto,
    v.valor_total,
    case when v.valor_total > 10000 then true else false end as is_alto_valor
from vendas v
left join clientes c using (id_cliente)
left join produtos p using (id_produto)
left join regioes  r using (id_regiao)
