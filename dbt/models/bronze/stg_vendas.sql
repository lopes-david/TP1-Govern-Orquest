select
    cast(id_venda        as integer)       as id_venda,
    cast(data_venda      as date)          as data_venda,
    cast(ano             as integer)       as ano,
    cast(mes             as integer)       as mes,
    cast(id_produto      as integer)       as id_produto,
    cast(id_cliente      as integer)       as id_cliente,
    cast(id_regiao       as integer)       as id_regiao,
    cast(quantidade      as integer)       as quantidade,
    cast(valor_unitario  as decimal(18,2)) as valor_unitario,
    cast(desconto        as decimal(5,4))  as desconto,
    cast(valor_total     as decimal(18,2)) as valor_total,
    canal_venda,
    status_venda,
    ingestion_ts
from {{ source('bronze', 'vendas') }}
where id_venda is not null
