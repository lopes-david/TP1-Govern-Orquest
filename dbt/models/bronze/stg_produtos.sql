select
    cast(id_produto      as integer)       as id_produto,
    nome_produto,
    categoria,
    cast(preco_unitario  as decimal(18,2)) as preco_unitario,
    fornecedor,
    case upper(trim(ativo)) when 'S' then true else false end as ativo,
    ingestion_ts
from {{ source('bronze', 'produtos') }}
where id_produto is not null
