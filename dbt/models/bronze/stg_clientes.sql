select
    cast(id_cliente      as integer) as id_cliente,
    nome_cliente,
    lower(email)                     as email,
    segmento,
    upper(estado)                    as estado,
    cast(id_regiao       as integer) as id_regiao,
    cast(data_cadastro   as date)    as data_cadastro,
    ingestion_ts
from {{ source('bronze', 'clientes') }}
where id_cliente is not null
