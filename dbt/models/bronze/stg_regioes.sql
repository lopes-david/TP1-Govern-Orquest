select
    cast(id_regiao  as integer) as id_regiao,
    nome_regiao
from {{ source('bronze', 'regioes') }}
where id_regiao is not null
