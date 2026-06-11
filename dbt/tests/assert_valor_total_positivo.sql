-- Nenhuma venda concluída pode ter valor_total negativo
select id_venda, valor_total
from {{ ref('int_vendas_enriched') }}
where status_venda = 'Concluída'
  and valor_total < 0
