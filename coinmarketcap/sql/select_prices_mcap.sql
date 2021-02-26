select 
cmc_id, 
symbol,
json_data.key::DATE as dt,
(json_data.value->'USD'->>0)::numeric as price, 
(json_data.value->'USD'->>2)::numeric as mcap
from coinmarketcap_daily cmc,
json_each(cmc.fixed_data) as json_data
where cmc_id in (1027,4679,1697,2011,1042,1518,5692,2586,1975,2539,2943,4944,4846)
and json_data.key::DATE >= '2021-01-01'