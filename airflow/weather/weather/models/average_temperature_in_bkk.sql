-- select avg(temp) from public.weathers
select 
    avg(temp) as avg_temp 

from {{ ref('stg_weathers') }}

-- select 31.39 as avg_temp