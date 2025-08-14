-- Custom test to ensure listing prices are positive
select *
from {{ ref('silver_listings') }}
where list_price <= 0 and list_price is not null
