-- Custom test to ensure crime incident dates are valid
select *
from {{ ref('silver_crime') }}
where incident_date is not null 
  and (incident_date < '2000-01-01' or incident_date > current_date())
