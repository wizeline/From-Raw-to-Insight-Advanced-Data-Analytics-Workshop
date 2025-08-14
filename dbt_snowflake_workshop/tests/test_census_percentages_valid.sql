-- Custom test to ensure census percentages are within valid ranges (0-100)
select *
from {{ ref('silver_census') }}
where (percent_housing_crowded < 0 or percent_housing_crowded > 100)
   or (percent_households_below_poverty < 0 or percent_households_below_poverty > 100)
   or (percent_unemployed < 0 or percent_unemployed > 100)
   or (percent_without_high_school < 0 or percent_without_high_school > 100)
   or (percent_dependent_population < 0 or percent_dependent_population > 100)
   or (hardship_index < 0 or hardship_index > 100)
