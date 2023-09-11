-- Databricks notebook source
-- Glucose effect on Stroke probability
select
  round(avg_glucose_level / 20, 0) * 20 as glucose,
  sum(stroke) as n_events,
  count(id) as observations,
  round(sum(stroke) / count(id), 2) as incidence_rate
from
  fact_healthcare_clean
group by
  1
having
  count(id) > 3
order by
  1 asc

-- COMMAND ----------

-- BMI and Smoking Level -> "Previously smoked" has the highest incidence
select
  round(bmi / 5, 0) * 5 as BMI,
  case
    when smoking_level <=.33 then 'Never Smoked'
    when smoking_level <=.66 then 'Previously Smoked'
    else 'Currently Smokes'
  end as smoking_level,
  sum(stroke) as n_events,
  count(id) as observations,
  round(sum(stroke) / count(id), 2) as incidence_rate
from
  fact_healthcare_clean
group by
  1,
  2
having
  count(id) > 10
order by
  1,
  2 asc

-- COMMAND ----------

-- Heart disease x Age
with events as (
  select
    age,
    heart_disease,
    sum(stroke) as n_events
  from
    fact_healthcare_clean
  group by
    1,
    2
),
totals as (
  select
    heart_disease,
    count(*) as group_total
  from
    fact_healthcare_clean
  group by
    1
)
select
  a.age,
  case
    when a.heart_disease = 1 then 'present'
    else 'absent'
  end as heart_disease,
  sum(b.n_events) / group_total as relative_incidence
from
  events a
  left join events b on b.age <= a.age
  and a.heart_disease = b.heart_disease
  left join totals t on a.heart_disease = t.heart_disease
group by
  1,
  2,
  group_total
order by
  2,
  1

-- COMMAND ----------

-- Simple Age impact (5 years age buckets)
select 
          round(age/5,0)*5 as age, 
          sum(stroke) as n_events,
          count(id) as observations,
          round(sum(stroke) / count(id),2) as incidence_rate
          from fact_healthcare_clean
          group by 1    
          having count(id) > 3
          order by 1 asc
