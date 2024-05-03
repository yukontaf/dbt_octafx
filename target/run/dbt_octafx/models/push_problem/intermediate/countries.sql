
  
    

    create or replace table `analytics-147612`.`dev_gsokolov`.`countries`
      
    
    

    OPTIONS()
    as (
      

SELECT
    c.country,
    c.code,
    ct.tier,
    concat("Tier ", ct.tier_id) AS tier_name
FROM `analytics-147612`.`wh_raw`.`countries` AS c
LEFT JOIN
    `analytics-147612`.`wh_raw`.`countries_tiers` AS ct
    ON c.country = ct.country
    );
  