dq_sql = "SELECT COUNT(CASE WHEN {} IS NULL THEN 1 END) FROM {}"

immigration_insert = """
    (state, cit_country, res_country, female_count, visa_business_count, visa_pleasure_count, total_count, age_mean, stay_dur_mean, year, month, day, weekday)
    SELECT state, cit_country, res_country, is_female as female_count, visa_business as visa_business_count, visa_pleasure as visa_pleasure_count, count as total_count, age as age_mean, stay_dur as stay_dur_mean, year, month, day, weekday
    FROM staged_events
    ORDER BY month, day
"""

# NOTE check eco footprint and biocapacity numbers (rounding to ints)
country_insert = """
    SELECT cts.country, iso.code_2 as iso_code, cts.year, cgr.net_migration, cgr.growth_rate, rmt.remittance_millions as remittance, car.country_area as area, cpp.midyear_male_pop as male_pop_midyear, cpp.midyear_female_pop as female_pop_midyear, cpp.midyear_pop as pop_midyear, cec.hdi, cec.total_ecological_footprint as eco_footprint, cec.total_biocapacity as biocapacity, clg.num_lang, clg.official_lang, chp.happiness_rank, chp.happiness_score
    FROM (SELECT DISTINCT cit_country AS country, year
      FROM staged_events
      UNION
      SELECT DISTINCT res_country, year
      FROM staged_events) AS cts
    LEFT JOIN iso_country_codes AS iso
    ON cts.country = lower(iso.name)
    LEFT JOIN country_growth AS cgr
    ON cts.country = lower(cgr.country_name)
    LEFT JOIN remittance AS rmt
    ON cts.country = lower(rmt.country)
    LEFT JOIN country_area AS car
    ON cts.country = lower(car.country_name)
    LEFT JOIN country_pop AS cpp
    ON cts.country = lower(cpp.country_name)
    LEFT JOIN country_eco AS cec
    ON cts.country = lower(cec.country)
    LEFT JOIN country_lang AS clg
    ON cts.country = lower(clg.country)
    LEFT JOIN country_happiness AS chp
    ON cts.country = lower(chp.country)
    ORDER BY country
"""


state_insert = """
    SELECT sts.state, sts.year, stp.amounts as pop, sti.amounts as income_per_capita, sta.q1 as gdp_all_q1, stg.q1 as gdp_government_q1, stv.q1 as gdp_private_q1, sta.q2 as gdp_all_q2, stg.q2 as gdp_government_q2, stv.q2 as gdp_private_q2, sta.q3 as gdp_all_q3, stg.q3 as gdp_government_q3, stv.q3 as gdp_private_q3, sta.q4 as gdp_all_q4, stg.q4 as gdp_government_q4, stv.q4 as gdp_private_q4
FROM (SELECT DISTINCT state, year
	 FROM staged_events) AS sts
JOIN state_names AS snm
ON sts.state = snm.code
JOIN (SELECT state, amounts
      FROM state_income
      WHERE description = 'Population') AS stp
ON snm.state = stp.state
JOIN (SELECT state, amounts
      FROM state_income
      WHERE description = 'Per capita personal income dollars') AS sti
ON snm.state = sti.state
JOIN (SELECT state, q1, q2, q3, q4
      FROM state_gdp
      WHERE description = 'All industry total') as sta
ON snm.state = sta.state
JOIN (SELECT state, q1, q2, q3, q4
      FROM state_gdp
      WHERE description = 'Government and government enterprises') as stg
ON snm.state = stg.state
JOIN (SELECT state, q1, q2, q3, q4
      FROM state_gdp
      WHERE description = 'Private industries') as stv
ON snm.state = stv.state
ORDER BY state
"""

# income_per_capita in dollars
# all gdp in millions of dollars
# net_migration per 1k
# growth_rate in %
# remittance in millions of dollars
# area in square km
# num_lan = 9 --> <10
create_tables = """
    CREATE TABLE IF NOT EXISTS public.immigration_fact (
        id int IDENTITY(1,1) PRIMARY KEY,
        state char(2),
        cit_country varchar(64),
        res_country varchar(64),
        female_count smallint,
        visa_business_count smallint,
        visa_pleasure_count smallint,
        total_count smallint,
        age_mean numeric,
        stay_dur_mean numeric,
        year smallint,
        month smallint,
        day smallint,
        weekday char(3)
    );
    CREATE TABLE IF NOT EXISTS public.country_dim (
        country varchar(64) PRIMARY KEY,
        iso_code char(2),
        year smallint,
        net_migration numeric(4,2),
        growth_rate numeric(5,3),
        remittance numeric(16,10),
        area numeric,
        male_pop_midyear bigint,
        female_pop_midyear bigint,
        pop_midyear bigint,
        hdi numeric(3,2),
        eco_footprint numeric,
        biocapacity numeric,
        num_lang smallint,
        official_lang varchar(128),
        happiness_rank smallint,
        happiness_score numeric(4,3)
    );
    CREATE TABLE IF NOT EXISTS public.state_dim (
        state char(2) PRIMARY KEY,
        year smallint,
        pop int,
        income_per_capita numeric(9,1),
        gdp_all_q1 numeric(8,1),
        gdp_government_q1 numeric(8,1),
        gdp_private_q1 numeric(8,1),
        gdp_all_q2 numeric(8,1),
        gdp_government_q2 numeric(8,1),
        gdp_private_q2 numeric(8,1),
        gdp_all_q3 numeric(8,1),
        gdp_government_q3 numeric(8,1),
        gdp_private_q3 numeric(8,1),
        gdp_all_q4 numeric(8,1),
        gdp_government_q4 numeric(8,1),
        gdp_private_q4 numeric(8,1)
    );
    CREATE TABLE IF NOT EXISTS public.staged_events (
        state char(2),
        cit_country varchar(64),
        res_country varchar(64),
        is_female numeric,
        visa_business numeric,
        visa_pleasure numeric,
        age numeric(7,4),
        stay_dur numeric,
        count numeric,
        year smallint,
        month smallint,
        day smallint,
        weekday char(3),
        date date
    );
    CREATE TABLE IF NOT EXISTS public.country_growth (
        country_code char(2),
        country_name varchar(64),
        year smallint,
        crude_birth_rate numeric(4,2),
        crude_death_rate numeric(4,2),
        net_migration numeric(4,2),
        rate_natural_increase numeric(5,3),
        growth_rate numeric(5,3)
    );
    CREATE TABLE IF NOT EXISTS public.country_area (
        country_code char(2),
        country_name varchar(64),
        country_area numeric
    );
    CREATE TABLE IF NOT EXISTS public.country_pop (
        country_code char(2),
        country_name varchar(64),
        year smallint,
        midyear_male_pop bigint,
        midyear_female_pop bigint,
        midyear_pop bigint
    );
    CREATE TABLE IF NOT EXISTS public.country_eco (
        country varchar(64),
        region varchar(64),
        population numeric,
        hdi numeric(3,2),
        gdp_per_capita numeric,
        cropland_footprint numeric,
        grazing_footprint numeric,
        forest_footprint numeric,
        carbon_footprint numeric,
        fish_footprint numeric,
        total_ecological_footprint numeric,
        cropland numeric,
        grazing_land numeric,
        forest_land numeric,
        fishing_water numeric,
        urban_land numeric,
        total_biocapacity numeric,
        biocapacity_deficit_or_reserve numeric,
        earths_required numeric,
        countries_required numeric,
        data_quality char(2)
    );
    CREATE TABLE IF NOT EXISTS public.country_lang (
        country varchar(64),
        num_lang smallint,
        official_lang varchar(128)
    );
    CREATE TABLE IF NOT EXISTS public.country_happiness (
        country varchar(64),
        region varchar(64),
        happiness_rank smallint,
        happiness_score numeric(4,3),
        lower_ci numeric(4,3),
        upper_ci numeric(4,3),
        economy numeric(6,5),
        family numeric(6,5),
        health numeric(6,5),
        freedom numeric(6,5),
        trust_govt_corruption numeric(6,5),
        generosity numeric(6,5),
        dystopia_residual numeric(6,5)
    );
    CREATE TABLE IF NOT EXISTS public.iso_country_codes (
        name varchar(64),
        code_2 char(2),
        code_3 char(3),
        code_num smallint
    );
    CREATE TABLE IF NOT EXISTS public.remittance (
        country varchar(64),
        remittance_millions numeric(16,10)
    );
    CREATE TABLE IF NOT EXISTS public.state_gdp (
        state varchar(32),
        description varchar(64),
        q1 numeric(8,1),
        q2 numeric(8,1),
        q3 numeric(8,1),
        q4 numeric(8,1)
    );
    CREATE TABLE IF NOT EXISTS public.state_income (
        state varchar(32),
        description varchar(64),
        amounts numeric(9,1)
    );
    CREATE TABLE IF NOT EXISTS public.state_names (
        state varchar(32),
        abbrev varchar(32),
        code char(2)
    );
"""
