create_tables = """
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
        "Country" varchar(64),
        "Region" varchar(64),
        "Population" numeric,
        "HDI" numeric(3,2),
        "GDP per Capita" numeric,
        "Cropland Footprint" numeric,
        "Grazing Footprint" numeric,
        "Forest Footprint" numeric,
        "Carbon Footprint" numeric,
        "Fish Footprint" numeric,
        "Total Ecological Footprint" numeric,
        "Cropland" numeric,
        "Grazing Land" numeric,
        "Forest Land" numeric,
        "Fishing Water" numeric,
        "Urban Land" numeric,
        "Total Biocapacity" numeric,
        "Biocapacity Deficit or Reserve" numeric,
        "Earths Required" numeric,
        "Countries Required" numeric,
        "Data Quality" char(2)
    );
    CREATE TABLE IF NOT EXISTS public.country_lang (
        country varchar(64),
        num_lang smallint,
        official_lang varchar(128)
    );
    CREATE TABLE IF NOT EXISTS public.country_happiness (
        "Country" varchar(64),
        "Region" varchar(64),
        "Happiness Rank" smallint,
        "Happiness Score" numeric(4,3),
        "Lower Confidence Interval" numeric(4,3),
        "Upper Confidence Interval" numeric(4,3),
        "Economy" numeric(6,5),
        "Family" numeric(6,5),
        "Health Life Expectancy" numeric(6,5),
        "Freedom" numeric(6,5),
        "Trust Government Corruption" numeric(6,5),
        "Generosity" numeric(6,5),
        "Dystopia Residual" numeric(6,5)
    );
    CREATE TABLE IF NOT EXISTS public.iso_country_codes (
        name varchar(64),
        code_2 char(2),
        code_3 char(3),
        code_num smallint,
        "ISO 3166-2" char(13)
    );
    CREATE TABLE IF NOT EXISTS public.remittance (
        country varchar(64),
        remittance_millions numeric(16,10)
    );
    CREATE TABLE IF NOT EXISTS public.state_gdp (
        state varchar(32),
        description varchar(64),
        "Q1" numeric(8,1),
        "Q2" numeric(8,1),
        "Q3" numeric(8,1),
        "Q4" numeric(8,1)
    );
    CREATE TABLE IF NOT EXISTS public.state_income (
        state varchar(32),
        description varchar(64),
        amounts numeric(9,1)
    );
    """
