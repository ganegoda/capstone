
create_table_immigration_staging = """
CREATE TABLE IF NOT EXISTS public.immigration_stg (
    cicid FLOAT,
    i94yr FLOAT ,
    i94mon FLOAT ,
    i94cit FLOAT,
    i94res FLOAT ,
    i94port CHAR(3),
    arrdate FLOAT,
    i94mode FLOAT,
    i94addr VARCHAR ,
    depdate FLOAT,
    i94bir FLOAT,
    i94visa FLOAT,
    count FLOAT,
    dtadfile VARCHAR(20),
    visapost CHAR(3),
    occup CHAR(3),
    entdepa CHAR(1),
    entdepd CHAR(1),
    entdepu CHAR(1),
    matflag CHAR(1),
    biryear FLOAT,
    dtaddto VARCHAR(20),
    gender CHAR(1),
    insnum VARCHAR(20),
    airline VARCHAR(20),
    admnum FLOAT,
    fltno VARCHAR(20),
    visatype VARCHAR(10)
)
DISTSTYLE EVEN
"""
# staging
create_us_city_demo_staging = """
CREATE TABLE IF NOT EXISTS public.us_city_demo_stg (
    city VARCHAR(255),
    state VARCHAR(50),
    median_age FLOAT,
    male_population FLOAT,
    female_population FLOAT,
    total_population FLOAT,
    number_of_veterans FLOAT,
    foreign_born FLOAT,
    average_household_size FLOAT,
    state_code CHAR(2),
    race VARCHAR(50),
    count INTEGER
)
DISTSTYLE AUTO
"""
# staging
create_airport_codes_staging = """
CREATE TABLE IF NOT EXISTS public.airport_codes_stg (
    ident VARCHAR(10),
    type VARCHAR(50),
    name VARCHAR(255),
    elevation_ft FLOAT,
    continent VARCHAR(50),
    iso_country VARCHAR(50),
    iso_region VARCHAR(50),
    municipality VARCHAR(100),
    gps_code VARCHAR(50),
    iata_code VARCHAR(50),
    local_code VARCHAR(50),
    coordinates VARCHAR(255)
)
DISTSTYLE AUTO
"""
# staging
create_world_temp_staging = """
CREATE TABLE IF NOT EXISTS public.world_temp_stg (
    dt DATE SORTKEY DISTKEY,
    avg_temp FLOAT,
    avg_temp_error FLOAT,
    city VARCHAR(100),
    country VARCHAR(50),
    latitude VARCHAR(10),
    longitude VARCHAR(10)
)
DISTSTYLE KEY
"""


# ----------------dim from SAS file----------------------------------
create_country = """
CREATE TABLE IF NOT EXISTS country (
    country_code INTEGER PRIMARY KEY,
    country VARCHAR(255)
)
DISTSTYLE ALL;
"""

create_entry_port = """
CREATE TABLE IF NOT EXISTS entry_port (
    port_code CHAR(3) PRIMARY KEY,
    addr VARCHAR(255),
    city VARCHAR(50),
    state VARCHAR(50)
)
DISTSTYLE ALL;
"""

create_arrival_mode = """
CREATE TABLE IF NOT EXISTS arrival_mode (
    arrival_code INTEGER PRIMARY KEY,
    arrival_type VARCHAR(20)
)
DISTSTYLE ALL;
"""

create_region = """
CREATE TABLE IF NOT EXISTS region (
    region_code CHAR(2) PRIMARY KEY,
    region_name VARCHAR(20)
)
DISTSTYLE ALL;
"""

create_visa_type = """
CREATE TABLE IF NOT EXISTS visa_type (
    visa_code INTEGER PRIMARY KEY,
    visa_type VARCHAR(20)
)
DISTSTYLE ALL;
"""

# ----------------dim from manually collected data CSVs-------------------
create_gender_type = """
CREATE TABLE IF NOT EXISTS gender (
    gender_code VARCHAR(1) PRIMARY KEY,
    gender VARCHAR(20)
)
DISTSTYLE ALL
"""

create_visa_class = """
CREATE TABLE IF NOT EXISTS visa (
    visa_class VARCHAR(10) PRIMARY KEY,
    visa_desc VARCHAR(255)
)
DISTSTYLE ALL
"""

# immigrations fact table
create_table_immigration = """
CREATE TABLE IF NOT EXISTS immigration (
    immigration_id BIGINT IDENTITY(1, 1),
    im_year INTEGER SORTKEY,
    im_month INTEGER DISTKEY,
    im_date INTEGER,
    citizenship INTEGER REFERENCES country(country_code),
    residence INTEGER REFERENCES country(country_code),
    entry_port CHAR(3) REFERENCES entry_port(port_code),
    arr_date DATE,
    arr_mode INTEGER REFERENCES arrival_mode(arrival_code),
    address VARCHAR REFERENCES region(region_code),
    dep_date DATE,
    age_at_arr INTEGER,
    visa_type INTEGER REFERENCES visa_type(visa_code),
    count INTEGER,
    date_file VARCHAR,
    visa_post CHAR(3),
    occupation CHAR(3),
    arrival_flag CHAR(1),
    departure_flag CHAR(1),
    update_flag CHAR(1),
    match_flag CHAR(1),
    birth_year INTEGER,
    admission_date DATE,
    gender CHAR(1) REFERENCES gender(gender_code),
    ins_num VARCHAR(20),
    airline VARCHAR(20),
    admission_number FLOAT,
    flight_no VARCHAR(20),
    visa_class VARCHAR(10) REFERENCES visa(visa_class)
)
DISTSTYLE KEY
"""


table_list = ['immigration', 'immigration_stg', 'us_city_demo_stg', 'airport_codes_stg', 'world_temp_stg',
              'country', 'entry_port', 'arrival_mode', 'region', 'visa_type',
              'gender', 'visa']

drop_table_template = "DROP TABLE IF EXISTS public.{}"
truncate_table_template = "TRUNCATE TABLE IF EXISTS public.{}"


# delete, create, turncate all tables
drop_table_queries = [drop_table_template.format(
    table) for table in table_list]

create_table_queries = [create_table_immigration_staging, 
                        create_us_city_demo_staging, 
                        create_airport_codes_staging, 
                        create_world_temp_staging,
                        create_country, create_entry_port, 
                        create_arrival_mode, create_region,
                        create_visa_type, create_gender_type, 
                        create_visa_class, create_table_immigration
                        ]

truncate_table_queries = [
    truncate_table_template.format(table) for table in table_list]
