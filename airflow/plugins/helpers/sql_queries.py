class SqlQueries:
    """ Defines sql queries for data cleaning and loading.
    Attributes:
        clean_airport_codes_stg (str): Data clean query for airport_codes_stg
        clean_us_city_demo_stg (str): Data clean query for us_city_demo_stg
        clean_world_temp_stg (str): Data clean query for world_temp_stg
        clean_load_immigration (str): Data clean/load query for immigration table
        clean_sql (obj:`dict`): Dictionary of data cleaning queries
    """

    clean_airport_codes_stg = (""" 
        ALTER TABLE public.airport_codes_stg ADD latitude FLOAT;

        ALTER TABLE public.airport_codes_stg ADD longitude FLOAT;

        UPDATE public.airport_codes_stg 
            SET latitude = CAST(split_part(coordinates, ',' , 2) AS FLOAT), 
                longitude = CAST(split_part(coordinates, ',' , 1) AS FLOAT);
        
        UPDATE public.airport_codes_stg
            SET elevation_ft = COALESCE(elevation_ft, -99999),
                continent = COALESCE(continent, 'NA'),
                iso_country = COALESCE(iso_country, '-'),
                iso_region = COALESCE(iso_region, '-'),
                gps_code = COALESCE(gps_code, '-'),
                iata_code = COALESCE(iata_code, '-'),
                local_code = COALESCE(continent, '-');
    """)

    clean_us_city_demo_stg = ("""
        UPDATE public.us_city_demo_stg
            SET male_population = COALESCE(male_population,0),
                female_population = COALESCE(female_population, 0),
                number_of_veterans = COALESCE(number_of_veterans, 0),
                foreign_born = COALESCE(foreign_born, 0),
                average_household_size = COALESCE(average_household_size, 0);   
    """)

    clean_world_temp_stg = ("""
        UPDATE public.world_temp_stg
            SET avg_temp = COALESCE(avg_temp, -999),
                avg_temp_error = COALESCE(avg_temp, -999);

        ALTER TABLE public.world_temp_stg ADD latitude_num FLOAT;
        ALTER TABLE public.world_temp_stg ADD longitude_num FLOAT;

        UPDATE public.world_temp_stg 
        SET 
            latitude_num = CASE
                            WHEN right(trim(latitude),1)='N' THEN CAST(left(trim(latitude), len(trim(latitude))-1) AS FLOAT)*1
                            WHEN right(trim(latitude),1)='S' THEN CAST(left(trim(latitude), len(trim(latitude))-1) AS FLOAT)*-1
                            END, 
            longitude_num = CASE
                            WHEN right(trim(longitude),1)='E' THEN CAST(left(trim(longitude), len(trim(longitude))-1) AS FLOAT)*1
                            WHEN right(trim(longitude),1)='W' THEN CAST(left(trim(longitude), len(trim(longitude))-1) AS FLOAT)*-1  
                            END;
    """)

    # INSERT INTO TABLE_NAME filled out inside operator
    clean_load_immigration = ("""
            (im_year, im_month, im_date, citizenship,
            residence, entry_port, arr_date, 
            arr_mode, address, dep_date,
            age_at_arr, visa_type, count, 
            date_file, visa_post, occupation,
            arrival_flag, departure_flag, update_flag, 
            match_flag, birth_year, admission_date, 
            gender, ins_num, airline, 
            admission_number, flight_no, visa_class)
        SELECT 
            CAST(i94yr as INTEGER), CAST(i94mon as INTEGER), 
            EXTRACT(DAY from DATEADD(day, cast(arrdate as integer), '1960-01-01')), CAST(i94cit as INTEGER),
            CAST(i94res as INTEGER), i94port, DATEADD(day, cast(arrdate as integer), '1960-01-01') as arrdate, 
            CAST(COALESCE(i94mode, 9) as INTEGER), COALESCE(i94addr, '99'), DATEADD(day, cast(COALESCE(depdate, 0 ) as integer), '1960-01-01'),
            CAST(COALESCE(i94bir, -1) as INTEGER), CAST(i94visa as INTEGER), CAST(count as INTEGER), 
            TO_DATE(COALESCE(dtadfile, '1900101'), 'YYYYMMDD') AS date_file, COALESCE(TRIM(visapost), 'UUU'), COALESCE(occup, '-'),
            COALESCE(entdepa, '-'), COALESCE(entdepd, '-'),COALESCE(entdepu, '-'),
            COALESCE(matflag, '-'), CAST(COALESCE(biryear, 1900) as INTEGER), 
            CASE WHEN REGEXP_COUNT(dtaddto, '^[0-9]+$')  AND LEN(dtaddto)=8 THEN TO_DATE(dtaddto, 'MMDDYYYY') ELSE '1900-01-01' END as admission_date,
            COALESCE(gender, 'U'), COALESCE(insnum, '0'), COALESCE(airline, '-'),
            admnum, COALESCE(fltno, '-'), TRIM(visatype)                                                 
        FROM public.immigration_stg;
    """)

    clean_sql = {'public.airport_codes_stg': clean_airport_codes_stg,
                 'public.us_city_demo_stg': clean_us_city_demo_stg, 'public.world_temp_stg': clean_world_temp_stg}
