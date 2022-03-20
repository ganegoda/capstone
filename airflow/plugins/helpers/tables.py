
class Tables:
    """ Custom operator parsing SAS file and loading to tables.
    Attributes:
        csv_parq_tables (obj:`dict'): Dictionary for tables, test definition
        csv_stg_tables (obj:`dict'): Dictionary for tables, test definition
        parq_immigration (obj:`dict'): Dictionary for tables, test definition
        sas_tables (obj:`dict'): Dictionary for tables, test definition
    """
    
    csv_parq_tables = [
        {'table_name': 'public.gender',
            'file_type': 'CSV',
            'key': 'data/gender.csv',
            'extra_params': "IGNOREHEADER 1 DELIMITER ',' CSV",
            'dq_checks': [{'check_sql': "SELECT COUNT(*) FROM public.gender WHERE gender_code is null", 'expected_result': 0}]},
        {'table_name': 'public.visa',
            'file_type': 'CSV',
            'key': 'data/visa_class.csv',
            'extra_params': "IGNOREHEADER 1 DELIMITER ',' CSV",
            'dq_checks': [{'check_sql': "SELECT COUNT(*) FROM public.visa WHERE visa_class is null", 'expected_result': 0}]}
                    ]
    
    csv_stg_tables = [
        {'table_name': 'public.us_city_demo_stg',
            'file_type': 'CSV',
            'key': 'data/us-cities-demographics.csv',
            'extra_params': "IGNOREHEADER 1 DELIMITER ';' CSV",
            'dq_checks': []},
        {'table_name': 'public.airport_codes_stg',
            'file_type': 'CSV',
            'key': 'data/airport-codes_csv.csv',
            'extra_params': "IGNOREHEADER 1 DELIMITER ',' CSV",
            'dq_checks': []},
        {'table_name': 'public.world_temp_stg',
            'file_type': 'CSV', 
            'key': 'data/GlobalLandTemperaturesByCity.csv',
            'extra_params': "IGNOREHEADER 1 DELIMITER ',' CSV",
            'dq_checks': []}
        ]

    parq_immigration = {
        'table_name': 'public.immigration_stg',
        'file_type': 'PARQUET',
        'key': 'sas-files/i94_jan16_sub.parquet',
        'extra_params': "FORMAT AS PARQUET",
        'dq_checks': []
        }
      
    sas_tables = [
        {'table_name': 'country',
            'parse_string': 'i94cntyl',
            'end_string': ';',
            'table_columns': ['country_code', 'country'],
            'dq_checks': [{'check_sql': "SELECT COUNT(*) FROM country WHERE public.country_code is null", 'expected_result': 0}]},
        {'table_name': 'entry_port',
            'parse_string': 'i94prtl',
            'end_string': ';',
            'table_columns': ['port_code', 'addr', 'city', 'state'],
            'dq_checks': [{'check_sql': "SELECT COUNT(*) FROM entry_port WHERE public.port_code is null", 'expected_result': 0}]},
        {'table_name': 'arrival_mode',
            'parse_string': 'i94model',
            'end_string': ';',
            'table_columns': ['arrival_code', 'arrival_type'],
            'dq_checks': [{'check_sql': "SELECT COUNT(*) FROM arrival_mode WHERE public.arrival_code is null", 'expected_result': 0}]},
        {'table_name': 'region',
            'parse_string': 'i94addrl',
            'end_string': ';',
            'table_columns': ['region_code', 'region_name'],
            'dq_checks': [{'check_sql': "SELECT COUNT(*) FROM region_code WHERE public.region_code is null", 'expected_result': 0}]},
        {'table_name': 'visa_type',
            'parse_string': 'I94VISA',
            'end_string': '*/',
            'table_columns': ['visa_code', 'visa_type'],
            'dq_checks': [{'check_sql': "SELECT COUNT(*) FROM visa_type WHERE public.visa_code is null", 'expected_result': 0}]}
    ]
