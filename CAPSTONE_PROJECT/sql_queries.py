import configparser

"""
    - Reads the config file and loads necessary parameters for further use
    - Returns the connection and cursor to sparkifydb
"""
config = configparser.ConfigParser()
config.read('dwh.cfg')

"""
    - DROP tables if they exist in the database
"""
staging_immi_table_drop = "DROP TABLE IF EXISTS staging_immi"
staging_state_table_drop = "DROP TABLE IF EXISTS staging_state"
immi_table_drop = "DROP TABLE IF EXISTS immigration"
arrival_table_drop = "DROP TABLE IF EXISTS arrival"
state_table_drop = "DROP TABLE IF EXISTS state"
"""
    - Create staging tables to extract data from S3 to Redshift and other tables for the project
"""
staging_immi_table_create = """
CREATE TABLE IF NOT EXISTS staging_immi
(
 un_named bigint,
 cicid varchar,
 i94yr float,
 i94mon float,
 i94cit float,
 i94res float,
 i94port varchar,
 arrdate float,
 i94mode float,
 i94addr varchar,
 depdate float,
 i94bir float,
 i94visa float,
 count float,
 dtadfile float8,
 visapost varchar,
 occup varchar,
 entdepa char,
 entdepd char,
 entdepu char,
 matflag char,
 biryear float,
 dtaddto varchar,
 gender char,
 insnum int,
 airline varchar,
 admnum float8,
 fltno varchar,
 visatype varchar 
)
 """

staging_state_table_create = """
CREATE TABLE IF NOT EXISTS staging_state
(
 city varchar,
 state varchar,
 median_age float,
 male_population float8,
 female_population float8,
 total_population bigint,
 number_of_veterans float,
 foreign_born float8,
 avg_household_size float,
 state_code varchar,
 race varchar,
 count bigint
)
 """

immigration_table_create = """
CREATE TABLE IF NOT EXISTS immigration
(
 admission_no float8,
 arrival_date float,
 state_visited varchar,
 gender char,
 visa_type char,
 birth_year float,
 PRIMARY KEY (admission_no)
) 
"""

state_table_create = """
CREATE TABLE IF NOT EXISTS state
(state_code varchar NOT NULL PRIMARY KEY,
 male_population bigint,
 female_population bigint,
 total_population bigint
)
"""

arrival_table_create = """
CREATE TABLE IF NOT EXISTS arrival
(arrdate date PRIMARY KEY,
 day smallint NOT NULL,
 week smallint NOT NULL,
 month smallint NOT NULL,
 year smallint NOT NULL,
 weekday int NOT NULL)
"""

"""
    - Use copy command to copy data from S3 song and log files and load them to staging tables in REDSHIFT
"""

staging_immi_copy = """
                       copy staging_immi from 's3://rustys34bucket/immigration_data_sample.csv'
                       credentials 'aws_iam_role={}'
                       delimiter ','
                       IGNOREHEADER 1;
                      """.format(config.get("IAM_ROLE","ARN"))
   

staging_state_copy = """
                       copy staging_state from 's3://rustys34bucket/us-cities-demographics.csv'
                       credentials 'aws_iam_role={}'
                       delimiter ';'
                       IGNOREHEADER 1;
                      """.format(config.get("IAM_ROLE","ARN"))

"""
    - FINAL tables required for the project, load data using staging tables
"""

immi_table_insert = ("""
INSERT INTO immigration (admission_no,arrival_date,state_visited,gender,visa_type,birth_year)
SELECT DISTINCT
                cast(admnum as bigint),
                arrdate,
                i94addr,
                gender,
                i94visa,
                biryear
FROM staging_immi;                         
""")

state_table_insert = ("""
INSERT INTO state
(
state_code, 
male_population, 
female_population,
total_population
)
SELECT state_code, SUM(total_population), SUM(male_population), SUM(female_population) 
FROM staging_state
GROUP BY state_code;
""")


"""
    - List of queries required in create_tables and etl programs
"""

#create_table_queries = [staging_immi_table_create, staging_state_table_create, immigration_table_create, state_table_create, arrival_table_create]
#drop_table_queries = [staging_immi_table_drop, staging_state_table_drop, immi_table_drop, state_table_drop, arrival_table_drop]
#copy_table_queries = [staging_immi_copy, staging_state_copy]
#insert_table_queries = [immi_table_insert, state_table_insert]

copy_table_queries = [staging_immi_copy, staging_state_copy]
create_table_queries = [staging_immi_table_create, immigration_table_create, staging_state_table_create, state_table_create]
insert_table_queries = [immi_table_insert, state_table_insert]
drop_table_queries = [staging_immi_table_drop, immi_table_drop, staging_state_table_drop, state_table_drop]
