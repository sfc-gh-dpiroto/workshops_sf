USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE DATABASE CITIBIKE;

USE DATABASE CITIBIKE;

USE SCHEMA PUBLIC; 

/*ESTRUTURA DE ARMAZENAMENTO*/
create or replace table trips (
    tripduration integer,
    starttime timestamp,
    stoptime timestamp,
    start_station_id integer,
    start_station_name string,
    start_station_latitude float,
    start_station_longitude float,
    end_station_id integer,
    end_station_name string,
    end_station_latitude float,
    end_station_longitude float,
    bikeid integer,
    membership_type string,
    usertype string,
    birth_year integer,
    gender integer
);

--CRIA STAGE PARA ACESSAR DADOS DO S3
CREATE STAGE citibike_trips 
	URL = 's3://snowflake-workshop-lab/citibike-trips-csv/' 
	DIRECTORY = ( ENABLE = true AUTO_REFRESH = true );

list @citibike_trips ;


--IMPORTAR DADOS
--truncate table trips;
create or replace file format csv type='csv'
  compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
  skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting data for zero to snowflake';

--Copiar dados 2013 (9s)
  copy into trips from @citibike_trips file_format=csv PATTERN = '.*2013.*csv.*' ;

--Qtde Linhas Importadas
select count(*) from trips;

--Resize do Warehouse
--alter warehouse COMPUTE_WH set warehouse_size = 'XXLARGE';

--truncate table trips;
copy into trips from @citibike_trips file_format=csv PATTERN = '.*csv.*' ;

--Qtde Linhas
select count(*) from trips;

alter warehouse COMPUTE_WH set warehouse_size = 'xsmall';


--Histórico de Queries


--Criar novo Warehouse: ANALYTICS_WH


--Resut Cache (Cross Warehouse)
select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)",
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)"
from trips
group by 1 order by 1;



--Clone de objectos
CREATE TABLE CLONE_TRIPS CLONE TRIPS;

SELECT * FROM CLONE_TRIPS LIMIT 100;



/*dados semi estruturados*/
create database weather;


use role accountadmin;

use warehouse compute_wh;

use database weather;

use schema public;

create table json_weather_data (v variant);

--Carregar dados semi estruturados 
create stage nyc_weather
url = 's3://snowflake-workshop-lab/zero-weather-nyc';

copy into json_weather_data
from @nyc_weather 
    file_format = (type = json strip_outer_array = true);

    --Qual formato do arquivo?
list @nyc_weather;

select * from json_weather_data limit 10;



// Criar view para estruturar dados
create or replace view json_weather_data_view as
select
    v:obsTime::timestamp as observation_time,
    v:station::string as station_id,
    v:name::string as city_name,
    v:country::string as country,
    v:latitude::float as city_lat,
    v:longitude::float as city_lon,
    v:weatherCondition::string as weather_conditions,
    v:coco::int as weather_conditions_code,
    v:temp::float as temp,
    v:prcp::float as rain,
    v:tsun::float as tsun,
    v:wdir::float as wind_dir,
    v:wspd::float as wind_speed,
    v:dwpt::float as dew_point,
    v:rhum::float as relative_humidity,
    v:pres::float as pressure
from
    json_weather_data
where
    station_id = '72502';

--Usando query para consultas
select * from json_weather_data_view
where date_trunc('month',observation_time) = '2018-01-01'
limit 20;


--Relacionar dados da tabela de Trips com a de Temperaturas / Graph
select 
    weather_conditions as conditions
    ,count(*) as num_trips
from citibike.public.trips
left outer join json_weather_data_view 
    on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1 
order by 2 desc;


--Time Travel
drop table json_weather_data;

undrop table json_weather_data;


--Rollback a table
use database citibike;
use schema public;
update trips set start_station_name = 'oops';

select
    start_station_name as "station",
    count(*) as "rides"
from trips
    group by 1
    order by 2 desc
limit 20;


select * from table(information_schema.query_history_by_session (result_limit=>5))
where query_text like 'update%' order by start_time desc ;

create or replace table trips as
(select * from trips before (statement => '01b2d740-0000-3b99-0000-00003a057769'));


select * from trips limit 100;




/*Pipeline Básico*/

CREATE OR REPLACE TABLE DADOS_RAW (
    ID INT IDENTITY,
    NOME VARCHAR(100),
    SOBRENOME VARCHAR(100),
    ANO_NASC int,
    ESTADO VARCHAR(100)
);

CREATE OR REPLACE TABLE DADOS_CURADOS(
    NOME_COMPLETO VARCHAR(100),
    IDADE INT,
    ESTADO VARCHAR(100)
);

CREATE OR REPLACE TABLE DADOS_ANALITICOS(
    ESTADO VARCHAR(10),
    QTDE INT
);


CREATE OR REPLACE STREAM SREAM_DADOS_RAW ON TABLE DADOS_RAW;
CREATE OR REPLACE STREAM SREAM_DADOS_CURADOS ON TABLE DADOS_CURADOS;

insert INTO DADOS_RAW (NOME, SOBRENOME, ANO_NASC, ESTADO)
VALUES ('DHIEGO', 'PIROTO', '1989', 'SP');

--Pipeline Curado
CREATE OR REPLACE TASK PROCESSAMENTO_CURADO
	WAREHOUSE = compute_wh
	SCHEDULE = '1 MINUTE'
WHEN
    SYSTEM$STREAM_HAS_DATA('SREAM_DADOS_RAW')
AS
    INSERT INTO DADOS_CURADOS (NOME_COMPLETO, IDADE, ESTADO)
    SELECT CONCAT(NOME, ' ' ,SOBRENOME), year(CURRENT_DATE()) - ANO_NASC, 
    CASE
        WHEN ESTADO = 'SP'THEN 'SAO PAULO'
        WHEN ESTADO = 'RJ'THEN 'RIO DE JANEIRO'
        WHEN ESTADO = 'MG' THEN 'MINAS GERAIS'
        ELSE 'FORA DE CONTEXTO' END AS ESTADO
    FROM SREAM_DADOS_RAW
;


--Pipeline Analitico
CREATE OR REPLACE TASK PROCESSAMENTO_ANALITICO
	WAREHOUSE = compute_wh
	AFTER PROCESSAMENTO_CURADO
WHEN
    SYSTEM$STREAM_HAS_DATA('SREAM_DADOS_CURADOS')
AS
    INSERT INTO DADOS_ANALITICOS (ESTADO, QTDE)
    SELECT ESTADO, COUNT(*) FROM SREAM_DADOS_CURADOS GROUP BY ESTADO;
;

ALTER TASK PROCESSAMENTO_CURADO RESUME;
ALTER TASK PROCESSAMENTO_ANALITICO RESUME;


SELECT * FROM DADOS_ANALITICOS;
SELECT * FROM DADOS_CURADOS;

insert INTO DADOS_RAW (NOME, SOBRENOME, ANO_NASC, ESTADO)
VALUES ('DHIEGO', 'PIROTO', '1989', 'RJ');


EXECUTE TASK PROCESSAMENTO_CURADO;









--CORRIGINDO ERRO DO PIPELINE
ALTER TABLE DADOS_ANALITICOS ALTER COLUMN ESTADO VARCHAR(100);

--REEXECUTA TASKS


insert INTO DADOS_RAW (NOME, SOBRENOME, ANO_NASC, ESTADO)
VALUES ('DHIEGO', 'PIROTO', '1989', 'MG');

EXECUTE TASK PROCESSAMENTO_CURADO;

SELECT * FROM DADOS_CURADOS;
SELECT * FROM DADOS_ANALITICOS;

