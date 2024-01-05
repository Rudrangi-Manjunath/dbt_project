
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (

    SELECT
        TIMESTAMP_MILLIS(CAST(JSON_EXTRACT_SCALAR(object, '$[0]') AS INT64)) AS date,
        JSON_EXTRACT_SCALAR(object, '$[1]') AS FOLLOWERS_COUNT,
        JSON_EXTRACT_SCALAR(object, '$[2]') AS FOLLOWERS_COUNT_CHANGE,
        JSON_EXTRACT_SCALAR(object, '$[3]') AS CHANGE_FOLLOWERS_COUNT,
        JSON_EXTRACT_SCALAR(object, '$[4]') AS CHANGE_FOLLOWERS_COUNT_CHANGE,
    FROM
       `airbyte_internal.transformed_events_raw__stream_follower_count`,
        UNNEST(JSON_EXTRACT_ARRAY(_airbyte_data, '$.data.rows')) AS object      
)
select * from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
