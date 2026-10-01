{{ config(
    materialized = 'table'
) }}

WITH fp_meta_data AS (

    SELECT
        A.pk_fp_meta_data,
        A.kafka_offset,
        A.kafka_mottatt_dato,
        A.kafka_partition,
        melding,
        b.kafka_offset C
    FROM
        {{ source('fam_fp','fam_fp_meta_data')}} A
        LEFT OUTER JOIN {{ source('fam_fp','json_fam_fp_fagsak')}} b
        ON A.kafka_offset = b.kafka_offset
        AND A.kafka_partition = b.kafka_partition
    WHERE
        A.kafka_mottatt_dato >= SYSDATE - 30
        AND b.kafka_offset IS NULL
)
SELECT
    *
FROM
    fp_meta_data
