{{
    config(
        materialized='incremental'
    )
}}

with fp_meta_data as (
  select * from {{ref ('fp_meldinger_til_aa_pakke_ut')}}
),

fp_fagsak as (
  select kafka_offset, saksnummer, fagsak_id, behandling_uuid, pk_fp_fagsak from {{ ref('json_fam_fp_fagsak') }}
),

pre_final as (
  select fp_meta_data.kafka_offset, j.*
  from fp_meta_data
      ,json_table(melding, '$' COLUMNS (
          saksnummer                 NUMBER PATH '$.saksnummer'
         ,fagsak_id                  NUMBER PATH '$.fagsakId'
         ,behandling_uuid            VARCHAR2(255) PATH '$.behandlingUuid'
         ,nested PATH '$.uttaksperioder[*]' COLUMNS (
            seq_i_array           FOR ORDINALITY
           ,fom                      DATE PATH '$.fom'
           ,tom                      DATE PATH '$.tom'
           ,type                     VARCHAR2(255) PATH '$.type'
           ,stonadskonto_type        VARCHAR2(255) PATH '$.stønadskontoType'
           ,rettighet_type           VARCHAR2(255) PATH '$.rettighetType'
           ,forklaring               VARCHAR2(255) PATH '$.forklaring'
           ,soknadsdato              DATE PATH '$.søknadsdato'
           ,er_utbetaling            VARCHAR2(255) PATH '$.erUtbetaling'
           ,virkedager               NUMBER PATH '$.virkedager'
           ,trekkdager               NUMBER PATH '$.trekkdager'
           ,gradering_aktivitet_type VARCHAR2(255) PATH '$.gradering.aktivitetType'
           ,gradering_arbeidsprosent NUMBER PATH '$.gradering.arbeidsprosent'
           ,samtidig_uttak_prosent   NUMBER PATH '$.samtidigUttakProsent'
          ) ) ) j
  where j.fom is not null
),

final as (
  select
    p.seq_i_array
   ,p.fom
   ,p.tom
   ,p.type
   ,p.stonadskonto_type
   ,p.rettighet_type
   ,p.forklaring
   ,p.soknadsdato
   ,p.er_utbetaling
   ,p.virkedager
   ,p.trekkdager
   ,p.gradering_aktivitet_type
   ,p.gradering_arbeidsprosent
   ,p.samtidig_uttak_prosent
   ,fp_fagsak.pk_fp_fagsak as fk_fp_fagsak
   ,p.kafka_offset
  from pre_final p
  join fp_fagsak
  on fp_fagsak.kafka_offset = p.kafka_offset
  and fp_fagsak.saksnummer = p.saksnummer
  and fp_fagsak.fagsak_id = p.fagsak_id
  and fp_fagsak.behandling_uuid = p.behandling_uuid
)

select
     dvh_fam_fp.fam_fp_seq.nextval as pk_fp_uttaksperioder
    ,seq_i_array
    ,fom
    ,tom
    ,type
    ,stonadskonto_type
    ,rettighet_type
    ,forklaring
    ,soknadsdato
    ,er_utbetaling
    ,virkedager
    ,trekkdager
    ,gradering_aktivitet_type
    ,gradering_arbeidsprosent
    ,samtidig_uttak_prosent
    ,fk_fp_fagsak
    ,kafka_offset
    ,localtimestamp as lastet_dato
from final