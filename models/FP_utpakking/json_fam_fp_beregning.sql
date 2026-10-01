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
          saksnummer      NUMBER PATH '$.saksnummer'
         ,fagsak_id       NUMBER PATH '$.fagsakId'
         ,behandling_uuid VARCHAR2(255) PATH '$.behandlingUuid'
         ,nested PATH '$.beregning' COLUMNS (
            grunnbelop         NUMBER PATH '$.grunnbeløp'
           ,hjemmel            VARCHAR2(255) PATH '$.hjemmel'
           ,fastsatt           VARCHAR2(255) PATH '$.fastsatt'
           ,aarsbelop_brutto   NUMBER PATH '$.årsbeløp.brutto'
           ,aarsbelop_avkortet NUMBER PATH '$.årsbeløp.avkortet'
           ,aarsbelop_redusert NUMBER PATH '$.årsbeløp.redusert'
           ,aarsbelop_dagsats  NUMBER PATH '$.årsbeløp.dagsats'
           ,nested PATH '$.andeler[*]' COLUMNS (
              andeler_aktivitet          VARCHAR2(255) PATH '$.aktivitet'
             ,andeler_arbeidsgiver       VARCHAR2(255) PATH '$.arbeidsgiver'
             ,andeler_aarsbelop_brutto   NUMBER PATH '$.årsbeløp.brutto'
             ,andeler_aarsbelop_avkortet NUMBER PATH '$.årsbeløp.avkortet'
             ,andeler_aarsbelop_redusert NUMBER PATH '$.årsbeløp.redusert'
             ,andeler_aarsbelop_dagsats  NUMBER PATH '$.årsbeløp.dagsats'
           ) ) ) ) j
  where j.grunnbelop is not null
),

final as (
  select
    p.grunnbelop
   ,hjemmel
   ,fastsatt
   ,p.aarsbelop_brutto
   ,p.aarsbelop_avkortet
   ,p.aarsbelop_redusert
   ,p.aarsbelop_dagsats
   ,p.andeler_aktivitet
   ,p.andeler_arbeidsgiver
   ,p.andeler_aarsbelop_brutto
   ,p.andeler_aarsbelop_avkortet
   ,p.andeler_aarsbelop_redusert
   ,p.andeler_aarsbelop_dagsats
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
     dvh_fam_fp.fam_fp_seq.nextval as pk_fp_beregning
    ,grunnbelop
    ,aarsbelop_brutto
    ,aarsbelop_avkortet
    ,aarsbelop_redusert
    ,aarsbelop_dagsats
    ,andeler_aktivitet
    ,andeler_arbeidsgiver
    ,andeler_aarsbelop_brutto
    ,andeler_aarsbelop_avkortet
    ,andeler_aarsbelop_redusert
    ,andeler_aarsbelop_dagsats
    ,fk_fp_fagsak
    ,kafka_offset
    ,localtimestamp as lastet_dato
    ,hjemmel
    ,fastsatt
from final
