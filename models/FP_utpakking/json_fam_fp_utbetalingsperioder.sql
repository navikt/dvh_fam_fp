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
          saksnummer                       NUMBER PATH '$.saksnummer'
         ,fagsak_id                        NUMBER PATH '$.fagsakId'
         ,behandling_uuid                  VARCHAR2(255) PATH '$.behandlingUuid'
         ,nested PATH '$.utbetalingssperioder[*]' COLUMNS (
            seq_i_array                 FOR ORDINALITY
           ,fom                            DATE PATH '$.fom'
           ,tom                            DATE PATH '$.tom'
           ,inntektskategori               VARCHAR2(255) PATH '$.inntektskategori'
           ,arbeidsgiver                   VARCHAR2(255) PATH '$.arbeidsgiver'
           ,mottaker                       VARCHAR2(255) PATH '$.mottaker'
           ,dagsats                        NUMBER PATH '$.dagsats'
           ,dagsats_fra_beregningsgrunnlag NUMBER PATH '$.dagsatsFraBeregningsgrunnlag'
           ,utbetalingsgrad                NUMBER PATH '$.utbetalingsgrad'
          ) ) ) j
  where j.fom is not null
),

final as (
  select
    p.seq_i_array
   ,p.fom
   ,p.tom
   ,p.inntektskategori
   ,p.arbeidsgiver
   ,p.mottaker
   ,p.dagsats
   ,p.dagsats_fra_beregningsgrunnlag
   ,p.utbetalingsgrad
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
     dvh_fam_fp.fam_fp_seq.nextval as pk_fp_utbetalingsperioder
    ,seq_i_array
    ,fom
    ,tom
    ,inntektskategori
    ,arbeidsgiver
    ,mottaker
    ,dagsats
    ,dagsats_fra_beregningsgrunnlag
    ,utbetalingsgrad
    ,fk_fp_fagsak
    ,kafka_offset
    ,localtimestamp as lastet_dato
from final
