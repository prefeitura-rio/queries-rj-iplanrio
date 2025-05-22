
/*
plus11

id_equipamento STRING
secretaria_responsavel STRING
tipo_equipamento STRING
nome_oficial STRING
nome_popular STRING

plus10 STRING
plus11 STRING
plus6 STRING
latitude FLOAT64
longitude FLOAT64
geometry GEOGRAPHY

endereco STRUCT<
    logradouro  STRING,
    numero      STRING,
    complemento STRING,
    bairro      STRING,
    cep         STRING
>

bairro STRUCT<
    id_bairro STRING,

>
contato STRUCT<
    telefones   ARRAY<STRING>,
    email       STRING,
    site        STRING,
    redes_social STRUCT<
    facebook  STRING,
    instagram STRING,
    twitter   STRING
    >
>

ativo                    BOOL,
aberto_ao_publico        BOOL,
horario_funcionamento        ARRAY<STRUCT<dia STRING, abre TIME, fecha TIME>>


fonte                    STRING,
vigencia_inicio          DATE,
vigencia_fim             DATE

metadata                 JSON,

update_at      TIMESTAMP,
*/

-- do union all with equipaments from other sources

CREATE OR REPLACE TABLE `rj-iplanrio.plus_codes.equipamentos` AS (

with saude AS (
    select
        plus8,
        geometry,
        plus11,
        id_equipamento,
        secretaria_responsavel,
        tipo_equipamento,
        nome_oficial,
        nome_popular,
        plus10,
        plus6,
        latitude,
        longitude,
        endereco,
        bairro,
        contato,
        ativo,
        aberto_ao_publico,
        horario_funcionamento,
        fonte,
        vigencia_inicio,
        vigencia_fim,
        metadata,
        update_at
    from `rj-iplanrio.plus_codes.equipamentos_saude`
)

select * from saude

)