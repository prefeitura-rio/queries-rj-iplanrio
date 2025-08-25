{{
  config(
    materialized='udf',
    parameter_list='latitude FLOAT64, longitude FLOAT64, len INT64',
    returns='STRING',
    language='js',
    library=['gs://rj-escritorio-dev/raw/js_packages/openlocationcode.min.js'],
    description='Encodes latitude and longitude coordinates into a Plus Code (Open Location Code) of specified length.'
  )
}}

return OpenLocationCode.encode(latitude, longitude, len);