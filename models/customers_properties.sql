

with source as (

    select * from {{ source('bloomreach', 'customers_properties') }}

),

renamed as (

    select
        internal_id,
        properties,
        raw_properties

    from source

)

select * from renamed
    
