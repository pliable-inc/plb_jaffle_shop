{{ config(materialized='table') }}


with stacked as (

    (
        select
            _plb_uuid,
            _plb_loaded_at,
            'customers' as _plb_source_model,
            ((
                case
                    when
                        "customers"."FIRST_NAME"::string is null
                        or "customers"."FIRST_NAME"::string::string = ''
                        then null
                    else "customers"."FIRST_NAME"::string
                end
            )) as "FIRST_NAME",
            ((
                case
                    when
                        "customers"."LAST_NAME"::string is null
                        or "customers"."LAST_NAME"::string::string = ''
                        then null
                    else "customers"."LAST_NAME"::string
                end
            )) as "LAST_NAME"
        from {{ ref('customers') }} as "customers"

    )

),


transformed as (

    select
        -- Generate a new deterministic UUID unique to this particular model
        _plb_loaded_at,
        'plb:::m:kait::r:' || sha1(_plb_uuid) as _plb_uuid,
        try_cast(("FIRST_NAME")::string as varchar) as "FIRST_NAME",
        try_cast(("LAST_NAME")::string as varchar) as "LAST_NAME"
    from stacked


)



select * from transformed
where 1 = 1
