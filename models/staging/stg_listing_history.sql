with base as (

    select * from source_data.listing_history

)

, history as (

    select 

        listing_id,
        price,
        available,
        date

    from base

)

select * from history