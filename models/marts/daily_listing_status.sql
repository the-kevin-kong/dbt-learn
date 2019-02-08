with listings as (

    select * from {{ref("stg_listings")}}

)

, history as (


    select * from {{ref("stg_listing_history")}}

)

, calendar as (

    select * from {{ref("utils_calendar")}}

)

, listing_info as (

    select 

        listing_id,
        maximum_nights,
        minimum_nights,
        cancellation_policy,
        instant_bookable,
        square_feet,
        amenities,
        beds,
        bedrooms,
        bathrooms,
        room_type,
        property_type

    from listings

)

, historical as (

    select

        date_day,
        listing_id,
        date,
        last_value(price ignore nulls) over (
            partition by listing_id
            order by date
            rows between unbounded preceding and current row
            ) as price,
        available

    from history
    right join calendar
    on history.date = calendar.date_day
        
)

, final as (

    select * from historical
    left join listing_info using(listing_id)

)

select * from final
order by date_day,
         listing_id