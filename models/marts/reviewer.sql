with listings as (

    select * from {{ref("stg_listings")}}

)

, reviews as (

    select * from {{ref("stg_reviews")}}

)

, joined as (

    select * from listings 
    inner join reviews using(listing_id)

)

-- , calc as (

--     select 
    
--         reviewer_name,
--         property_type,
--         count(*) as num_reviews

--     from joined 
--     group by 1,2
-- )

, final as (

    select 

        reviewer_name,
        min(date) as first_review_date,
        count(review_id) as num_reviews,
        listagg(distinct neighbourhood_cleansed, ',') as neightborhoods_visited

    from joined
    group by 1

)

select * from final