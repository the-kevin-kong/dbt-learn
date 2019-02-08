with listings as (

    select * from {{ref("stg_listings")}}

)

, calc as (

    select 

        host_id,
        host_name,
        host_identity_verified,
        is_superhost,
        host_since,
        listing_id,
        number_of_reviews,
        review_scores_rating * number_of_reviews as total_rating,
        price

    from listings
    
)

, final as (

    select 

        host_id,
        host_name,
        host_identity_verified,
        is_superhost,
        host_since,
        count(listing_id) as num_listings,
        sum(total_rating) / sum(number_of_reviews) as weighted_rating

    from calc
    group by 1,2,3,4,5

)

select * from final