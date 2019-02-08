with base as (

    select * from source_data.reviews

)

, reviews as (

    select 

        review,
        listing_id,
        id as review_id,
        comments,
        reviewer_name,
        date

    from base

)

select * from reviews