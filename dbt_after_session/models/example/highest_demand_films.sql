select category.name as genres, sum (amount) as total_sales
    from {{ source ('cross_border_analytics', 'category') }} category
    join {{ source ('cross_border_analytics', 'film_category') }} film_category
    on film_category.category_id = category.category_id
    join {{ source ('cross_border_analytics', 'film') }} film
    on film.film_id = film_category.film_id
    join {{ source ('cross_border_analytics', 'inventory') }} inventory
    on inventory.film_id = film.film_id
    join {{ source ('cross_border_analytics', 'rental') }} rental
    on rental.inventory_id = inventory.inventory_id
    join {{ source ('cross_border_analytics', 'payment') }} payment
    on rental.rental_id = payment.rental_id
    group by 1
    order by 2 desc
    limit 3
