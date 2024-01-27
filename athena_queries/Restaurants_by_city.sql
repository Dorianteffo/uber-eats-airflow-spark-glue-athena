SELECT 
    city, 
    COUNT(restaurant_id) AS number_of_restaurants
FROM "uber-eats-data"."address_table_avro" 
GROUP BY 1 
ORDER BY number_of_restaurants DESC;