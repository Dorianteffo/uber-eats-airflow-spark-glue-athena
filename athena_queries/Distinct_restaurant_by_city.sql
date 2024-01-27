SELECT 
    r.restaurant_name, 
    COUNT(a.city) AS number_of_city
FROM "uber-eats-data"."address_table_avro" AS a 
JOIN "uber-eats-data"."restaurant_table_avro" AS r ON a.restaurant_id = r.restaurant_id
GROUP BY 1 
ORDER BY number_of_city DESC;