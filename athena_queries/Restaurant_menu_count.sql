SELECT 
    r.restaurant_name , 
    COUNT(m.menu_id) AS total_menu_available
FROM "uber-eats-data"."menu_table_avro" m
JOIN "uber-eats-data"."restaurant_table_avro" r ON m.restaurant_id = r.restaurant_id
GROUP BY r.restaurant_name 
ORDER BY total_menu_available DESC