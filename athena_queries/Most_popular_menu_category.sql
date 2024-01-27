SELECT 
    menu_category, 
    COUNT(menu_id) AS total_menu 
FROM "uber-eats-data"."menu_table_avro"
GROUP BY menu_category
ORDER BY total_menu DESC
LIMIT 10;