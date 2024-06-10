with pre_table AS (select di.item AS item,	
		di.hierarchy,
		di.sort_order,
		dii.lang,
		dii.description,
		dii.notes
from odata_dataset_dimension_item AS di
join odata_dataset_dimension_item_info AS dii on di.item_index = dii.item_index
where di.dataset = 'care0198' and di.dimension = 'Year')

SELECT item, 
	   hierarchy,
	   sort_order,
       GROUP_CONCAT(CASE WHEN "lang" == 'cy-gb' THEN description END) as description_cy,
       GROUP_CONCAT(CASE WHEN "lang" == 'en-gb' THEN description END) as description_en,
	   GROUP_CONCAT(CASE WHEN "lang" == 'cy-gb' THEN notes END) as notes_cy,
	   GROUP_CONCAT(CASE WHEN "lang" == 'en-gb' THEN notes END) as notes_en
FROM pre_table
GROUP BY item