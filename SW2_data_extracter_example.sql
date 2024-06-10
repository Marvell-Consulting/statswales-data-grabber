WITH m AS (select fact, value from dataset_measure where dataset = 'care0198'), 
d AS (SELECT fact, 
       GROUP_CONCAT(CASE WHEN "dimension" == 'LocalAuthority' THEN item END) as LocalAuthority_id,
       GROUP_CONCAT(CASE WHEN "dimension" == 'Measure' THEN item END) as Measure_id,
       GROUP_CONCAT(CASE WHEN "dimension" == 'Year' THEN item END) as Year_id
FROM dataset_dimension
where dataset = 'care0198'
GROUP BY fact)
	
SELECT value,
	LocalAuthority_id,
	Measure_id,
	Year_id
FROM m
JOIN d ON m.fact = d.fact