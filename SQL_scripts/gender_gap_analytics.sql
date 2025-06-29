SELECT 
    discipline,
    male,
    female,
    total,
    ROUND(male_percentage * 100, 2) AS male_percent,
    ROUND(female_percentage * 100, 2) AS female_percent,
    ROUND(gender_gap * 100, 2) AS gender_gap_percent,
    high_gender_gap
FROM [OlympicInsightDB].[dbo].[entriesgender]
ORDER BY gender_gap_percent DESC;
