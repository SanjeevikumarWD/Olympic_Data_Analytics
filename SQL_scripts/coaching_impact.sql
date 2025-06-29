SELECT 
    a.country,
    a.discipline,
    a.athlete_count,
    c.coach_count,
    m.total AS medals_won,
    ROUND(1.0 * m.total / NULLIF(c.coach_count, 0), 2) AS medals_per_coach
FROM [OlympicInsightDB].[dbo].[athlete_counts] a
LEFT JOIN [OlympicInsightDB].[dbo].[coach_counts] c
    ON a.country = c.country AND a.discipline = c.discipline
LEFT JOIN [OlympicInsightDB].[dbo].[medals] m
    ON a.country = m.team_country
WHERE c.coach_count IS NOT NULL
ORDER BY medals_per_coach DESC;
