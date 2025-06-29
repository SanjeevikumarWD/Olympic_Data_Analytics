SELECT TOP 10
    team_country,
    gold,
    silver,
    bronze,
    total,
    weighted_medal_score
FROM [OlympicInsightDB].[dbo].[medals]
ORDER BY total DESC;
