SELECT 
    team_name,
    country,
    discipline,
    event,
    ROUND(medals_per_team_member, 2) AS medals_per_team_member
FROM [OlympicInsightDB].[dbo].[teams]
WHERE medal_contributor = 1
ORDER BY medals_per_team_member DESC;
