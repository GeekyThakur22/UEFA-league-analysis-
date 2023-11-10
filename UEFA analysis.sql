
                                     ||--UEFA CHAMPIONS LEAGUE ANALYSIS--||


select*
from AllTimeRankingByClub$
select*
from AllTimeRankingByCountry$

--details and comparison between ranking by clubs and ranking by country

--#1 ALL time rankings by club

--a)Get the top 10 clubs with the most titles in UEFA Champions League

select top 10 Club,Titles
from AllTimeRankingByClub$
order by Titles desc

--b)Calculate the average goals scored and conceded by clubs

SELECT
    [Club],
    AVG([Goals For]) AS AvgGoalsFor,
    AVG([Goals Against]) AS AvgGoalsAgainst
FROM [uefa champions league].[dbo].[AllTimeRankingByClub$]
GROUP BY [Club]

--c) Find clubs with the highest goal difference on average

SELECT TOP 10
    [Club],
    AVG([Goal Diff]) AS AvgGoalDifference
FROM [uefa champions league].[dbo].[AllTimeRankingByClub$]
GROUP BY [Club]
ORDER BY AvgGoalDifference DESC;

-- d)Count the number of clubs from each country

SELECT
    [Country],
    COUNT([Club]) AS ClubCount
FROM [uefa champions league].[dbo].[AllTimeRankingByClub$]
GROUP BY [Country]
ORDER BY ClubCount DESC;

 --e)Identify Clubs with the Most Consecutive Titles


 WITH ConsecutiveTitles AS (
    SELECT
        [Club],
        [Titles],
        [Country],
        ROW_NUMBER() OVER (PARTITION BY [Club], [Country] ORDER BY [Participated]) - 
        ROW_NUMBER() OVER (PARTITION BY [Club], [Country], [Titles] ORDER BY [Participated]) AS ConsecutiveGroup
    FROM [uefa champions league].[dbo].[AllTimeRankingByClub$]
)

SELECT
    [Club],
    [Country],
    MAX([Titles]) AS MaxConsecutiveTitles
FROM ConsecutiveTitles
GROUP BY [Club], [Country]
ORDER BY MaxConsecutiveTitles DESC;

--#2 ALL time rankings by club

-- a)Get the top 10 countries with the most titles in UEFA Champions League
SELECT TOP 10
    [Country],
    [Titles]
FROM [uefa champions league].[dbo].[AllTimeRankingByCountry$]
ORDER BY [Titles] DESC;

-- b)Calculate the average goals scored and conceded by countries
SELECT
    [Country],
    AVG([Goals For]) AS AvgGoalsFor,
    AVG([Goals Against]) AS AvgGoalsAgainst
FROM [uefa champions league].[dbo].[AllTimeRankingByCountry$]
GROUP BY [Country];

-- c)Find countries with the highest goal difference on average
SELECT TOP 10
    [Country],
    AVG([Goal Diff]) AS AvgGoalDifference
FROM [uefa champions league].[dbo].[AllTimeRankingByCountry$]
GROUP BY [Country]
ORDER BY AvgGoalDifference DESC;

--d)Top Performing Countries by Win Percentage
SELECT
    [Country],
    (SUM([Win]) * 100.0 / SUM([Played])) AS WinPercentage
FROM [uefa champions league].[dbo].[AllTimeRankingByCountry$]
GROUP BY [Country]
ORDER BY WinPercentage DESC;


--e)Comparing Win Percentage and Titles
SELECT
    [Country],
    SUM([Win]) AS TotalWins,
    SUM([Played]) AS TotalMatchesPlayed,
    (SUM([Win]) * 100.0 / SUM([Played])) AS WinPercentage,
    [Titles]
FROM [uefa champions league].[dbo].[AllTimeRankingByCountry$]
GROUP BY [Country], [Titles]
ORDER BY WinPercentage DESC, TotalWins DESC;

--------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------

--## coaches appearences and details

 --a)sum of total appearences by a coach
 SELECT TOP 1000
    [Coach],
    SUM([Appearance]) AS TotalAppearances
FROM [uefa champions league].[dbo].[CoachesAppearTotals$]
GROUP BY [Coach]
ORDER BY TotalAppearances DESC;

--b)avg club apperances
SELECT [Club],
    AVG([Appearance]) AS AvgClubAppearance
FROM [uefa champions league].[dbo].[CoachesAppearDetails$]
GROUP BY [Club]
ORDER BY AvgClubAppearance DESC;

--c)Max appearences
SELECT [Coach],
    [Club],
    MAX([Appearance]) AS MaxAppearance
FROM [uefa champions league].[dbo].[CoachesAppearDetails$]
GROUP BY [Coach], [Club]
ORDER BY MaxAppearance DESC;


--d)Find Coaches Who Worked with Multiple Clubs

WITH CoachClubCount AS (
    SELECT
        [Coach],
        COUNT(DISTINCT [Club]) AS ClubCount
    FROM [uefa champions league].[dbo].[CoachesAppearDetails$]
    GROUP BY [Coach]
)
SELECT
    [Coach],
    ClubCount
FROM CoachClubCount
WHERE ClubCount > 1
ORDER BY ClubCount DESC;


-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------

--## GoalStatsPerGroupRound$

select*
  from GoalStatsPerGroupRound$

--a)List the Top Scoring Seasons

  SELECT TOP 10
    [Season],
    SUM([Gls]) AS TotalGoals
FROM [uefa champions league].[dbo].[GoalStatsPerGroupRound$]
GROUP BY [Season]
ORDER BY TotalGoals DESC;

--b)Calculate Average Goals per Season

SELECT
    [Season],
    AVG([Gls]) AS AverageGoals
FROM [uefa champions league].[dbo].[GoalStatsPerGroupRound$]
GROUP BY [Season]
ORDER BY AverageGoals DESC;

--c)Identify the Top Goal Scorers in a Specific Season

SELECT TOP 10
    [Season],
    [G],
    [Gls]
FROM [uefa champions league].[dbo].[GoalStatsPerGroupRound$]
WHERE [Season] = 'YourSelectedSeason'
ORDER BY [Gls] DESC;



--d)Find the Teams with the Most Goals in the Group Stage

SELECT TOP 10
    [Season],
    [G],
    SUM([Gls]) AS TotalGroupStageGoals
FROM [uefa champions league].[dbo].[GoalStatsPerGroupRound$]
GROUP BY [Season], [G]
ORDER BY TotalGroupStageGoals DESC;


--e)Calculate Goal Distribution in Different Rounds
SELECT
    [Season],
   SUM(CAST([G] AS bigint) )AS GroupStageGoals,
    SUM(CAST([R16] AS bigint)) AS Round16Goals,
    SUM(CAST([QF] AS bigint)) AS QuarterFinalGoals,
    SUM(CAST([SF] AS bigint)) AS SemiFinalGoals,
    SUM(CAST([F11] AS bigint)) AS FinalGoals
FROM [uefa champions league].[dbo].[GoalStatsPerGroupRound$]
GROUP BY [Season]
ORDER BY [Season];


--f)Identify Teams with Consistent Goal Scoring in Multiple Seasons
WITH TeamSeasonGoals AS (
    SELECT
        [Season],
        [G] AS Team,
        SUM([Gls]) AS TotalGoals
    FROM [uefa champions league].[dbo].[GoalStatsPerGroupRound$]
    GROUP BY [Season], [G]
)
SELECT
    Team1.[Season] AS Season1,
    Team1.Team AS Team,
    Team2.[Season] AS Season2,
    Team2.TotalGoals AS GoalsInSeason2
FROM TeamSeasonGoals AS Team1
JOIN TeamSeasonGoals AS Team2
    ON Team1.Team = Team2.Team
    AND Team1.[Season] < Team2.[Season]
ORDER BY Team1.Team, Team1.[Season], Team2.[Season];


-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
--##PlayerAppearDetails

 --a)Total Appearances per Player
  SELECT [Player], SUM([Appearances]) AS Total_Appearances
FROM [uefa champions league].[dbo].[PlayerAppearDetails$]
GROUP BY [Player]
ORDER BY Total_Appearances DESC;

--b)Average Appearances per Club:
SELECT [Club], AVG([Appearances]) AS Avg_Appearances
FROM [uefa champions league].[dbo].[PlayerAppearDetails$]
GROUP BY [Club]
ORDER BY Avg_Appearances DESC;

--c)Calculating the Percentage of Appearances for Each Player:
SELECT [F1], [Player], [Club], [Appearances],
       [Appearances] * 100.0 / SUM([Appearances]) OVER(PARTITION BY [Club]) AS Percentage_of_Appearances
FROM [uefa champions league].[dbo].[PlayerAppearDetails$];

--d)Ranking Players by Appearances:
SELECT [F1], [Player], [Club], [Appearances],
       RANK() OVER(ORDER BY [Appearances] DESC) AS Appearances_Rank
FROM [uefa champions league].[dbo].[PlayerAppearDetails$];

--e)Top Players with Most Appearances for Each Club:
WITH RankedPlayers AS (
    SELECT [Player], [Club], [Appearances],
           ROW_NUMBER() OVER(PARTITION BY [Club] ORDER BY [Appearances] DESC) AS Player_Rank
    FROM [uefa champions league].[dbo].[PlayerAppearDetails$]
)

SELECT [Club], [Player], [Appearances]
FROM RankedPlayers
WHERE Player_Rank = 1;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--##TopGoalScorer


--a)Top Clubs by Total Goals:

SELECT [Club], SUM([Goals]) AS TotalGoals
FROM [dbo].[TopGoalScorer$]
GROUP BY [Club]
ORDER BY TotalGoals DESC

--b)Players with the Best Goals-to-Appearances Ratio:

SELECT [Player], [Club], ([Goals] / [Appearances]) AS GoalsToAppearancesRatio
FROM [dbo].[TopGoalScorer$]
ORDER BY GoalsToAppearancesRatio DESC

--c)Player with the Highest Average Goals per Year:

SELECT
   [Player],
   AVG([Goals]) AS AverageGoalsPerYear
FROM [dbo].[TopGoalScorer$]
GROUP BY [Player]
ORDER BY AverageGoalsPerYear DESC

--d)Rank Players Within Each Year Based on Goals:
WITH PlayerRank AS (
   SELECT
      [Year],
      [Player],
      [Club],
      [Goals],
      ROW_NUMBER() OVER (PARTITION BY [Year] ORDER BY [Goals] DESC) AS GoalRank
   FROM [dbo].[TopGoalScorer$]
)
SELECT * FROM PlayerRank
WHERE GoalRank = 1

--e)Find Players with the Highest Goal Improvement Year-over-Year:
SELECT
   [Player],
   MAX(GoalsChange) AS MaxGoalImprovement
FROM (
   SELECT
      [Year],
      [Player],
      [Goals],
      LAG([Goals]) OVER (PARTITION BY [Player] ORDER BY [Year]) AS PreviousYearGoals,
      [Goals] - LAG([Goals]) OVER (PARTITION BY [Player] ORDER BY [Year]) AS GoalsChange
   FROM [dbo].[TopGoalScorer$]
) AS Subquery
GROUP BY [Player]


