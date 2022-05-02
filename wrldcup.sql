SELECT team_id,
       team_name,
       COALESCE(num_points, 0) AS num_points
FROM teams
LEFT JOIN(SELECT host_team AS team_iid,
       SUM(num_host_points) OVER(PARTITION BY host_team ORDER BY host_team) AS num_points
          FROM
          (WITH total AS (
                SELECT team_name,
                       host_team,
                       num_host_points,
                       guest_team,
                       num_guest_points
                FROM teams
                LEFT JOIN(SELECT match_id,
                                 host_team,
                                 host_goals,
                                 guest_goals,
                                 CASE
                                 WHEN (host_goals = guest_goals) then 1
                                 WHEN (host_goals > guest_goals) then 3
                                 WHEN (host_goals < guest_goals) then 0
                                 ELSE 0
                                 END AS num_host_points
                           FROM matches) AS results_1
                           ON results_1.host_team = teams.team_id
                JOIN(SELECT match_id,
                                 guest_team,
                                 host_goals,
                                 guest_goals,
                                 CASE
                                 WHEN (host_goals = guest_goals) then 1
                                 WHEN (host_goals < guest_goals) then 3
                                 WHEN (host_goals > guest_goals) then 0
                                 ELSE 0
                                 END AS num_guest_points
                           FROM matches) AS results_2
                           ON results_2.match_id = results_1.match_id)
            SELECT host_team,
                   num_host_points
            FROM total
            UNION ALL
            SELECT guest_team,
                   num_guest_points
            FROM total) AS results
            ) AS combo
            ON teams.team_id = combo.team_iid
GROUP BY teams.team_id,
        team_name,
        num_points
ORDER BY 3 DESC, 1
