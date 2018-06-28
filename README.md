# dfs-baseball-project
Daily Fantasy Baseball Project

Requires download of contest .csv from Draftkings and Fanduel to function (Fanduel WIP)

Scrapes data of starting players in contest, creates scores based on statistics, uses algorithm to sort players and create optimal lineup within the salary and roster constraints

At this time, must run main to create SQL tables, then can run updatescript independent (I wanted to be able to run update overnight). Finally, run dailyoptimizer when the lineups have been released.

