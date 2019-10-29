# graphlytic
A search engine built on Spark

This is a monorepo housing three codebases:
graphlytic -- Spark code for calculating a reverse index on the Wikipedia dataset
graphlyticLoad -- Spark code for loading the results into a Redis back-end
graphlyticFrontend -- A flask application for querying the dataset

A deployment of the app should be live at (while AWS credits last): www.graphlytic.net
