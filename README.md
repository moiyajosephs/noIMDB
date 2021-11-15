# noIMDB

## Description
Make - Project 1 will be a Scala console application that is retrieving data using Hive or MapReduce. Your job is to build a real-time news analyzer. This application should allow users to view the trending topics (e.g. all trending topics for news related to "politics", "tv shows", "movies", "video games", or "sports" only [choose one topic for project]).


## Goals
ALL user interaction must come purely from the console application
- Hive/MapReduce must:
    - scrap data from datasets from an API based on your topic of choice
- Your console application must: 
    - query data to answer at least 6 analysis questions of your choice
    - have a login system for all users with passwords (scala)
        - 2 types of users: BASIC and ADMIN
        - Users should also be able to update username and password

### Stretch Goals:
- Passwords must be encrypted
- Export all results into a JSON file/ can optional because(changes done)

## Accomplished
[x] User login
[x] Admin login
[x] Save to json
[x] Six possible queries
1) Count # of TV Shows
2) Find the most common genre of a database
3) Percentage of TV Shows in a year
4) Which databse has more TV Shows
4) Are there common TV shows
6) Future query: Future percentage of TV shows to be released in a given year


Datasets gotten from Kaggle.com:
<ul>
    <li> <a href="https://www.kaggle.com/shivamb/disney-movies-and-tv-shows"> Disney+ datasets </a> </li>
    <li> <a href="https://www.kaggle.com/shivamb/hulu-movies-and-tv-shows"> Hulu datasets </a> </li>
    <li> <a href="https://www.kaggle.com/abhikaggle8/netflix-rating-distributions"> Netflix datasets </a> </li>
</ul>
