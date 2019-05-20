
# Project 03: Data Warehouse



```python
import configparser
import psycopg2
%reload_ext sql
```


```python
config = configparser.ConfigParser()
config.read('rs_dwh.cfg')

db_host = config['CLUSTER']['HOST']
db_port = config['CLUSTER']['DB_PORT']
db_name = config['CLUSTER']['DB_NAME']
db_user = config['CLUSTER']['DB_USER']
db_pass = config['CLUSTER']['DB_PASSWORD']

connection_string = "postgresql://{user}:{password}@{host}:{port}/{name}".format(user=db_user, password=db_pass, host=db_host, port=db_port, name=db_name)
print('connection_string=%s' % connection_string) 

%sql $connection_string
```

    connection_string=postgresql://sparkify:aEe8c38U6;7MtNPG@172.31.5.253:5439/sparkify





    'Connected: sparkify@sparkify'




```python
%%sql
-- what are the free and paid user counts by location?
select f.location, f.level, count(level)
from f_songplay f
group by f.location, f.level
order by f.location, f.level;

```

     * postgresql://sparkify:***@172.31.5.253:5439/sparkify
    76 rows affected.





<table>
    <tr>
        <th>location</th>
        <th>level</th>
        <th>count</th>
    </tr>
    <tr>
        <td>Atlanta-Sandy Springs-Roswell, GA</td>
        <td>free</td>
        <td>28</td>
    </tr>
    <tr>
        <td>Atlanta-Sandy Springs-Roswell, GA</td>
        <td>paid</td>
        <td>428</td>
    </tr>
    <tr>
        <td>Augusta-Richmond County, GA-SC</td>
        <td>paid</td>
        <td>140</td>
    </tr>
    <tr>
        <td>Birmingham-Hoover, AL</td>
        <td>free</td>
        <td>15</td>
    </tr>
    <tr>
        <td>Birmingham-Hoover, AL</td>
        <td>paid</td>
        <td>208</td>
    </tr>
    <tr>
        <td>Cedar Rapids, IA</td>
        <td>free</td>
        <td>10</td>
    </tr>
    <tr>
        <td>Chicago-Naperville-Elgin, IL-IN-WI</td>
        <td>free</td>
        <td>13</td>
    </tr>
    <tr>
        <td>Chicago-Naperville-Elgin, IL-IN-WI</td>
        <td>paid</td>
        <td>462</td>
    </tr>
    <tr>
        <td>Columbia, SC</td>
        <td>free</td>
        <td>25</td>
    </tr>
    <tr>
        <td>Dallas-Fort Worth-Arlington, TX</td>
        <td>free</td>
        <td>21</td>
    </tr>
    <tr>
        <td>Detroit-Warren-Dearborn, MI</td>
        <td>free</td>
        <td>4</td>
    </tr>
    <tr>
        <td>Detroit-Warren-Dearborn, MI</td>
        <td>paid</td>
        <td>72</td>
    </tr>
    <tr>
        <td>Elkhart-Goshen, IN</td>
        <td>free</td>
        <td>2</td>
    </tr>
    <tr>
        <td>Eugene, OR</td>
        <td>free</td>
        <td>20</td>
    </tr>
    <tr>
        <td>Eureka-Arcata-Fortuna, CA</td>
        <td>free</td>
        <td>16</td>
    </tr>
    <tr>
        <td>Harrisburg-Carlisle, PA</td>
        <td>free</td>
        <td>37</td>
    </tr>
    <tr>
        <td>Houston-The Woodlands-Sugar Land, TX</td>
        <td>free</td>
        <td>66</td>
    </tr>
    <tr>
        <td>Indianapolis-Carmel-Anderson, IN</td>
        <td>free</td>
        <td>14</td>
    </tr>
    <tr>
        <td>Janesville-Beloit, WI</td>
        <td>free</td>
        <td>7</td>
    </tr>
    <tr>
        <td>Janesville-Beloit, WI</td>
        <td>paid</td>
        <td>241</td>
    </tr>
    <tr>
        <td>Klamath Falls, OR</td>
        <td>free</td>
        <td>20</td>
    </tr>
    <tr>
        <td>La Crosse-Onalaska, WI-MN</td>
        <td>free</td>
        <td>45</td>
    </tr>
    <tr>
        <td>Lake Havasu City-Kingman, AZ</td>
        <td>paid</td>
        <td>321</td>
    </tr>
    <tr>
        <td>Lansing-East Lansing, MI</td>
        <td>paid</td>
        <td>557</td>
    </tr>
    <tr>
        <td>Las Cruces, NM</td>
        <td>free</td>
        <td>6</td>
    </tr>
    <tr>
        <td>London, KY</td>
        <td>free</td>
        <td>5</td>
    </tr>
    <tr>
        <td>Longview, TX</td>
        <td>paid</td>
        <td>17</td>
    </tr>
    <tr>
        <td>Los Angeles-Long Beach-Anaheim, CA</td>
        <td>free</td>
        <td>6</td>
    </tr>
    <tr>
        <td>Lubbock, TX</td>
        <td>free</td>
        <td>27</td>
    </tr>
    <tr>
        <td>Marinette, WI-MI</td>
        <td>paid</td>
        <td>169</td>
    </tr>
    <tr>
        <td>Miami-Fort Lauderdale-West Palm Beach, FL</td>
        <td>free</td>
        <td>2</td>
    </tr>
    <tr>
        <td>Milwaukee-Waukesha-West Allis, WI</td>
        <td>free</td>
        <td>9</td>
    </tr>
    <tr>
        <td>Minneapolis-St. Paul-Bloomington, MN-WI</td>
        <td>free</td>
        <td>15</td>
    </tr>
    <tr>
        <td>Myrtle Beach-Conway-North Myrtle Beach, SC-NC</td>
        <td>free</td>
        <td>1</td>
    </tr>
    <tr>
        <td>Nashville-Davidson--Murfreesboro--Franklin, TN</td>
        <td>free</td>
        <td>25</td>
    </tr>
    <tr>
        <td>New Haven-Milford, CT</td>
        <td>free</td>
        <td>48</td>
    </tr>
    <tr>
        <td>New Orleans-Metairie, LA</td>
        <td>free</td>
        <td>55</td>
    </tr>
    <tr>
        <td>New York-Newark-Jersey City, NY-NJ-PA</td>
        <td>free</td>
        <td>113</td>
    </tr>
    <tr>
        <td>New York-Newark-Jersey City, NY-NJ-PA</td>
        <td>paid</td>
        <td>149</td>
    </tr>
    <tr>
        <td>Ogden-Clearfield, UT</td>
        <td>free</td>
        <td>7</td>
    </tr>
    <tr>
        <td>Oxnard-Thousand Oaks-Ventura, CA</td>
        <td>free</td>
        <td>7</td>
    </tr>
    <tr>
        <td>Palestine, TX</td>
        <td>free</td>
        <td>27</td>
    </tr>
    <tr>
        <td>Parkersburg-Vienna, WV</td>
        <td>free</td>
        <td>5</td>
    </tr>
    <tr>
        <td>Pensacola-Ferry Pass-Brent, FL</td>
        <td>free</td>
        <td>3</td>
    </tr>
    <tr>
        <td>Philadelphia-Camden-Wilmington, PA-NJ-DE-MD</td>
        <td>free</td>
        <td>35</td>
    </tr>
    <tr>
        <td>Phoenix-Mesa-Scottsdale, AZ</td>
        <td>free</td>
        <td>27</td>
    </tr>
    <tr>
        <td>Plymouth, IN</td>
        <td>free</td>
        <td>10</td>
    </tr>
    <tr>
        <td>Portland-South Portland, ME</td>
        <td>free</td>
        <td>17</td>
    </tr>
    <tr>
        <td>Portland-South Portland, ME</td>
        <td>paid</td>
        <td>648</td>
    </tr>
    <tr>
        <td>Portland-Vancouver-Hillsboro, OR-WA</td>
        <td>free</td>
        <td>7</td>
    </tr>
    <tr>
        <td>Raleigh, NC</td>
        <td>free</td>
        <td>4</td>
    </tr>
    <tr>
        <td>Red Bluff, CA</td>
        <td>free</td>
        <td>23</td>
    </tr>
    <tr>
        <td>Red Bluff, CA</td>
        <td>paid</td>
        <td>178</td>
    </tr>
    <tr>
        <td>Richmond, VA</td>
        <td>free</td>
        <td>13</td>
    </tr>
    <tr>
        <td>Sacramento--Roseville--Arden-Arcade, CA</td>
        <td>free</td>
        <td>29</td>
    </tr>
    <tr>
        <td>Sacramento--Roseville--Arden-Arcade, CA</td>
        <td>paid</td>
        <td>241</td>
    </tr>
    <tr>
        <td>Saginaw, MI</td>
        <td>free</td>
        <td>3</td>
    </tr>
    <tr>
        <td>Salinas, CA</td>
        <td>free</td>
        <td>37</td>
    </tr>
    <tr>
        <td>Salt Lake City, UT</td>
        <td>free</td>
        <td>4</td>
    </tr>
    <tr>
        <td>San Antonio-New Braunfels, TX</td>
        <td>free</td>
        <td>19</td>
    </tr>
    <tr>
        <td>San Antonio-New Braunfels, TX</td>
        <td>paid</td>
        <td>33</td>
    </tr>
    <tr>
        <td>San Diego-Carlsbad, CA</td>
        <td>free</td>
        <td>5</td>
    </tr>
    <tr>
        <td>San Francisco-Oakland-Hayward, CA</td>
        <td>free</td>
        <td>41</td>
    </tr>
    <tr>
        <td>San Francisco-Oakland-Hayward, CA</td>
        <td>paid</td>
        <td>650</td>
    </tr>
    <tr>
        <td>San Jose-Sunnyvale-Santa Clara, CA</td>
        <td>free</td>
        <td>114</td>
    </tr>
    <tr>
        <td>San Jose-Sunnyvale-Santa Clara, CA</td>
        <td>paid</td>
        <td>178</td>
    </tr>
    <tr>
        <td>Santa Rosa, CA</td>
        <td>free</td>
        <td>20</td>
    </tr>
    <tr>
        <td>Seattle-Tacoma-Bellevue, WA</td>
        <td>free</td>
        <td>14</td>
    </tr>
    <tr>
        <td>St. Louis, MO-IL</td>
        <td>free</td>
        <td>16</td>
    </tr>
    <tr>
        <td>Tampa-St. Petersburg-Clearwater, FL</td>
        <td>free</td>
        <td>18</td>
    </tr>
    <tr>
        <td>Tampa-St. Petersburg-Clearwater, FL</td>
        <td>paid</td>
        <td>289</td>
    </tr>
    <tr>
        <td>Washington-Arlington-Alexandria, DC-VA-MD-WV</td>
        <td>free</td>
        <td>34</td>
    </tr>
    <tr>
        <td>Waterloo-Cedar Falls, IA</td>
        <td>paid</td>
        <td>397</td>
    </tr>
    <tr>
        <td>Winston-Salem, NC</td>
        <td>paid</td>
        <td>213</td>
    </tr>
    <tr>
        <td>Youngstown-Warren-Boardman, OH-PA</td>
        <td>free</td>
        <td>8</td>
    </tr>
    <tr>
        <td>Yuba City, CA</td>
        <td>free</td>
        <td>27</td>
    </tr>
</table>




```python
%%sql
-- what are the top 10 most played artists
select
    row_number() over (order by count(f.artist_id) desc) as rank,
    d1.name as artist_name
from f_songplay f
join d_artist d1 on d1.artist_id = f.artist_id
where f.artist_id is not null
group by f.artist_id, d1.name
order by count(f.artist_id) desc
limit 10;

```

     * postgresql://sparkify:***@172.31.5.253:5439/sparkify
    10 rows affected.





<table>
    <tr>
        <th>rank</th>
        <th>artist_name</th>
    </tr>
    <tr>
        <td>1</td>
        <td>Kings Of Leon</td>
    </tr>
    <tr>
        <td>2</td>
        <td>Foo Fighters</td>
    </tr>
    <tr>
        <td>3</td>
        <td>Lily Allen</td>
    </tr>
    <tr>
        <td>4</td>
        <td>Rise Against</td>
    </tr>
    <tr>
        <td>5</td>
        <td>Black Eyed Peas</td>
    </tr>
    <tr>
        <td>6</td>
        <td>Shakira</td>
    </tr>
    <tr>
        <td>7</td>
        <td>Pearl Jam</td>
    </tr>
    <tr>
        <td>8</td>
        <td>Van Halen</td>
    </tr>
    <tr>
        <td>9</td>
        <td>Weezer</td>
    </tr>
    <tr>
        <td>10</td>
        <td>The Smiths</td>
    </tr>
</table>




```python
%%sql
-- what are the top 10 most played artists by gender
with
female_top10_artists as (
    select
        row_number() over (order by count(f.artist_id) desc, d1.gender, d2.name) as rank,
        d1.gender,
        d2.name
    from f_songplay f
    join d_user d1 on d1.user_id = f.user_id
    join d_artist d2 on d2.artist_id = f.artist_id
    where f.artist_id is not null
    and d1.gender = 'F'
    group by d1.gender, d2.name
    order by count(f.artist_id) desc, d1.gender, d2.name
    limit 10
),
male_top10_artists as (
    select
        row_number() over (order by count(f.artist_id) desc, d1.gender, d2.name) as rank,
        d1.gender,
        d2.name
    from f_songplay f
    join d_user d1 on d1.user_id = f.user_id
    join d_artist d2 on d2.artist_id = f.artist_id
    where f.artist_id is not null
    and d1.gender = 'M'
    group by d1.gender, d2.name
    order by count(f.artist_id) desc, d1.gender, d2.name
    limit 10
)
select F.rank, F.name as female, M.name as male
from female_top10_artists as F
join male_top10_artists as M on F.rank = M.rank;

```

     * postgresql://sparkify:***@172.31.5.253:5439/sparkify
    10 rows affected.





<table>
    <tr>
        <th>rank</th>
        <th>female</th>
        <th>male</th>
    </tr>
    <tr>
        <td>6</td>
        <td>Rise Against</td>
        <td>Pearl Jam</td>
    </tr>
    <tr>
        <td>7</td>
        <td>Shakira</td>
        <td>Shakira</td>
    </tr>
    <tr>
        <td>9</td>
        <td>Kid Cudi</td>
        <td>Van Halen</td>
    </tr>
    <tr>
        <td>3</td>
        <td>Black Eyed Peas</td>
        <td>Lily Allen</td>
    </tr>
    <tr>
        <td>2</td>
        <td>Lily Allen</td>
        <td>Foo Fighters</td>
    </tr>
    <tr>
        <td>4</td>
        <td>Foo Fighters</td>
        <td>Rise Against</td>
    </tr>
    <tr>
        <td>5</td>
        <td>Weezer</td>
        <td>Black Eyed Peas</td>
    </tr>
    <tr>
        <td>10</td>
        <td>Lupe Fiasco</td>
        <td>Calle 13</td>
    </tr>
    <tr>
        <td>1</td>
        <td>Kings Of Leon</td>
        <td>Kings Of Leon</td>
    </tr>
    <tr>
        <td>8</td>
        <td>Shakira Featuring Wyclef Jean</td>
        <td>Shakira Featuring Wyclef Jean</td>
    </tr>
</table>




```python
%%sql
-- where are most Colplay song plays occuring?
select count(f.location), f.location
from f_songplay f
join d_artist d1 on d1.artist_id = f.artist_id
where d1.name = 'Coldplay'
group by f.location
order by count(f.location) desc
limit 10;

```

     * postgresql://sparkify:***@172.31.5.253:5439/sparkify
    0 rows affected.





<table>
    <tr>
        <th>count</th>
        <th>location</th>
    </tr>
</table>




```python
%%sql
-- where are most Kings of Leon song plays occuring?
select count(f.location), f.location
from f_songplay f
join d_artist d1 on d1.artist_id = f.artist_id
where d1.name = 'Kings Of Leon'
group by f.location
order by count(f.location) desc
limit 10;
```

     * postgresql://sparkify:***@172.31.5.253:5439/sparkify
    10 rows affected.





<table>
    <tr>
        <th>count</th>
        <th>location</th>
    </tr>
    <tr>
        <td>9</td>
        <td>San Francisco-Oakland-Hayward, CA</td>
    </tr>
    <tr>
        <td>9</td>
        <td>Atlanta-Sandy Springs-Roswell, GA</td>
    </tr>
    <tr>
        <td>7</td>
        <td>Lansing-East Lansing, MI</td>
    </tr>
    <tr>
        <td>5</td>
        <td>Chicago-Naperville-Elgin, IL-IN-WI</td>
    </tr>
    <tr>
        <td>4</td>
        <td>Waterloo-Cedar Falls, IA</td>
    </tr>
    <tr>
        <td>3</td>
        <td>New York-Newark-Jersey City, NY-NJ-PA</td>
    </tr>
    <tr>
        <td>3</td>
        <td>Lake Havasu City-Kingman, AZ</td>
    </tr>
    <tr>
        <td>3</td>
        <td>Portland-South Portland, ME</td>
    </tr>
    <tr>
        <td>2</td>
        <td>Dallas-Fort Worth-Arlington, TX</td>
    </tr>
    <tr>
        <td>2</td>
        <td>Janesville-Beloit, WI</td>
    </tr>
</table>




```python

```
