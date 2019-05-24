
# Project 03: Data Warehouse



```python
import configparser
import psycopg2
%reload_ext sql
```


```python
config = configparser.ConfigParser()
config.read('pg_dwh.cfg')

db_host = config['CLUSTER']['HOST']
db_port = config['CLUSTER']['DB_PORT']
db_name = config['CLUSTER']['DB_NAME']
db_user = config['CLUSTER']['DB_USER']
db_pass = config['CLUSTER']['DB_PASSWORD']

connection_string = "postgresql://{user}:{password}@{host}:{port}/{name}".format(user=db_user, password=db_pass, host=db_host, port=db_port, name=db_name)
print('connection_string=%s' % connection_string) 

%sql $connection_string
```

    connection_string=postgresql://sparkify:sparkify@127.0.0.1:5432/sparkify





    'Connected: sparkify@sparkify'




```python
%%sql
-- what are the free and paid user counts by location?
select f.location, f.level, count(level)
from f_songplay f
group by f.location, f.level
order by f.location, f.level;

```

     * postgresql://sparkify:***@127.0.0.1:5432/sparkify
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

     * postgresql://sparkify:***@127.0.0.1:5432/sparkify
    10 rows affected.





<table>
    <tr>
        <th>rank</th>
        <th>artist_name</th>
    </tr>
    <tr>
        <td>1</td>
        <td>Coldplay</td>
    </tr>
    <tr>
        <td>2</td>
        <td>Kings Of Leon</td>
    </tr>
    <tr>
        <td>3</td>
        <td>Dwight Yoakam</td>
    </tr>
    <tr>
        <td>4</td>
        <td>The Black Keys</td>
    </tr>
    <tr>
        <td>5</td>
        <td>Muse</td>
    </tr>
    <tr>
        <td>6</td>
        <td>Jack Johnson</td>
    </tr>
    <tr>
        <td>7</td>
        <td>The Killers</td>
    </tr>
    <tr>
        <td>8</td>
        <td>John Mayer</td>
    </tr>
    <tr>
        <td>9</td>
        <td>Radiohead</td>
    </tr>
    <tr>
        <td>10</td>
        <td>Alliance Ethnik</td>
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

     * postgresql://sparkify:***@127.0.0.1:5432/sparkify
    10 rows affected.





<table>
    <tr>
        <th>rank</th>
        <th>female</th>
        <th>male</th>
    </tr>
    <tr>
        <td>1</td>
        <td>Coldplay</td>
        <td>Coldplay</td>
    </tr>
    <tr>
        <td>2</td>
        <td>Kings Of Leon</td>
        <td>Kings Of Leon</td>
    </tr>
    <tr>
        <td>3</td>
        <td>Dwight Yoakam</td>
        <td>The Black Keys</td>
    </tr>
    <tr>
        <td>4</td>
        <td>Alliance Ethnik</td>
        <td>Metallica</td>
    </tr>
    <tr>
        <td>5</td>
        <td>Jack Johnson</td>
        <td>Muse</td>
    </tr>
    <tr>
        <td>6</td>
        <td>Linkin Park</td>
        <td>Jack Johnson</td>
    </tr>
    <tr>
        <td>7</td>
        <td>Muse</td>
        <td>John Mayer</td>
    </tr>
    <tr>
        <td>8</td>
        <td>Kanye West</td>
        <td>Dwight Yoakam</td>
    </tr>
    <tr>
        <td>9</td>
        <td>OneRepublic</td>
        <td>Radiohead</td>
    </tr>
    <tr>
        <td>10</td>
        <td>The Killers</td>
        <td>The Killers</td>
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

     * postgresql://sparkify:***@127.0.0.1:5432/sparkify
    10 rows affected.





<table>
    <tr>
        <th>count</th>
        <th>location</th>
    </tr>
    <tr>
        <td>10</td>
        <td>Lansing-East Lansing, MI</td>
    </tr>
    <tr>
        <td>7</td>
        <td>San Francisco-Oakland-Hayward, CA</td>
    </tr>
    <tr>
        <td>5</td>
        <td>Lake Havasu City-Kingman, AZ</td>
    </tr>
    <tr>
        <td>4</td>
        <td>Augusta-Richmond County, GA-SC</td>
    </tr>
    <tr>
        <td>4</td>
        <td>Portland-South Portland, ME</td>
    </tr>
    <tr>
        <td>3</td>
        <td>Chicago-Naperville-Elgin, IL-IN-WI</td>
    </tr>
    <tr>
        <td>3</td>
        <td>Janesville-Beloit, WI</td>
    </tr>
    <tr>
        <td>3</td>
        <td>Nashville-Davidson--Murfreesboro--Franklin, TN</td>
    </tr>
    <tr>
        <td>2</td>
        <td>Red Bluff, CA</td>
    </tr>
    <tr>
        <td>2</td>
        <td>Atlanta-Sandy Springs-Roswell, GA</td>
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

     * postgresql://sparkify:***@127.0.0.1:5432/sparkify
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
        <td>Portland-South Portland, ME</td>
    </tr>
    <tr>
        <td>3</td>
        <td>Lake Havasu City-Kingman, AZ</td>
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
