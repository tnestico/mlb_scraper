# MLB Scraper

This Python module provides a class `MLB_Scrape` that interacts with the MLB Stats API to retrieve various types of baseball-related data. The data is processed and returned as Polars DataFrames for easy manipulation and analysis.

## Requirements

- Python 3.x
- `requests` library
- `polars` library
- `numpy` library
- `tqdm` library
- `pytz` library

You can install the required libraries using pip:

```sh 
pip install requests polars numpy tqdm pytz
```

## Class and Methods

### MLB_Scrape

#### `__init__(self)`
Initializes the `MLB_Scrape` class.

#### `get_sport_id(self)`
Retrieves the list of sports from the MLB API and processes it into a Polars DataFrame.

- **Returns**: `pl.DataFrame` - A DataFrame containing the sports information.

#### `get_sport_id_check(self, sport_id: int = 1)`
Checks if the provided sport ID exists in the list of sports retrieved from the MLB API.

- **Parameters**:
  - `sport_id` (int): The sport ID to check. Default is 1.
- **Returns**: `bool` - True if the sport ID exists, False otherwise. If False, prints the available sport IDs.

#### `get_schedule(self, year_input: list = [2024], sport_id: list = [1], game_type: list = ['R'])`
Retrieves the schedule of baseball games based on the specified parameters.

- **Parameters**:
  - `year_input` (list): A list of years to filter the schedule. Default is [2024].
  - `sport_id` (list): A list of sport IDs to filter the schedule. Default is [1].
  - `game_type` (list): A list of game types to filter the schedule. Default is ['R'].
- **Returns**: `pl.DataFrame` - A DataFrame containing the game schedule information, including game ID, date, time, away team, home team, game state, venue ID, and venue name. If the schedule length is 0, it returns a message indicating that different parameters should be selected.

#### `get_data(self, game_list_input: list)`
Retrieves live game data for a list of game IDs.

- **Parameters**:
  - `game_list_input` (list): A list of game IDs for which to retrieve live data.
- **Returns**: `list` - A list of JSON responses containing live game data for each game ID.

#### `get_teams(self)`
Retrieves information about MLB teams from the MLB API and processes it into a Polars DataFrame.

- **Returns**: `pl.DataFrame` - A DataFrame containing team information, including team ID, city, name, franchise, abbreviation, parent organization ID, parent organization name, league ID, and league name.

#### `get_leagues(self)`
Retrieves information about MLB leagues from the MLB API and processes it into a Polars DataFrame.

- **Returns**: `pl.DataFrame` - A DataFrame containing league information, including league ID, league name, league abbreviation, and sport ID.

#### `get_player_games_list(self, player_id: int, season: int)`
Retrieves a list of game IDs for a specific player in a given season.

- **Parameters**:
  - `player_id` (int): The ID of the player.
  - `season` (int): The season year for which to retrieve the game list.
- **Returns**: `list` - A list of game IDs in which the player participated during the specified season.

#### `get_data_df(self, data_list: list)`
Converts a list of game data JSON objects into a Polars DataFrame.

- **Parameters**:
  - `data_list` (list): A list of JSON objects containing game data.
- **Returns**: `pl.DataFrame` - A DataFrame containing the structured game data.

#### `get_game_types(self)`
Retrieves the different types of MLB games from the MLB API and processes them into a Polars DataFrame.

- **Returns**: `pl.DataFrame` - A DataFrame containing the game types information.

## Usage
```python
from api_scraper import MLB_Scrape

# Initialize the scraper
scraper = MLB_Scrape()

# Get sport IDs
sport_ids = scraper.get_sport_id()
print(sport_ids)

shape: (18, 7)
┌──────┬──────┬─────────────────────┬────────────────────┬──────────────┬───────────┬──────────────┐
│ id   ┆ code ┆ link                ┆ name               ┆ abbreviation ┆ sortOrder ┆ activeStatus │
│ ---  ┆ ---  ┆ ---                 ┆ ---                ┆ ---          ┆ ---       ┆ ---          │
│ i64  ┆ str  ┆ str                 ┆ str                ┆ str          ┆ i64       ┆ bool         │
╞══════╪══════╪═════════════════════╪════════════════════╪══════════════╪═══════════╪══════════════╡
│ 1    ┆ mlb  ┆ /api/v1/sports/1    ┆ Major League       ┆ MLB          ┆ 11        ┆ true         │
│      ┆      ┆                     ┆ Baseball           ┆              ┆           ┆              │
│ 11   ┆ aaa  ┆ /api/v1/sports/11   ┆ Triple-A           ┆ AAA          ┆ 101       ┆ true         │
│ 12   ┆ aax  ┆ /api/v1/sports/12   ┆ Double-A           ┆ AA           ┆ 201       ┆ true         │
│ 13   ┆ afa  ┆ /api/v1/sports/13   ┆ High-A             ┆ A+           ┆ 301       ┆ true         │
│ 14   ┆ afx  ┆ /api/v1/sports/14   ┆ Single-A           ┆ A            ┆ 401       ┆ true         │
│ …    ┆ …    ┆ …                   ┆ …                  ┆ …            ┆ …         ┆ …            │
│ 509  ┆ nae  ┆ /api/v1/sports/509  ┆ International      ┆ 18U          ┆ 3503      ┆ true         │
│      ┆      ┆                     ┆ Baseball (18U)     ┆              ┆           ┆              │
│ 510  ┆ nas  ┆ /api/v1/sports/510  ┆ International      ┆ 16U          ┆ 3505      ┆ true         │
│      ┆      ┆                     ┆ Baseball (16 and…  ┆              ┆           ┆              │
│ 6005 ┆ ame  ┆ /api/v1/sports/6005 ┆ International      ┆ AME          ┆ 3509      ┆ true         │
│      ┆      ┆                     ┆ Baseball (amateu…  ┆              ┆           ┆              │
│ 22   ┆ bbc  ┆ /api/v1/sports/22   ┆ College Baseball   ┆ College      ┆ 5101      ┆ true         │
│ 586  ┆ hsb  ┆ /api/v1/sports/586  ┆ High School        ┆ H.S.         ┆ 6201      ┆ true         │
│      ┆      ┆                     ┆ Baseball           ┆              ┆           ┆              │
└──────┴──────┴─────────────────────┴────────────────────┴──────────────┴───────────┴──────────────┘

# Get the game schedule
schedule = scraper.get_schedule(year_input=[2024], sport_id=[1], game_type=['R'])
print(schedule)

shape: (2_430, 8)
┌─────────┬──────────┬────────────┬───────────────┬──────────────┬───────┬──────────┬──────────────┐
│ game_id ┆ time     ┆ date       ┆ away          ┆ home         ┆ state ┆ venue_id ┆ venue_name   │
│ ---     ┆ ---      ┆ ---        ┆ ---           ┆ ---          ┆ ---   ┆ ---      ┆ ---          │
│ i64     ┆ str      ┆ date       ┆ str           ┆ str          ┆ str   ┆ i64      ┆ str          │
╞═════════╪══════════╪════════════╪═══════════════╪══════════════╪═══════╪══════════╪══════════════╡
│ 745444  ┆ 06:05 AM ┆ 2024-03-20 ┆ Los Angeles   ┆ San Diego    ┆ F     ┆ 5150     ┆ Gocheok Sky  │
│         ┆          ┆            ┆ Dodgers       ┆ Padres       ┆       ┆          ┆ Dome         │
│ 746175  ┆ 06:05 AM ┆ 2024-03-21 ┆ San Diego     ┆ Los Angeles  ┆ F     ┆ 5150     ┆ Gocheok Sky  │
│         ┆          ┆            ┆ Padres        ┆ Dodgers      ┆       ┆          ┆ Dome         │
│ 746418  ┆ 04:10 PM ┆ 2024-03-28 ┆ New York      ┆ Houston      ┆ F     ┆ 2392     ┆ Minute Maid  │
│         ┆          ┆            ┆ Yankees       ┆ Astros       ┆       ┆          ┆ Park         │
│ 746335  ┆ 04:10 PM ┆ 2024-03-28 ┆ Minnesota     ┆ Kansas City  ┆ F     ┆ 7        ┆ Kauffman     │
│         ┆          ┆            ┆ Twins         ┆ Royals       ┆       ┆          ┆ Stadium      │
│ 746165  ┆ 04:10 PM ┆ 2024-03-28 ┆ St. Louis     ┆ Los Angeles  ┆ F     ┆ 22       ┆ Dodger       │
│         ┆          ┆            ┆ Cardinals     ┆ Dodgers      ┆       ┆          ┆ Stadium      │
│ …       ┆ …        ┆ …          ┆ …             ┆ …            ┆ …     ┆ …        ┆ …            │
│ 745282  ┆ 03:05 PM ┆ 2024-09-29 ┆ St. Louis     ┆ San          ┆ S     ┆ 2395     ┆ Oracle Park  │
│         ┆          ┆            ┆ Cardinals     ┆ Francisco    ┆       ┆          ┆              │
│         ┆          ┆            ┆               ┆ Giants       ┆       ┆          ┆              │
│ 744880  ┆ 03:07 PM ┆ 2024-09-29 ┆ Miami Marlins ┆ Toronto Blue ┆ S     ┆ 14       ┆ Rogers       │
│         ┆          ┆            ┆               ┆ Jays         ┆       ┆          ┆ Centre       │
│ 746577  ┆ 03:10 PM ┆ 2024-09-29 ┆ Houston       ┆ Cleveland    ┆ S     ┆ 5        ┆ Progressive  │
│         ┆          ┆            ┆ Astros        ┆ Guardians    ┆       ┆          ┆ Field        │
│ 745932  ┆ 03:10 PM ┆ 2024-09-29 ┆ New York Mets ┆ Milwaukee    ┆ S     ┆ 32       ┆ American     │
│         ┆          ┆            ┆               ┆ Brewers      ┆       ┆          ┆ Family Field │
│ 747147  ┆ 03:10 PM ┆ 2024-09-29 ┆ San Diego     ┆ Arizona      ┆ S     ┆ 15       ┆ Chase Field  │
│         ┆          ┆            ┆ Padres        ┆ Diamondbacks ┆       ┆          ┆              │
└─────────┴──────────┴────────────┴───────────────┴──────────────┴───────┴──────────┴──────────────┘

# Get live game data
game_data = scraper.get_data(game_list_input=[745444,746175])

# Convert game data to DataFrame
data_df = scraper.get_data_df(data_list=game_data)
print(data_df)

shape: (690, 78)
┌─────────┬────────────┬───────────┬──────────────┬───┬────────────┬─────┬────────────┬────────────┐
│ game_id ┆ game_date  ┆ batter_id ┆ batter_name  ┆ … ┆ event_type ┆ rbi ┆ away_score ┆ home_score │
│ ---     ┆ ---        ┆ ---       ┆ ---          ┆   ┆ ---        ┆ --- ┆ ---        ┆ ---        │
│ i64     ┆ str        ┆ i64       ┆ str          ┆   ┆ str        ┆ f64 ┆ f64        ┆ f64        │
╞═════════╪════════════╪═══════════╪══════════════╪═══╪════════════╪═════╪════════════╪════════════╡
│ 745444  ┆ 2024-03-20 ┆ 605141    ┆ Mookie Betts ┆ … ┆ NaN        ┆ NaN ┆ NaN        ┆ NaN        │
│ 745444  ┆ 2024-03-20 ┆ 605141    ┆ Mookie Betts ┆ … ┆ NaN        ┆ NaN ┆ NaN        ┆ NaN        │
│ 745444  ┆ 2024-03-20 ┆ 605141    ┆ Mookie Betts ┆ … ┆ NaN        ┆ NaN ┆ NaN        ┆ NaN        │
│ 745444  ┆ 2024-03-20 ┆ 605141    ┆ Mookie Betts ┆ … ┆ NaN        ┆ NaN ┆ NaN        ┆ NaN        │
│ 745444  ┆ 2024-03-20 ┆ 605141    ┆ Mookie Betts ┆ … ┆ walk       ┆ 0.0 ┆ 0.0        ┆ 0.0        │
│ …       ┆ …          ┆ …         ┆ …            ┆ … ┆ …          ┆ …   ┆ …          ┆ …          │
│ 746175  ┆ 2024-03-21 ┆ 669257    ┆ Will Smith   ┆ … ┆ NaN        ┆ NaN ┆ NaN        ┆ NaN        │
│ 746175  ┆ 2024-03-21 ┆ 669257    ┆ Will Smith   ┆ … ┆ field_out  ┆ 0.0 ┆ 15.0       ┆ 11.0       │
│ 746175  ┆ 2024-03-21 ┆ 571970    ┆ Max Muncy    ┆ … ┆ NaN        ┆ NaN ┆ NaN        ┆ NaN        │
│ 746175  ┆ 2024-03-21 ┆ 571970    ┆ Max Muncy    ┆ … ┆ NaN        ┆ NaN ┆ NaN        ┆ NaN        │
│ 746175  ┆ 2024-03-21 ┆ 571970    ┆ Max Muncy    ┆ … ┆ strikeout  ┆ 0.0 ┆ 15.0       ┆ 11.0       │
└─────────┴────────────┴───────────┴──────────────┴───┴────────────┴─────┴────────────┴────────────┘
```

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

