import requests
import polars as pl
import numpy as np
from datetime import datetime
from tqdm import tqdm
from pytz import timezone
import re
from concurrent.futures import ThreadPoolExecutor, as_completed


class MLB_Scrape:

    def __init__(self):
        # Initialize your class here if needed
        pass

    def get_sport_id(self):
        """
        Retrieves the list of sports from the MLB API and processes it into a Polars DataFrame.
        
        Returns:
        - df (pl.DataFrame): A DataFrame containing the sports information.
        """
        # Make API call to retrieve sports information
        response = requests.get(url='https://statsapi.mlb.com/api/v1/sports').json()
        
        # Convert the JSON response into a Polars DataFrame
        df = pl.DataFrame(response['sports'])
        
        return df

    def get_sport_id_check(self, sport_id: int = 1):
        """
        Checks if the provided sport ID exists in the list of sports retrieved from the MLB API.
        
        Parameters:
        - sport_id (int): The sport ID to check. Default is 1.
        
        Returns:
        - bool: True if the sport ID exists, False otherwise. If False, prints the available sport IDs.
        """
        # Retrieve the list of sports from the MLB API
        sport_id_df = self.get_sport_id()
        
        # Check if the provided sport ID exists in the DataFrame
        if sport_id not in sport_id_df['id']:
            print('Please Select a New Sport ID from the following')
            print(sport_id_df)
            return False
        
        return True


    def get_game_types(self):
        """
        Retrieves the different types of MLB games from the MLB API and processes them into a Polars DataFrame.
        
        Returns:
        - df (pl.DataFrame): A DataFrame containing the game types information.
        """
        # Make API call to retrieve game types information
        response = requests.get(url='https://statsapi.mlb.com/api/v1/gameTypes').json()
        
        # Convert the JSON response into a Polars DataFrame
        df = pl.DataFrame(response)
        
        return df

    def get_schedule(self,
                    year_input: list = [2024],
                    sport_id: list = [1],
                    game_type: list = ['R']):
        
        """
        Retrieves the schedule of baseball games based on the specified parameters.
        Parameters:
        - year_input (list): A list of years to filter the schedule. Default is [2024].
        - sport_id (list): A list of sport IDs to filter the schedule. Default is [1].
        - game_type (list): A list of game types to filter the schedule. Default is ['R'].
        Returns:
        - game_df (pandas.DataFrame): A DataFrame containing the game schedule information, including game ID, date, time, away team, home team, game state, venue ID, and venue name. If the schedule length is 0, it returns a message indicating that different parameters should be selected.
        """

        # Type checks
        if not isinstance(year_input, list) or not all(isinstance(year, int) for year in year_input):
            raise ValueError("year_input must be a list of integers.")
        if not isinstance(sport_id, list) or not all(isinstance(sid, int) for sid in sport_id):
            raise ValueError("sport_id must be a list of integers.")

        if not isinstance(game_type, list) or not all(isinstance(gt, str) for gt in game_type):
            raise ValueError("game_type must be a list of strings.")

        eastern = timezone('US/Eastern')

        # Convert input lists to comma-separated strings
        year_input_str = ','.join([str(x) for x in year_input])
        sport_id_str = ','.join([str(x) for x in sport_id])
        game_type_str = ','.join([str(x) for x in game_type])

        # Make API call to retrieve game schedule
        game_call = requests.get(url=f'https://statsapi.mlb.com/api/v1/schedule/?sportId={sport_id_str}&gameTypes={game_type_str}&season={year_input_str}&hydrate=lineup,players').json()
        try:
            # Extract relevant data from the API response
            game_list = [item for sublist in [[y.get('gamePk') for y in x['games']] for x in game_call['dates']] for item in sublist]
            time_list = [item for sublist in [[y.get('gameDate') for y in x['games']] for x in game_call['dates']] for item in sublist]
            date_list = [item for sublist in [[y.get('officialDate') for y in x['games']] for x in game_call['dates']] for item in sublist]
            away_team_list = [item for sublist in [[y['teams']['away']['team'].get('name') for y in x['games']] for x in game_call['dates']] for item in sublist]
            home_team_list = [item for sublist in [[y['teams']['home']['team'].get('name') for y in x['games']] for x in game_call['dates']] for item in sublist]
            state_list = [item for sublist in [[y['status'].get('codedGameState') for y in x['games']] for x in game_call['dates']] for item in sublist]
            venue_id = [item for sublist in [[y['venue'].get('id', None) for y in x['games']] for x in game_call['dates']] for item in sublist]
            venue_name = [item for sublist in [[y['venue'].get('name') for y in x['games']] for x in game_call['dates']] for item in sublist]

            # Create a Polars DataFrame with the extracted data
            game_df = pl.DataFrame(data={'game_id': game_list,
                                        'time': time_list,
                                        'date': date_list,
                                        'away': away_team_list,
                                        'home': home_team_list,
                                        'state': state_list,
                                        'venue_id': venue_id,
                                        'venue_name': venue_name})

        
            # Check if the DataFrame is empty
            if len(game_df) == 0:
                print('Schedule Length of 0, please select different parameters.')
                return None

            # Convert date and time columns to appropriate formats
            game_df = game_df.with_columns(
                game_df['date'].str.to_date(),
                game_df['time'].str.to_datetime().dt.convert_time_zone(eastern.zone).dt.strftime("%I:%M %p"))

            # Remove duplicate games and sort by date
            game_df = game_df.unique(subset='game_id').sort('date')

            # Check again if the DataFrame is empty after processing
            if len(game_df) == 0:
                print('Schedule Length of 0, please select different parameters.')
                return None
        except KeyError:
            print('No Data for Selected Parameters')
            return None
        

        return game_df
    

    def get_data(self, game_list_input: list):
        """
        Retrieves live game data for a list of game IDs in parallel.
        
        Parameters:
        - game_list_input (list): A list of game IDs for which to retrieve live data.
        
        Returns:
        - data_total (list): A list of JSON responses containing live game data for each game ID.
        """
        data_total = []
        print('This May Take a While. Progress Bar shows Completion of Data Retrieval.')
        
        def fetch_data(game_id):
            r = requests.get(f'https://statsapi.mlb.com/api/v1.1/game/{game_id}/feed/live')
            return r.json()
        
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(fetch_data, game_id): game_id for game_id in game_list_input}
            for future in tqdm(as_completed(futures), total=len(futures), desc="Processing", unit="iteration"):
                data_total.append(future.result())
        
        return data_total

    def get_data_df(self, data_list):
        """
        Converts a list of game data JSON objects into a Polars DataFrame.
        
        Parameters:
        - data_list (list): A list of JSON objects containing game data.
        
        Returns:
        - data_df (pl.DataFrame): A DataFrame containing the structured game data.
        """
        swing_list = ['X','F','S','D','E','T','W']
        whiff_list = ['S','T','W']
        print('Converting Data to Dataframe.')
        game_id = []
        game_date = []
        batter_id = []
        batter_name = []
        batter_hand = []
        batter_team = []
        batter_team_id = []
        pitcher_id = []
        pitcher_name = []
        pitcher_hand = []
        pitcher_team = []
        pitcher_team_id = []

        play_description = []
        play_code = []
        in_play = []
        is_strike = []
        is_swing = []
        is_whiff = []
        is_out = []
        is_ball = []
        is_review = []
        pitch_type = []
        pitch_description = []
        strikes = []
        balls = []
        outs = []
        strikes_after = []
        balls_after = []
        outs_after = []

        start_speed = []
        end_speed = []
        sz_top = []
        sz_bot = []
        x = []
        y = []
        ax = []
        ay = []
        az = []
        pfxx = []
        pfxz = []
        px = []
        pz = []
        vx0 = []
        vy0 = []
        vz0 = []
        x0 = []
        y0 = []
        z0 = []
        zone = []
        type_confidence = []
        plate_time = []
        extension = []
        spin_rate = []
        spin_direction = []
        vb = []
        ivb = []
        hb = []

        launch_speed = []
        launch_angle = []
        launch_distance = []
        launch_location = []
        trajectory = []
        hardness = []
        hit_x = []
        hit_y = []

        index_play = []
        play_id = []
        start_time = []
        end_time = []
        is_pitch = []
        type_type = []


        type_ab = []
        ab_number = []
        event = []
        event_type = []
        rbi = []
        away_score = []
        home_score = []

        for data in data_list:
            try:
                for ab_id in range(len(data['liveData']['plays']['allPlays'])):
                    ab_list = data['liveData']['plays']['allPlays'][ab_id]
                    for n in range(len(ab_list['playEvents'])):
                
                        
                        if ab_list['playEvents'][n]['isPitch'] == True or 'call' in ab_list['playEvents'][n]['details']:
                            ab_number.append(ab_list['atBatIndex'] if 'atBatIndex' in ab_list else None)
    
                            game_id.append(data['gamePk'])
                            game_date.append(data['gameData']['datetime']['officialDate'])
                            if 'matchup' in ab_list:
                              batter_id.append(ab_list['matchup']['batter']['id'] if 'batter' in ab_list['matchup'] else None)
                              if 'batter' in ab_list['matchup']:
                                batter_name.append(ab_list['matchup']['batter']['fullName'] if 'fullName' in ab_list['matchup']['batter'] else None)
                              else:
                                batter_name.append(None)
    
                              batter_hand.append(ab_list['matchup']['batSide']['code'] if 'batSide' in ab_list['matchup'] else None)
                              pitcher_id.append(ab_list['matchup']['pitcher']['id'] if 'pitcher' in ab_list['matchup'] else None)
                              if 'pitcher' in ab_list['matchup']:
                                pitcher_name.append(ab_list['matchup']['pitcher']['fullName'] if 'fullName' in ab_list['matchup']['pitcher'] else None)
                              else:
                                pitcher_name.append(None)
                            
                              pitcher_hand.append(ab_list['matchup']['pitchHand']['code'] if 'pitchHand' in ab_list['matchup'] else None)
    
    
                            if ab_list['about']['isTopInning']:
                                batter_team.append(data['gameData']['teams']['away']['abbreviation'] if 'away' in data['gameData']['teams'] else None)
                                batter_team_id.append(data['gameData']['teams']['away']['id'] if 'away' in data['gameData']['teams'] else None)
                                pitcher_team.append(data['gameData']['teams']['home']['abbreviation'] if 'home' in data['gameData']['teams'] else None)
                                pitcher_team_id.append(data['gameData']['teams']['home']['id'] if 'home' in data['gameData']['teams'] else None)
    
                            else:
                                batter_team.append(data['gameData']['teams']['home']['abbreviation'] if 'home' in data['gameData']['teams'] else None)
                                batter_team_id.append(data['gameData']['teams']['home']['id'] if 'home' in data['gameData']['teams'] else None)
                                pitcher_team.append(data['gameData']['teams']['away']['abbreviation'] if 'away' in data['gameData']['teams'] else None)
                                pitcher_team_id.append(data['gameData']['teams']['away']['id'] if 'away' in data['gameData']['teams'] else None)
    
                            play_description.append(ab_list['playEvents'][n]['details']['description'] if 'description' in ab_list['playEvents'][n]['details'] else None)
                            play_code.append(ab_list['playEvents'][n]['details']['code'] if 'code' in ab_list['playEvents'][n]['details'] else None)
                            in_play.append(ab_list['playEvents'][n]['details']['isInPlay'] if 'isInPlay' in ab_list['playEvents'][n]['details'] else None)
                            is_strike.append(ab_list['playEvents'][n]['details']['isStrike'] if 'isStrike' in ab_list['playEvents'][n]['details'] else None)
    
                            if 'details' in ab_list['playEvents'][n]:
                                is_swing.append(True if ab_list['playEvents'][n]['details']['code'] in swing_list else None)
                                is_whiff.append(True if ab_list['playEvents'][n]['details']['code'] in whiff_list else None)
                            else:
                                is_swing.append(None)
                                is_whiff.append(None)
    
                            is_ball.append(ab_list['playEvents'][n]['details']['isOut'] if 'isOut' in ab_list['playEvents'][n]['details'] else None)
                            is_review.append(ab_list['playEvents'][n]['details']['hasReview'] if 'hasReview' in ab_list['playEvents'][n]['details'] else None)
                            pitch_type.append(ab_list['playEvents'][n]['details']['type']['code'] if 'type' in ab_list['playEvents'][n]['details'] else None)
                            pitch_description.append(ab_list['playEvents'][n]['details']['type']['description'] if 'type' in ab_list['playEvents'][n]['details'] else None)
    
                            if ab_list['playEvents'][n]['pitchNumber'] == 1:
                                strikes.append(0)
                                balls.append(0)
                                strikes_after.append(ab_list['playEvents'][n]['count']['strikes'] if 'strikes' in ab_list['playEvents'][n]['count'] else None)
                                balls_after.append(ab_list['playEvents'][n]['count']['balls'] if 'balls' in ab_list['playEvents'][n]['count'] else None)
                                outs.append(ab_list['playEvents'][n]['count']['outs'] if 'outs' in ab_list['playEvents'][n]['count'] else None)
                                outs_after.append(ab_list['playEvents'][n]['count']['outs'] if 'outs' in ab_list['playEvents'][n]['count'] else None)
    
                            else:
                                strikes.append(ab_list['playEvents'][n-1]['count']['strikes'] if 'strikes' in ab_list['playEvents'][n-1]['count'] else None)
                                balls.append(ab_list['playEvents'][n-1]['count']['balls'] if 'balls' in ab_list['playEvents'][n-1]['count'] else None)
                                outs.append(ab_list['playEvents'][n-1]['count']['outs'] if 'outs' in ab_list['playEvents'][n-1]['count'] else None)
    
                                strikes_after.append(ab_list['playEvents'][n]['count']['strikes'] if 'strikes' in ab_list['playEvents'][n]['count'] else None)
                                balls_after.append(ab_list['playEvents'][n]['count']['balls'] if 'balls' in ab_list['playEvents'][n]['count'] else None)
                                outs_after.append(ab_list['playEvents'][n]['count']['outs'] if 'outs' in ab_list['playEvents'][n]['count'] else None)
    
    
                            if 'pitchData' in ab_list['playEvents'][n]:
    
                                start_speed.append(ab_list['playEvents'][n]['pitchData']['startSpeed'] if 'startSpeed' in ab_list['playEvents'][n]['pitchData'] else None)
                                end_speed.append(ab_list['playEvents'][n]['pitchData']['endSpeed'] if 'endSpeed' in ab_list['playEvents'][n]['pitchData'] else None)
    
                                sz_top.append(ab_list['playEvents'][n]['pitchData']['strikeZoneTop'] if 'strikeZoneTop' in ab_list['playEvents'][n]['pitchData'] else None)
                                sz_bot.append(ab_list['playEvents'][n]['pitchData']['strikeZoneBottom'] if 'strikeZoneBottom' in ab_list['playEvents'][n]['pitchData'] else None)
                                x.append(ab_list['playEvents'][n]['pitchData']['coordinates']['x'] if 'x' in ab_list['playEvents'][n]['pitchData']['coordinates'] else None)
                                y.append(ab_list['playEvents'][n]['pitchData']['coordinates']['y'] if 'y' in ab_list['playEvents'][n]['pitchData']['coordinates'] else None)
    
                                ax.append(ab_list['playEvents'][n]['pitchData']['coordinates']['aX'] if 'aX' in ab_list['playEvents'][n]['pitchData']['coordinates'] else None)
                                ay.append(ab_list['playEvents'][n]['pitchData']['coordinates']['aY'] if 'aY' in ab_list['playEvents'][n]['pitchData']['coordinates'] else None)
                                az.append(ab_list['playEvents'][n]['pitchData']['coordinates']['aZ'] if 'aZ' in ab_list['playEvents'][n]['pitchData']['coordinates'] else None)
                                pfxx.append(ab_list['playEvents'][n]['pitchData']['coordinates']['pfxX'] if 'pfxX' in ab_list['playEvents'][n]['pitchData']['coordinates'] else None)
                                pfxz.append(ab_list['playEvents'][n]['pitchData']['coordinates']['pfxZ'] if 'pfxZ' in ab_list['playEvents'][n]['pitchData']['coordinates'] else None)
                                px.append(ab_list['playEvents'][n]['pitchData']['coordinates']['pX'] if 'pX' in ab_list['playEvents'][n]['pitchData']['coordinates'] else None)
                                pz.append(ab_list['playEvents'][n]['pitchData']['coordinates']['pZ'] if 'pZ' in ab_list['playEvents'][n]['pitchData']['coordinates'] else None)
                                vx0.append(ab_list['playEvents'][n]['pitchData']['coordinates']['vX0'] if 'vX0' in ab_list['playEvents'][n]['pitchData']['coordinates'] else None)
                                vy0.append(ab_list['playEvents'][n]['pitchData']['coordinates']['vY0'] if 'vY0' in ab_list['playEvents'][n]['pitchData']['coordinates'] else None)
                                vz0.append(ab_list['playEvents'][n]['pitchData']['coordinates']['vZ0'] if 'vZ0' in ab_list['playEvents'][n]['pitchData']['coordinates'] else None)
                                x0.append(ab_list['playEvents'][n]['pitchData']['coordinates']['x0'] if 'x0' in ab_list['playEvents'][n]['pitchData']['coordinates'] else None)
                                y0.append(ab_list['playEvents'][n]['pitchData']['coordinates']['y0'] if 'y0' in ab_list['playEvents'][n]['pitchData']['coordinates'] else None)
                                z0.append(ab_list['playEvents'][n]['pitchData']['coordinates']['z0'] if 'z0' in ab_list['playEvents'][n]['pitchData']['coordinates'] else None)
    
                                zone.append(ab_list['playEvents'][n]['pitchData']['zone'] if 'zone' in ab_list['playEvents'][n]['pitchData'] else None)
                                type_confidence.append(ab_list['playEvents'][n]['pitchData']['typeConfidence'] if 'typeConfidence' in ab_list['playEvents'][n]['pitchData'] else None)
                                plate_time.append(ab_list['playEvents'][n]['pitchData']['plateTime'] if 'plateTime' in ab_list['playEvents'][n]['pitchData'] else None)
                                extension.append(ab_list['playEvents'][n]['pitchData']['extension'] if 'extension' in ab_list['playEvents'][n]['pitchData'] else None)
    
                                if 'breaks' in ab_list['playEvents'][n]['pitchData']:
                                    spin_rate.append(ab_list['playEvents'][n]['pitchData']['breaks']['spinRate'] if 'spinRate' in ab_list['playEvents'][n]['pitchData']['breaks'] else None)
                                    spin_direction.append(ab_list['playEvents'][n]['pitchData']['breaks']['spinDirection'] if 'spinDirection' in ab_list['playEvents'][n]['pitchData']['breaks'] else None)
                                    vb.append(ab_list['playEvents'][n]['pitchData']['breaks']['breakVertical'] if 'breakVertical' in ab_list['playEvents'][n]['pitchData']['breaks'] else None)                               
                                    ivb.append(ab_list['playEvents'][n]['pitchData']['breaks']['breakVerticalInduced'] if 'breakVerticalInduced' in ab_list['playEvents'][n]['pitchData']['breaks'] else None)
                                    hb.append(ab_list['playEvents'][n]['pitchData']['breaks']['breakHorizontal'] if 'breakHorizontal' in ab_list['playEvents'][n]['pitchData']['breaks'] else None)
    
                            else:
                                start_speed.append(None)
                                end_speed.append(None)
    
                                sz_top.append(None)
                                sz_bot.append(None)
                                x.append(None)
                                y.append(None)
    
                                ax.append(None)
                                ay.append(None)
                                az.append(None)
                                pfxx.append(None)
                                pfxz.append(None)
                                px.append(None)
                                pz.append(None)
                                vx0.append(None)
                                vy0.append(None)
                                vz0.append(None)
                                x0.append(None)
                                y0.append(None)
                                z0.append(None)
    
                                zone.append(None)
                                type_confidence.append(None)
                                plate_time.append(None)
                                extension.append(None)
                                spin_rate.append(None)
                                spin_direction.append(None)
                                vb.append(None)
                                ivb.append(None)
                                hb.append(None)
    
                            if 'hitData' in ab_list['playEvents'][n]:
                                launch_speed.append(ab_list['playEvents'][n]['hitData']['launchSpeed'] if 'launchSpeed' in ab_list['playEvents'][n]['hitData'] else None)
                                launch_angle.append(ab_list['playEvents'][n]['hitData']['launchAngle'] if 'launchAngle' in ab_list['playEvents'][n]['hitData'] else None)
                                launch_distance.append(ab_list['playEvents'][n]['hitData']['totalDistance'] if 'totalDistance' in ab_list['playEvents'][n]['hitData'] else None)
                                launch_location.append(ab_list['playEvents'][n]['hitData']['location'] if 'location' in ab_list['playEvents'][n]['hitData'] else None)
    
                                trajectory.append(ab_list['playEvents'][n]['hitData']['trajectory'] if 'trajectory' in ab_list['playEvents'][n]['hitData'] else None)
                                hardness.append(ab_list['playEvents'][n]['hitData']['hardness'] if 'hardness' in ab_list['playEvents'][n]['hitData'] else None)
                                hit_x.append(ab_list['playEvents'][n]['hitData']['coordinates']['coordX'] if 'coordX' in ab_list['playEvents'][n]['hitData']['coordinates'] else None)
                                hit_y.append(ab_list['playEvents'][n]['hitData']['coordinates']['coordY'] if 'coordY' in ab_list['playEvents'][n]['hitData']['coordinates'] else None)
                            else:
                                launch_speed.append(None)
                                launch_angle.append(None)
                                launch_distance.append(None)
                                launch_location.append(None)
                                trajectory.append(None)
                                hardness.append(None)
                                hit_x.append(None)
                                hit_y.append(None)
    
                            index_play.append(ab_list['playEvents'][n]['index'] if 'index' in ab_list['playEvents'][n] else None)
                            play_id.append(ab_list['playEvents'][n]['playId'] if 'playId' in ab_list['playEvents'][n] else None)
                            start_time.append(ab_list['playEvents'][n]['startTime'] if 'startTime' in ab_list['playEvents'][n] else None)
                            end_time.append(ab_list['playEvents'][n]['endTime'] if 'endTime' in ab_list['playEvents'][n] else None)
                            is_pitch.append(ab_list['playEvents'][n]['isPitch'] if 'isPitch' in ab_list['playEvents'][n] else None)
                            type_type.append(ab_list['playEvents'][n]['type'] if 'type' in ab_list['playEvents'][n] else None)
    
    
    
                            if n == len(ab_list['playEvents']) - 1 :
    
                                type_ab.append(data['liveData']['plays']['allPlays'][ab_id]['result']['type'] if 'type' in data['liveData']['plays']['allPlays'][ab_id]['result'] else None)
                                event.append(data['liveData']['plays']['allPlays'][ab_id]['result']['event'] if 'event' in data['liveData']['plays']['allPlays'][ab_id]['result'] else None)
                                event_type.append(data['liveData']['plays']['allPlays'][ab_id]['result']['eventType'] if 'eventType' in data['liveData']['plays']['allPlays'][ab_id]['result'] else None)
                                rbi.append(data['liveData']['plays']['allPlays'][ab_id]['result']['rbi'] if 'rbi' in data['liveData']['plays']['allPlays'][ab_id]['result'] else None)
                                away_score.append(data['liveData']['plays']['allPlays'][ab_id]['result']['awayScore'] if 'awayScore' in data['liveData']['plays']['allPlays'][ab_id]['result'] else None)
                                home_score.append(data['liveData']['plays']['allPlays'][ab_id]['result']['homeScore'] if 'homeScore' in data['liveData']['plays']['allPlays'][ab_id]['result'] else None)
                                is_out.append(data['liveData']['plays']['allPlays'][ab_id]['result']['isOut'] if 'isOut' in data['liveData']['plays']['allPlays'][ab_id]['result'] else None)
    
                            else:
    
                                type_ab.append(None)
                                event.append(None)
                                event_type.append(None)
                                rbi.append(None)
                                away_score.append(None)
                                home_score.append(None)
                                is_out.append(None)
    
                        elif ab_list['playEvents'][n]['count']['balls'] == 4:
    
                            event.append(data['liveData']['plays']['allPlays'][ab_id]['result']['event'])
                            event_type.append(data['liveData']['plays']['allPlays'][ab_id]['result']['eventType'])
    
    
                            game_id.append(data['gamePk'])
                            game_date.append(data['gameData']['datetime']['officialDate'])
                            batter_id.append(ab_list['matchup']['batter']['id'] if 'batter' in ab_list['matchup'] else None)
                            batter_name.append(ab_list['matchup']['batter']['fullName'] if 'batter' in ab_list['matchup'] else None)
                            batter_hand.append(ab_list['matchup']['batSide']['code'] if 'batSide' in ab_list['matchup'] else None)
                            pitcher_id.append(ab_list['matchup']['pitcher']['id'] if 'pitcher' in ab_list['matchup'] else None)
                            pitcher_name.append(ab_list['matchup']['pitcher']['fullName'] if 'pitcher' in ab_list['matchup'] else None)
                            pitcher_hand.append(ab_list['matchup']['pitchHand']['code'] if 'pitchHand' in ab_list['matchup'] else None)
                            if ab_list['about']['isTopInning']:
                                batter_team.append(data['gameData']['teams']['away']['abbreviation'] if 'away' in data['gameData']['teams'] else None)
                                batter_team_id.append(data['gameData']['teams']['away']['id'] if 'away' in data['gameData']['teams'] else None)
                                pitcher_team.append(data['gameData']['teams']['home']['abbreviation'] if 'home' in data['gameData']['teams'] else None)
                                pitcher_team_id.append(data['gameData']['teams']['away']['id'] if 'away' in data['gameData']['teams'] else None)
                            else:
                                batter_team.append(data['gameData']['teams']['home']['abbreviation'] if 'home' in data['gameData']['teams'] else None)
                                batter_team_id.append(data['gameData']['teams']['home']['id'] if 'home' in data['gameData']['teams'] else None)
                                pitcher_team.append(data['gameData']['teams']['away']['abbreviation'] if 'away' in data['gameData']['teams'] else None)
                                pitcher_team_id.append(data['gameData']['teams']['home']['id'] if 'home' in data['gameData']['teams'] else None)
    
                            play_description.append(None)
                            play_code.append(None)
                            in_play.append(None)
                            is_strike.append(None)
                            is_ball.append(None)
                            is_review.append(None)
                            pitch_type.append(None)
                            pitch_description.append(None)
                            strikes.append(ab_list['playEvents'][n]['count']['balls'] if 'balls' in ab_list['playEvents'][n]['count'] else None)
                            balls.append(ab_list['playEvents'][n]['count']['strikes'] if 'strikes' in ab_list['playEvents'][n]['count'] else None)
                            outs.append(ab_list['playEvents'][n]['count']['outs'] if 'outs' in ab_list['playEvents'][n]['count'] else None)
                            strikes_after.append(ab_list['playEvents'][n]['count']['balls'] if 'balls' in ab_list['playEvents'][n]['count'] else None)
                            balls_after.append(ab_list['playEvents'][n]['count']['strikes'] if 'strikes' in ab_list['playEvents'][n]['count'] else None)
                            outs_after.append(ab_list['playEvents'][n]['count']['outs'] if 'outs' in ab_list['playEvents'][n]['count'] else None)
                            index_play.append(ab_list['playEvents'][n]['index'] if 'index' in ab_list['playEvents'][n] else None)
                            play_id.append(ab_list['playEvents'][n]['playId'] if 'playId' in ab_list['playEvents'][n] else None)
                            start_time.append(ab_list['playEvents'][n]['startTime'] if 'startTime' in ab_list['playEvents'][n] else None)
                            end_time.append(ab_list['playEvents'][n]['endTime'] if 'endTime' in ab_list['playEvents'][n] else None)
                            is_pitch.append(ab_list['playEvents'][n]['isPitch'] if 'isPitch' in ab_list['playEvents'][n] else None)
                            type_type.append(ab_list['playEvents'][n]['type'] if 'type' in ab_list['playEvents'][n] else None)
    
    
    
                            is_swing.append(None)
                            is_whiff.append(None)
                            start_speed.append(None)
                            end_speed.append(None)
                            sz_top.append(None)
                            sz_bot.append(None)
                            x.append(None)
                            y.append(None)
                            ax.append(None)
                            ay.append(None)
                            az.append(None)
                            pfxx.append(None)
                            pfxz.append(None)
                            px.append(None)
                            pz.append(None)
                            vx0.append(None)
                            vy0.append(None)
                            vz0.append(None)
                            x0.append(None)
                            y0.append(None)
                            z0.append(None)
                            zone.append(None)
                            type_confidence.append(None)
                            plate_time.append(None)
                            extension.append(None)
                            spin_rate.append(None)
                            spin_direction.append(None)
                            vb.append(None)
                            ivb.append(None)
                            hb.append(None)
                            launch_speed.append(None)
                            launch_angle.append(None)
                            launch_distance.append(None)
                            launch_location.append(None)
                            trajectory.append(None)
                            hardness.append(None)
                            hit_x.append(None)
                            hit_y.append(None)
                            type_ab.append(None)
                            ab_number.append(None)
    
                            rbi.append(None)
                            away_score.append(None)
                            home_score.append(None)
                            is_out.append(None)

            except KeyError:
                print(f"No Data for Game")
        
        df  = pl.DataFrame(data={
            'game_id':game_id,
            'game_date':game_date,
            'batter_id':batter_id,
            'batter_name':batter_name,
            'batter_hand':batter_hand,
            'batter_team':batter_team,
            'batter_team_id':batter_team_id,
            'pitcher_id':pitcher_id,
            'pitcher_name':pitcher_name,
            'pitcher_hand':pitcher_hand,
            'pitcher_team':pitcher_team,
            'pitcher_team_id':pitcher_team_id,
            'ab_number':ab_number,
            'play_description':play_description,
            'play_code':play_code,
            'in_play':in_play,
            'is_strike':is_strike,
            'is_swing':is_swing,
            'is_whiff':is_whiff,
            'is_out':is_out,
            'is_ball':is_ball,
            'is_review':is_review,
            'pitch_type':pitch_type,
            'pitch_description':pitch_description,
            'strikes':strikes,
            'balls':balls,
            'outs':outs,
            'strikes_after':strikes_after,
            'balls_after':balls_after,
            'outs_after':outs_after,            
            'start_speed':start_speed,
            'end_speed':end_speed,
            'sz_top':sz_top,
            'sz_bot':sz_bot,
            'x':x,
            'y':y,
            'ax':ax,
            'ay':ay,
            'az':az,
            'pfxx':pfxx,
            'pfxz':pfxz,
            'px':px,
            'pz':pz,
            'vx0':vx0,
            'vy0':vy0,
            'vz0':vz0,
            'x0':x0,
            'y0':y0,
            'z0':z0,
            'zone':zone,
            'type_confidence':type_confidence,
            'plate_time':plate_time,
            'extension':extension,
            'spin_rate':spin_rate,
            'spin_direction':spin_direction,
            'vb':vb,
            'ivb':ivb,
            'hb':hb,
            'launch_speed':launch_speed,
            'launch_angle':launch_angle,
            'launch_distance':launch_distance,
            'launch_location':launch_location,
            'trajectory':trajectory,
            'hardness':hardness,
            'hit_x':hit_x,
            'hit_y':hit_y,
            'index_play':index_play,
            'play_id':play_id,
            'start_time':start_time,
            'end_time':end_time,
            'is_pitch':is_pitch,
            'type_type':type_type,
            'type_ab':type_ab,
            'event':event,
            'event_type':event_type,
            'rbi':rbi,
            'away_score':away_score,
            'home_score':home_score,

            },strict=False
            )

        return df

    def get_teams(self):
        """
        Retrieves information about MLB teams from the MLB API and processes it into a Polars DataFrame.
        
        Returns:
        - mlb_teams_df (pl.DataFrame): A DataFrame containing team information, including team ID, city, name, franchise, abbreviation, parent organization ID, parent organization name, league ID, and league name.
        """
        # Make API call to retrieve team information
        teams = requests.get(url='https://statsapi.mlb.com/api/v1/teams/').json()

        # Extract relevant data from the API response
        mlb_teams_city = [x['franchiseName'] if 'franchiseName' in x else None for x in teams['teams']]
        mlb_teams_name = [x['teamName'] if 'franchiseName' in x else None for x in teams['teams']]
        mlb_teams_franchise = [x['name'] if 'franchiseName' in x else None for x in teams['teams']]
        mlb_teams_id = [x['id'] if 'franchiseName' in x else None for x in teams['teams']]
        mlb_teams_abb = [x['abbreviation'] if 'franchiseName' in x else None for x in teams['teams']]
        mlb_teams_parent_id = [x['parentOrgId'] if 'parentOrgId' in x else None for x in teams['teams']]
        mlb_teams_parent = [x['parentOrgName'] if 'parentOrgName' in x else None for x in teams['teams']]
        mlb_teams_league_id = [x['league']['id'] if 'id' in x['league'] else None for x in teams['teams']]
        mlb_teams_league_name = [x['league']['name'] if 'name' in x['league'] else None for x in teams['teams']]

        # Create a Polars DataFrame with the extracted data
        mlb_teams_df = pl.DataFrame(data={'team_id': mlb_teams_id,
                                        'city': mlb_teams_franchise,
                                        'name': mlb_teams_name,
                                        'franchise': mlb_teams_franchise,
                                        'abbreviation': mlb_teams_abb,
                                        'parent_org_id': mlb_teams_parent_id,
                                        'parent_org': mlb_teams_parent,
                                        'league_id': mlb_teams_league_id,
                                        'league_name': mlb_teams_league_name
                                        }).unique().drop_nulls(subset=['team_id']).sort('team_id')

        # Fill missing parent organization IDs with team IDs
        mlb_teams_df = mlb_teams_df.with_columns(
            pl.when(pl.col('parent_org_id').is_null())
            .then(pl.col('team_id'))
            .otherwise(pl.col('parent_org_id'))
            .alias('parent_org_id')
        )

        # Fill missing parent organization names with franchise names
        mlb_teams_df = mlb_teams_df.with_columns(
            pl.when(pl.col('parent_org').is_null())
            .then(pl.col('franchise'))
            .otherwise(pl.col('parent_org'))
            .alias('parent_org')
        )

        # Create a dictionary for mapping team IDs to abbreviations
        abbreviation_dict = mlb_teams_df.select(['team_id', 'abbreviation']).to_dict(as_series=False)
        abbreviation_map = {k: v for k, v in zip(abbreviation_dict['team_id'], abbreviation_dict['abbreviation'])}

        # Create a DataFrame for parent organization abbreviations
        abbreviation_df = mlb_teams_df.select(['team_id', 'abbreviation']).rename({'team_id': 'parent_org_id', 'abbreviation': 'parent_org_abbreviation'})

        # Join the parent organization abbreviations with the main DataFrame
        mlb_teams_df = mlb_teams_df.join(abbreviation_df, on='parent_org_id', how='left')

        return mlb_teams_df

    def get_leagues(self):
        """
        Retrieves information about MLB leagues from the MLB API and processes it into a Polars DataFrame.
        
        Returns:
        - leagues_df (pl.DataFrame): A DataFrame containing league information, including league ID, league name, league abbreviation, and sport ID.
        """
        # Make API call to retrieve league information
        leagues = requests.get(url='https://statsapi.mlb.com/api/v1/leagues/').json()

        # Extract relevant data from the API response
        sport_id = [x['sport']['id'] if 'sport' in x else None for x in leagues['leagues']]
        league_id = [x['id'] if 'id' in x else None for x in leagues['leagues']]
        league_name = [x['name'] if 'name' in x else None for x in leagues['leagues']]
        league_abbreviation = [x['abbreviation'] if 'abbreviation' in x else None for x in leagues['leagues']]

        # Create a Polars DataFrame with the extracted data
        leagues_df = pl.DataFrame(data={
            'league_id': league_id,
            'league_name': league_name,
            'league_abbreviation': league_abbreviation,
            'sport_id': sport_id,
        })

        return leagues_df

    def get_player_games_list(self, player_id: int, 
                              season: int, 
                              start_date: str = None, 
                              end_date: str = None, 
                              sport_id: int = 1, 
                              game_type: list = ['R'],
                              pitching: bool = True):
        """
        Retrieves a list of game IDs for a specific player in a given season.
        
        Parameters:
        - player_id (int): The ID of the player.
        - season (int): The season year for which to retrieve the game list.
        - start_date (str): The start date (YYYY-MM-DD) of the range (default is January 1st of the specified season).
        - end_date (str): The end date (YYYY-MM-DD) of the range (default is December 31st of the specified season).
        - sport_id (int): The ID of the sport for which to retrieve player data.
        - game_type (list): A list of game types to filter the schedule. Default is ['R'].
        - pitching (bool): Return pitching games.
        
        Returns:
        - player_game_list (list): A list of game IDs in which the player participated during the specified season.
        """
        # Set default start and end dates if not provided
        if not start_date:
            start_date = f'{season}-01-01'
        if not end_date:
            end_date = f'{season}-12-31'

        # Determine the group based on the pitching flag
        group = 'pitching' if pitching else 'hitting'

        # Validate date format
        date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
        if not date_pattern.match(start_date):
            raise ValueError(f"start_date {start_date} is not in YYYY-MM-DD format")
        if not date_pattern.match(end_date):
            raise ValueError(f"end_date {end_date} is not in YYYY-MM-DD format")

        # Convert game type list to a comma-separated string
        game_type_str = ','.join([str(x) for x in game_type])

        # Make API call to retrieve player game logs
        response = requests.get(url=f'http://statsapi.mlb.com/api/v1/people/{player_id}?hydrate=stats(group={group},type=gameLog,season={season},startDate={start_date},endDate={end_date},sportId={sport_id},gameType=[{game_type_str}]),hydrations').json()
        
        # Check if stats are available in the response
        if 'stats' not in response['people'][0]:
            print(f'No {group} games found for player {player_id} in season {season}')
            return []

        # Extract game IDs from the API response
        player_game_list = [x['game']['gamePk'] for x in response['people'][0]['stats'][0]['splits']]
        
        return player_game_list
        
    def get_players(self, sport_id: int, season: int, game_type: list = ['R']):
        """
        Retrieves data frame of players in a given league

        Parameters:
        - sport_id (int): The ID of the sport for which to retrieve player data.
        - season (int): The season year for which to retrieve player data.
        - game_type (list): A list of game types to filter the players. Default is ['R'].

        Returns:
        - player_df (pl.DataFrame): A DataFrame containing player information, including player ID, name, position, team, and age.
        """
        game_type_str = ','.join([str(x) for x in game_type])

        # If game type is 'S', fetch data from a different endpoint
        if game_type_str == 'S':
            # Fetch pitcher data
            pitcher_data = requests.get(f'https://bdfed.stitch.mlbinfra.com/bdfed/stats/player?&env=prod&season={season}&sportId=1&stats=season&group=pitching&gameType=S&limit=1000000&offset=0&sortStat=inningsPitched&order=asc').json()
            fullName_list = [x['playerFullName'] for x in pitcher_data['stats']]
            firstName_list = [x['playerFirstName'] for x in pitcher_data['stats']]
            lastName_list = [x['playerLastName'] for x in pitcher_data['stats']]
            id_list = [x['playerId'] for x in pitcher_data['stats']]
            position_list = [x['primaryPositionAbbrev'] for x in pitcher_data['stats']]
            team_list = [x['teamId'] for x in pitcher_data['stats']]
            
            df_pitcher = pl.DataFrame(data={
                'player_id': id_list,
                'first_name': firstName_list,
                'last_name': lastName_list,
                'name': fullName_list,
                'position': position_list,
                'team': team_list
            })
            
            # Fetch batter data
            batter_data = requests.get(f'https://bdfed.stitch.mlbinfra.com/bdfed/stats/player?&env=prod&season={season}&sportId=1&stats=season&group=hitting&gameType=S&limit=1000000&offset=0').json()
            fullName_list = [x['playerFullName'] for x in batter_data['stats']]
            firstName_list = [x['playerFirstName'] for x in batter_data['stats']]
            lastName_list = [x['playerLastName'] for x in batter_data['stats']]
            id_list = [x['playerId'] for x in batter_data['stats']]
            position_list = [x['primaryPositionAbbrev'] for x in batter_data['stats']]
            team_list = [x['teamId'] for x in batter_data['stats']]
            
            df_batter = pl.DataFrame(data={
                'player_id': id_list,
                'first_name': firstName_list,
                'last_name': lastName_list,
                'name': fullName_list,
                'position': position_list,
                'team': team_list
            })

            # Combine pitcher and batter data
            df = pl.concat([df_pitcher, df_batter]).unique().drop_nulls(subset=['player_id']).sort('player_id')
        
        else:
            # Fetch player data for other game types
            player_data = requests.get(url=f'https://statsapi.mlb.com/api/v1/sports/{sport_id}/players?season={season}&gameType=[{game_type_str}]').json()['people']

            # Extract relevant data
            fullName_list = [x['fullName'] for x in player_data]
            firstName_list = [x['firstName'] for x in player_data]
            lastName_list = [x['lastName'] for x in player_data]
            id_list = [x['id'] for x in player_data]
            position_list = [x['primaryPosition']['abbreviation'] if 'primaryPosition' in x else None for x in player_data]
            team_list = [x['currentTeam']['id'] if 'currentTeam' in x else None for x in player_data]
            weight_list = [x['weight'] if 'weight' in x else None for x in player_data]
            height_list = [x['height'] if 'height' in x else None for x in player_data]
            age_list = [x['currentAge'] if 'currentAge' in x else None for x in player_data]
            birthDate_list = [x['birthDate'] if 'birthDate' in x else None for x in player_data]
    
            df = pl.DataFrame(data={
                'player_id': id_list,
                'first_name': firstName_list,
                'last_name': lastName_list,
                'name': fullName_list,
                'position': position_list,
                'team': team_list,
                'weight': weight_list,
                'height': height_list,
                'age': age_list,
                'birthDate': birthDate_list
            })
                
        return df
