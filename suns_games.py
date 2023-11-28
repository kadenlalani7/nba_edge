import requests
import psycopg2
import logging
from datetime import datetime
from config import db_config

# Configure logging
logging.basicConfig(level=logging.INFO)

def get_suns_games(start_year, current_year, team_id):
    """
    Fetches games for the Phoenix Suns for specified seasons.

    Parameters:
    start_year (int): The starting year of the season range.
    current_year (int): The ending year of the season range.
    team_id (int): The team ID for the Phoenix Suns.

    Returns:
    list: A list of game data dictionaries.
    """
    base_url = 'https://www.balldontlie.io/api/v1/games'
    games = []

    for season in range(start_year, current_year + 1):
        page = 1
        total_pages = 1

        while page <= total_pages:
            try:
                response = requests.get(base_url, params={
                    'seasons[]': season,
                    'team_ids[]': team_id,
                    'per_page': 100,
                    'page': page
                })
                response.raise_for_status()
            except requests.RequestException as e:
                logging.error(f"HTTP Request failed: {e}")
                break

            data = response.json()
            total_pages = data['meta']['total_pages']
            games.extend(data['data'])
            page += 1

    return games

def extract_game_info(games):
    """
    Extracts necessary information from each game.

    Parameters:
    games (list): List of game data dictionaries.

    Returns:
    list: A list of dictionaries containing extracted game information.
    """
    extracted_data = []

    for game in games:
        game_info = {
            'id': game['id'],
            'date': game['date'],
            'home_team_id': game['home_team']['id'],
            'visitor_team_id': game['visitor_team']['id'],
            'season': game['season'],
            'period': game['period'],
            'status': game['status'],
            'time': game['time'],
            'postseason': game['postseason'],
            'game_postseason': game.get('game_postseason', False)
        }
        extracted_data.append(game_info)

    return extracted_data

def insert_games_into_db(games):
    """
    Inserts game data into the PostgreSQL database.

    Parameters:
    games (list): List of dictionaries containing game information.
    """
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        for game in games:
            cur.execute(
                "INSERT INTO games (id, date, home_team_id, visitor_team_id, season, period, status, time, postseason, game_postseason) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (game['id'], game['date'], game['home_team_id'], game['visitor_team_id'], game['season'], game['period'], game['status'], game['time'], game['postseason'], game['game_postseason'])
            )

        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Database error: {error}")
    finally:
        if conn is not None:
            conn.close()

def main():
    # Phoenix Suns Team ID (replace with the actual ID)
    suns_team_id = 24
    current_year = datetime.now().year

    suns_games = get_suns_games(2016, current_year, suns_team_id)
    extracted_suns_games = extract_game_info(suns_games)
    insert_games_into_db(extracted_suns_games)

    logging.info("Data insertion complete.")

if __name__ == "__main__":
    main()
