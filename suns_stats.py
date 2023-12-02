import requests
import psycopg2
from config import db_config
import time

# Phoenix Suns Team ID (replace with the actual ID)
suns_team_id = 24

def get_game_ids():
    """Fetch game IDs for Phoenix Suns games from the database."""
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    cur.execute("SELECT id FROM games WHERE home_team_id = %s OR visitor_team_id = %s", (suns_team_id, suns_team_id))
    game_ids = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return game_ids

def get_player_stats(game_ids):
    """Fetch player stats for the given game IDs from the API."""
    stats = []
    base_url = 'https://www.balldontlie.io/api/v1/stats'
    for game_id in game_ids:
        response = requests.get(base_url, params={'game_ids[]': game_id})
        time.sleep(0.7)
        data = response.json()['data']
        stats.extend(data)
    return stats

def insert_player_stats_into_db(stats):
    """Insert player stats into the database."""
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()

    for stat in stats:
        print(stat)
        try:
            cur.execute(
                "INSERT INTO player_stats (player_id, game_id, ast, blk, dreb, fg3_pct, fg3a, fg3m, fg_pct, fga, fgm, ft_pct, fta, ftm, min, oreb, pf, pts, reb, stl, turnover) SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s WHERE NOT EXISTS (SELECT 1 FROM player_stats WHERE player_id = %s AND game_id = %s)",
                (stat['player']['id'], stat['game']['id'], stat['ast'], stat['blk'], stat['dreb'], stat['fg3_pct'], stat['fg3a'], stat['fg3m'], stat['fg_pct'], stat['fga'], stat['fgm'], stat['ft_pct'], stat['fta'], stat['ftm'], stat['min'], stat['oreb'], stat['pf'], stat['pts'], stat['reb'], stat['stl'], stat['turnover'], stat['player']['id'], stat['game']['id'])
            )
            print(f"Inserted stats for player {stat['player']['id']} in game {stat['game']['id']}")
        except psycopg2.Error as e:
            print(f"Database error: {e}")
            continue

    conn.commit()
    cur.close()
    conn.close()

def main():

    # Get game IDs from database
    game_ids = get_game_ids()

    # Get player stats for these games
    player_stats = get_player_stats(game_ids)

    # Insert player stats into the database
    insert_player_stats_into_db(player_stats)

if __name__ == "__main__":
    main()
