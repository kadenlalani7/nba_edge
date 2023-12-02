import psycopg2
import pandas as pd
from config import db_config

def get_player_stats(season, team_id):
    """Fetch player stats from the database for a specific season and team."""
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT ps.player_id, p.first_name, p.last_name, ps.game_id, ps.ast, ps.blk, 
               ps.fg3a, ps.fg3m, ps.fga, ps.fgm,
               ps.fta, ps.ftm, ps.min, ps.pf, ps.pts, ps.reb, 
               ps.stl, ps.turnover, g.date
        FROM player_stats ps
        JOIN games g ON ps.game_id = g.id
        JOIN players p ON ps.player_id = p.id
        WHERE (g.home_team_id = %s OR g.visitor_team_id = %s) AND g.season = %s
        ORDER BY ps.player_id, g.date DESC
    """, (team_id, team_id, season))

    columns = [desc[0] for desc in cur.description]
    data = cur.fetchall()
    cur.close()
    conn.close()

    return pd.DataFrame(data, columns=columns)

def calculate_averages(df, recent_games_count):
    """Calculate season and recent performance averages."""
    df = df.drop(columns=['game_id', 'date'])
    df_filtered = df[df['min'] >= 8]
    season_avg = df.groupby(['player_id', 'first_name', 'last_name']).mean().reset_index()
    recent_avg = df_filtered.groupby('player_id').head(recent_games_count).groupby(['player_id', 'first_name', 'last_name']).mean().reset_index()
    return season_avg, recent_avg

def compare_performance(season_avg_df, recent_avg_df):
    """Compare recent performance to season averages."""
    comparison_df = pd.merge(season_avg_df, recent_avg_df, on=['player_id', 'first_name', 'last_name'], suffixes=('_season', '_recent'))
    return comparison_df

def main():
    season = 2023  # Example season
    team_id = 14   # LA Lakers Team ID

    player_stats = get_player_stats(season, team_id)
    season_avg, recent_avg = calculate_averages(player_stats, recent_games_count=4)
    performance_comparison = compare_performance(season_avg, recent_avg)

    # Saving the comparison to a CSV file
    performance_comparison.to_csv('lakers_performance_comparison.csv', index=False)
    print("Performance Comparison saved to 'lakers_performance_comparison.csv'.")

if __name__ == "__main__":
    main()
