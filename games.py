import requests
from datetime import datetime, timedelta

def get_upcoming_games():
    # Get today's and tomorrow's date in 'YYYY-MM-DD' format
    today = datetime.now().strftime('%Y-%m-%d')
    tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')

    # Base URL for the games endpoint
    base_url = 'https://www.balldontlie.io/api/v1/games'

    # Initialize an empty list to store the team names
    upcoming_games = []

    # Pagination setup
    page = 0
    total_pages = 1

    # Fetching the games in the next 24 hours
    while page < total_pages:
        # API request with query parameters
        response = requests.get(base_url, params={
            'start_date': today,
            'end_date': tomorrow,
            'per_page': 100,
            'page': page
        })
        data = response.json()

        # Update total_pages from the response metadata
        total_pages = data['meta']['total_pages']

        # Extract team names and add to the list
        for game in data['data']:
            home_team = game['home_team']['full_name']
            visitor_team = game['visitor_team']['full_name']
            upcoming_games.append(f"{home_team} vs {visitor_team} @ {game['status']}")

        # Increment page number for next iteration
        page += 1

    return upcoming_games

# Get the upcoming games and print them
upcoming_games_list = get_upcoming_games()
for game in upcoming_games_list:
    print(game)
