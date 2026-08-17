"""
50 hardcoded popular IMDb titles used as the content master for testing.
Replaces the real IMDb title.basics download for fast local validation.

Schema matches the IMDb title.basics + title.ratings join described in
skills/aws/analytics-team/streaming/01_source_imdb_dataset.md.
"""

MOCK_TITLES = [
    {"tconst": "tt0111161", "primary_title": "The Shawshank Redemption", "title_type": "movie", "genres": "Drama", "runtime_minutes": 142, "imdb_rating": 9.3, "num_votes": 2900000},
    {"tconst": "tt0468569", "primary_title": "The Dark Knight", "title_type": "movie", "genres": "Action,Crime,Drama", "runtime_minutes": 152, "imdb_rating": 9.0, "num_votes": 2900000},
    {"tconst": "tt0944947", "primary_title": "Game of Thrones", "title_type": "tvSeries", "genres": "Action,Adventure,Drama", "runtime_minutes": 57, "imdb_rating": 9.2, "num_votes": 2400000},
    {"tconst": "tt0903747", "primary_title": "Breaking Bad", "title_type": "tvSeries", "genres": "Crime,Drama,Thriller", "runtime_minutes": 49, "imdb_rating": 9.5, "num_votes": 2200000},
    {"tconst": "tt4574334", "primary_title": "Stranger Things", "title_type": "tvSeries", "genres": "Drama,Fantasy,Horror", "runtime_minutes": 51, "imdb_rating": 8.7, "num_votes": 1500000},
    {"tconst": "tt2861424", "primary_title": "Rick and Morty", "title_type": "tvSeries", "genres": "Animation,Adventure,Comedy", "runtime_minutes": 23, "imdb_rating": 9.1, "num_votes": 600000},
    {"tconst": "tt0098904", "primary_title": "Seinfeld", "title_type": "tvSeries", "genres": "Comedy", "runtime_minutes": 22, "imdb_rating": 8.9, "num_votes": 350000},
    {"tconst": "tt0386676", "primary_title": "The Office", "title_type": "tvSeries", "genres": "Comedy", "runtime_minutes": 22, "imdb_rating": 9.0, "num_votes": 800000},
    {"tconst": "tt0108778", "primary_title": "Friends", "title_type": "tvSeries", "genres": "Comedy,Romance", "runtime_minutes": 22, "imdb_rating": 8.9, "num_votes": 1100000},
    {"tconst": "tt0773262", "primary_title": "Dexter", "title_type": "tvSeries", "genres": "Crime,Drama,Mystery", "runtime_minutes": 53, "imdb_rating": 8.6, "num_votes": 750000},
    {"tconst": "tt0167260", "primary_title": "The Lord of the Rings: The Return of the King", "title_type": "movie", "genres": "Action,Adventure,Drama", "runtime_minutes": 201, "imdb_rating": 9.0, "num_votes": 1900000},
    {"tconst": "tt0167261", "primary_title": "The Lord of the Rings: The Two Towers", "title_type": "movie", "genres": "Action,Adventure,Drama", "runtime_minutes": 179, "imdb_rating": 8.8, "num_votes": 1700000},
    {"tconst": "tt0120737", "primary_title": "The Lord of the Rings: The Fellowship of the Ring", "title_type": "movie", "genres": "Action,Adventure,Drama", "runtime_minutes": 178, "imdb_rating": 8.9, "num_votes": 1900000},
    {"tconst": "tt0816692", "primary_title": "Interstellar", "title_type": "movie", "genres": "Adventure,Drama,Sci-Fi", "runtime_minutes": 169, "imdb_rating": 8.7, "num_votes": 2000000},
    {"tconst": "tt1375666", "primary_title": "Inception", "title_type": "movie", "genres": "Action,Adventure,Sci-Fi", "runtime_minutes": 148, "imdb_rating": 8.8, "num_votes": 2500000},
    {"tconst": "tt0137523", "primary_title": "Fight Club", "title_type": "movie", "genres": "Drama", "runtime_minutes": 139, "imdb_rating": 8.8, "num_votes": 2200000},
    {"tconst": "tt0109830", "primary_title": "Forrest Gump", "title_type": "movie", "genres": "Drama,Romance", "runtime_minutes": 142, "imdb_rating": 8.8, "num_votes": 2200000},
    {"tconst": "tt0110912", "primary_title": "Pulp Fiction", "title_type": "movie", "genres": "Crime,Drama", "runtime_minutes": 154, "imdb_rating": 8.9, "num_votes": 2200000},
    {"tconst": "tt0068646", "primary_title": "The Godfather", "title_type": "movie", "genres": "Crime,Drama", "runtime_minutes": 175, "imdb_rating": 9.2, "num_votes": 2000000},
    {"tconst": "tt0050083", "primary_title": "12 Angry Men", "title_type": "movie", "genres": "Crime,Drama", "runtime_minutes": 96, "imdb_rating": 9.0, "num_votes": 850000},
    {"tconst": "tt0099685", "primary_title": "Goodfellas", "title_type": "movie", "genres": "Biography,Crime,Drama", "runtime_minutes": 145, "imdb_rating": 8.7, "num_votes": 1200000},
    {"tconst": "tt0102926", "primary_title": "The Silence of the Lambs", "title_type": "movie", "genres": "Crime,Drama,Thriller", "runtime_minutes": 118, "imdb_rating": 8.6, "num_votes": 1500000},
    {"tconst": "tt0080684", "primary_title": "Star Wars: Episode V - The Empire Strikes Back", "title_type": "movie", "genres": "Action,Adventure,Fantasy", "runtime_minutes": 124, "imdb_rating": 8.7, "num_votes": 1400000},
    {"tconst": "tt0076759", "primary_title": "Star Wars", "title_type": "movie", "genres": "Action,Adventure,Fantasy", "runtime_minutes": 121, "imdb_rating": 8.6, "num_votes": 1500000},
    {"tconst": "tt0073486", "primary_title": "One Flew Over the Cuckoo's Nest", "title_type": "movie", "genres": "Drama", "runtime_minutes": 133, "imdb_rating": 8.7, "num_votes": 1100000},
    {"tconst": "tt0114369", "primary_title": "Se7en", "title_type": "movie", "genres": "Crime,Drama,Mystery", "runtime_minutes": 127, "imdb_rating": 8.6, "num_votes": 1700000},
    {"tconst": "tt0317248", "primary_title": "City of God", "title_type": "movie", "genres": "Crime,Drama", "runtime_minutes": 130, "imdb_rating": 8.6, "num_votes": 800000},
    {"tconst": "tt0114814", "primary_title": "The Usual Suspects", "title_type": "movie", "genres": "Crime,Drama,Mystery", "runtime_minutes": 106, "imdb_rating": 8.5, "num_votes": 1100000},
    {"tconst": "tt0118799", "primary_title": "Life Is Beautiful", "title_type": "movie", "genres": "Comedy,Drama,Romance", "runtime_minutes": 116, "imdb_rating": 8.6, "num_votes": 730000},
    {"tconst": "tt0245429", "primary_title": "Spirited Away", "title_type": "movie", "genres": "Animation,Adventure,Family", "runtime_minutes": 125, "imdb_rating": 8.6, "num_votes": 800000},
    {"tconst": "tt0848228", "primary_title": "The Avengers", "title_type": "movie", "genres": "Action,Adventure,Sci-Fi", "runtime_minutes": 143, "imdb_rating": 8.0, "num_votes": 1500000},
    {"tconst": "tt4154796", "primary_title": "Avengers: Endgame", "title_type": "movie", "genres": "Action,Adventure,Drama", "runtime_minutes": 181, "imdb_rating": 8.4, "num_votes": 1200000},
    {"tconst": "tt6710474", "primary_title": "Everything Everywhere All at Once", "title_type": "movie", "genres": "Action,Adventure,Comedy", "runtime_minutes": 139, "imdb_rating": 7.8, "num_votes": 600000},
    {"tconst": "tt9362722", "primary_title": "Spider-Man: Across the Spider-Verse", "title_type": "movie", "genres": "Animation,Action,Adventure", "runtime_minutes": 140, "imdb_rating": 8.6, "num_votes": 450000},
    {"tconst": "tt2380307", "primary_title": "Coco", "title_type": "movie", "genres": "Animation,Adventure,Comedy", "runtime_minutes": 105, "imdb_rating": 8.4, "num_votes": 600000},
    {"tconst": "tt2096673", "primary_title": "Inside Out", "title_type": "movie", "genres": "Animation,Adventure,Comedy", "runtime_minutes": 95, "imdb_rating": 8.1, "num_votes": 800000},
    {"tconst": "tt0382932", "primary_title": "Ratatouille", "title_type": "movie", "genres": "Animation,Adventure,Comedy", "runtime_minutes": 111, "imdb_rating": 8.1, "num_votes": 800000},
    {"tconst": "tt6644200", "primary_title": "A Quiet Place", "title_type": "movie", "genres": "Drama,Horror,Sci-Fi", "runtime_minutes": 90, "imdb_rating": 7.5, "num_votes": 600000},
    {"tconst": "tt7286456", "primary_title": "Joker", "title_type": "movie", "genres": "Crime,Drama,Thriller", "runtime_minutes": 122, "imdb_rating": 8.4, "num_votes": 1500000},
    {"tconst": "tt1853728", "primary_title": "Django Unchained", "title_type": "movie", "genres": "Drama,Western", "runtime_minutes": 165, "imdb_rating": 8.4, "num_votes": 1700000},
    {"tconst": "tt0407887", "primary_title": "The Departed", "title_type": "movie", "genres": "Crime,Drama,Thriller", "runtime_minutes": 151, "imdb_rating": 8.5, "num_votes": 1400000},
    {"tconst": "tt7740496", "primary_title": "The Crown", "title_type": "tvSeries", "genres": "Biography,Drama,History", "runtime_minutes": 58, "imdb_rating": 8.6, "num_votes": 250000},
    {"tconst": "tt5180504", "primary_title": "The Witcher", "title_type": "tvSeries", "genres": "Action,Adventure,Drama", "runtime_minutes": 60, "imdb_rating": 8.0, "num_votes": 530000},
    {"tconst": "tt7366338", "primary_title": "Chernobyl", "title_type": "tvSeries", "genres": "Drama,History,Thriller", "runtime_minutes": 73, "imdb_rating": 9.4, "num_votes": 870000},
    {"tconst": "tt8111088", "primary_title": "The Mandalorian", "title_type": "tvSeries", "genres": "Action,Adventure,Fantasy", "runtime_minutes": 40, "imdb_rating": 8.7, "num_votes": 540000},
    {"tconst": "tt5052448", "primary_title": "Black Mirror", "title_type": "tvSeries", "genres": "Drama,Sci-Fi,Thriller", "runtime_minutes": 60, "imdb_rating": 8.7, "num_votes": 670000},
    {"tconst": "tt2707408", "primary_title": "Narcos", "title_type": "tvSeries", "genres": "Biography,Crime,Drama", "runtime_minutes": 49, "imdb_rating": 8.8, "num_votes": 470000},
    {"tconst": "tt2356777", "primary_title": "True Detective", "title_type": "tvSeries", "genres": "Crime,Drama,Mystery", "runtime_minutes": 55, "imdb_rating": 8.9, "num_votes": 670000},
    {"tconst": "tt0397306", "primary_title": "American Dad!", "title_type": "tvSeries", "genres": "Animation,Comedy", "runtime_minutes": 22, "imdb_rating": 7.4, "num_votes": 80000},
    {"tconst": "tt0182576", "primary_title": "Family Guy", "title_type": "tvSeries", "genres": "Animation,Comedy", "runtime_minutes": 22, "imdb_rating": 8.2, "num_votes": 380000},
]


def get_all_genres():
    """Extract sorted unique genre list from MOCK_TITLES."""
    genres = set()
    for title in MOCK_TITLES:
        for g in title["genres"].split(","):
            genres.add(g.strip())
    return sorted(genres)


if __name__ == "__main__":
    print(f"Mock titles: {len(MOCK_TITLES)}")
    movies = sum(1 for t in MOCK_TITLES if t["title_type"] == "movie")
    series = sum(1 for t in MOCK_TITLES if t["title_type"] == "tvSeries")
    print(f"  movies: {movies}")
    print(f"  series: {series}")
    print(f"Genres ({len(get_all_genres())}): {', '.join(get_all_genres())}")
