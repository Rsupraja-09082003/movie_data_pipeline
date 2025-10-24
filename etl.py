"""
Movie Data Pipeline ETL Script (robust title matching)
Extracts data from CSV and OMDb API, transforms it, and loads into SQLite database

Improvements:
- Cleaner function organization and logging
- Better inline comments
- Nested loops simplified slightly
- Minor readability enhancements
"""

import pandas as pd
import requests
import sqlite3
import time
import re
import unicodedata
from datetime import datetime

# ---------------------------
# Configuration
# ---------------------------
OMDB_API_KEY = "your_api_key"  # Replace with your OMDb API key
OMDB_BASE_URL = "https://www.omdbapi.com/"
DB_NAME = "movies.db"
CSV_MOVIES = "movies.csv"
CSV_RATINGS = "ratings.csv"

REQUEST_DELAY = 0.5  # seconds between OMDb API calls
API_RETRIES = 2      # number of retries per request

# ---------------------------
# Database setup
# ---------------------------
def create_database():
    """Create database tables from schema.sql"""
    print("Creating database schema...")
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    with open("schema.sql", "r", encoding="utf-8") as f:
        cur.executescript(f.read())
    conn.commit()
    conn.close()
    print("✓ Database schema created")

# ---------------------------
# Utilities
# ---------------------------
def remove_diacritics(text: str) -> str:
    
    if not isinstance(text, str):
        return text
    nfkd = unicodedata.normalize("NFKD", text) #Remove diacritics/accents, e.g., 'Cité' -> 'Cite'
    return "".join(c for c in nfkd if not unicodedata.combining(c))

def tidy_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip() #Normalize spaces and strip leading/trailing whitespace

# ---------------------------
# Title cleaning & candidate generation
# ---------------------------
def base_clean(title: str) -> str:
    t = re.sub(r"\s*\(\d{4}\)\s*$", "", title).strip() #Remove trailing year and outer quotes
    t = t.strip(' "\'')
    return tidy_whitespace(t)

def move_trailing_article(t: str) -> str:
    m = re.match(r'^(.*),\s*(The|A|An)$', t, flags=re.IGNORECASE) #"""Convert 'Name, The' -> 'The Name'"""
    return f"{m.group(2)} {m.group(1)}".strip() if m else t

def remove_parenthetical_alternates(t: str) -> str:
    """Remove parenthetical alternates, e.g., '(Original title ...)'"""
    t2 = re.sub(
        r'\s*\(.*?(a\.k\.a\.|aka|original|original title|la|le|der|el|cite|cité|versión|version).*?\)\s*',
        ' ', t, flags=re.IGNORECASE
    )
    t2 = re.sub(r'\s*\([^)]*\)\s*', ' ', t2)
    return tidy_whitespace(t2)

def extract_parenthetical_alternate(t: str):
    m = re.search(r'\(([^)]+)\)', t)
    if not m:
        return None
    inside = m.group(1).strip() #Return short alternate title from parentheses if present
    if len(inside) <= 30 and ',' not in inside:
        return inside
    return None

def generate_title_candidates(original_title: str):
    candidates = []
    base = base_clean(original_title) #Generate prioritized list of title candidates for OMDb query
    if not base:
        return candidates

    # Candidate variations
    moved = move_trailing_article(base)
    removed_paren = remove_parenthetical_alternates(base)
    alt = extract_parenthetical_alternate(base)

    for t in [moved, removed_paren, alt, base]:
        if t and t not in candidates:
            candidates.append(t)

    # Add diacritic-removed variants
    for c in list(candidates):
        translit = remove_diacritics(c)
        if translit != c and translit not in candidates:
            candidates.append(translit)

    # Deduplicate final list
    return [tidy_whitespace(c) for c in candidates if c]


# OMDb fetch

def fetch_omdb_data(title_candidates, year=None):
    """Fetch movie data from OMDb using multiple candidates and retries"""
    default = {
        'imdb_id': None, 'plot': 'Not Available', 'director': 'Unknown',
        'box_office': None, 'runtime': None, 'imdb_rating': None
    }

    for candidate in title_candidates:
        # Try with year and without year
        params_list = [{'t': candidate, 'y': int(year)}] if year else []
        params_list.append({'t': candidate})

        for params_extra in params_list:
            params = {'apikey': OMDB_API_KEY, 'type': 'movie'}
            params.update(params_extra)
            for attempt in range(API_RETRIES):
                try:
                    r = requests.get(OMDB_BASE_URL, params=params, timeout=10)
                    r.raise_for_status()
                    data = r.json()
                    if data.get('Response') == 'True':
                        print(f"  ✓ OMDb match: '{candidate}' (year param: {params_extra.get('y','none')})")
                        return {
                            'imdb_id': data.get('imdbID'),
                            'plot': data.get('Plot') or 'Not Available',
                            'director': data.get('Director') or 'Unknown',
                            'box_office': data.get('BoxOffice'),
                            'runtime': data.get('Runtime'),
                            'imdb_rating': float(data.get('imdbRating')) if data.get('imdbRating') not in (None,'N/A') else None
                        }
                    break  # break on unsuccessful response to try next candidate
                except requests.RequestException as e:
                    print(f"    ✗ Network/API error (attempt {attempt+1}) for '{candidate}': {e}")
                    time.sleep(1)
            time.sleep(0.1)  # small delay
    print("  ⚠ No OMDb match for candidates:", title_candidates[:3], "...")
    return default

# Extraction
def extract_movies():
    """Extract movies from CSV and enrich with OMDb data"""
    print("\n" + "="*50)
    print("EXTRACTING MOVIES DATA")
    print("="*50)

    df = pd.read_csv(CSV_MOVIES)
    df['release_year'] = df['title'].apply(lambda t: int(re.search(r'\((\d{4})\)', t).group(1)) if re.search(r'\((\d{4})\)', t) else None)

    for idx, row in df.iterrows():
        original = row['title']
        candidates = generate_title_candidates(original)
        print(f"\nProcessing movieId={row['movieId']} original='{original}'")
        omdb_info = fetch_omdb_data(candidates, row['release_year'])
        for key, value in omdb_info.items():
            df.at[idx, key] = value
        time.sleep(REQUEST_DELAY)

    print(f"\n✓ Finished enriching {len(df)} movies")
    return df

def extract_ratings():
    """Extract ratings CSV"""
    print("\n" + "="*50)
    print("EXTRACTING RATINGS")
    print("="*50)
    df = pd.read_csv(CSV_RATINGS)
    print(f"✓ Loaded {len(df)} ratings")
    return df

# Transform
def transform_genres(movies_df):
    """Extract movie-genre mapping from MovieLens 'genres' column"""
    print("\n" + "="*50)
    print("TRANSFORMING GENRES")
    print("="*50)
    records = []
    for _, r in movies_df.iterrows():
        if pd.notna(r.get('genres')):
            for g in str(r['genres']).split('|'):
                g = g.strip()
                if g and g != '(no genres listed)':
                    records.append({'movie_id': int(r['movieId']), 'genre_name': g})
    genres_df = pd.DataFrame(records)
    print(f"✓ Extracted {len(genres_df)} movie-genre rows ({genres_df['genre_name'].nunique()} unique genres)")
    return genres_df

# Load
def load_data(movies_df, ratings_df, genres_df):
    """Load movies, genres, movie_genres, and ratings into SQLite"""
    print("\n" + "="*50)
    print("LOADING DATA INTO DB")
    print("="*50)
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    try:
        # Movies table
        movies_to_load = movies_df[['movieId','title','release_year','imdb_id','plot','director','box_office_dollars','runtime_mins','imdb_rating']].copy()
        movies_to_load.columns = ['movie_id','title','release_year','imdb_id','plot','director','box_office_dollars','runtime_mins','imdb_rating']

        def full_title_clean(title):
            t = base_clean(title)
            t = move_trailing_article(t)
            t = remove_parenthetical_alternates(t)
            t = remove_diacritics(t)
            return tidy_whitespace(t)

        movies_to_load['title'] = movies_to_load['title'].apply(full_title_clean)
        # Clean up box_office and runtime fields
        def clean_box_office(value):
            if isinstance(value, str):
                value = value.replace("$", "").replace(",", "").strip()
            return value or None

        def clean_runtime(value):
            if isinstance(value, str):
                value = value.replace("min", "").replace("mins", "").strip()
            return value or None

        movies_to_load['box_office_dollars'] = movies_to_load['box_office_dollars'].apply(clean_box_office)
        movies_to_load['runtime_mins'] = movies_to_load['runtime_mins'].apply(clean_runtime)

        for _, r in movies_to_load.iterrows():
            cur.execute("""
                INSERT OR REPLACE INTO movies
                (movie_id, title, release_year, imdb_id, director, plot, box_office_dollars,runtime_mins,imdb_rating)
                VALUES (?, ?, ?, ?, ?, ?, ?,?,?)
            """, (int(r['movie_id']), r['title'], int(r['release_year']) if pd.notna(r['release_year']) else None,
                  r['imdb_id'], r['director'], r['plot'], r['box_office_dollars'],r['runtime_mins'],r['imdb_rating']))
        print(f"✓ Loaded {len(movies_to_load)} movies")

        # Genres table
        for g in genres_df['genre_name'].unique() if not genres_df.empty else []:
            cur.execute("INSERT OR IGNORE INTO genres (genre_name) VALUES (?)", (g,))
        print(f"✓ Loaded {genres_df['genre_name'].nunique() if not genres_df.empty else 0} unique genres")

        # Movie-genres mapping
        for _, r in genres_df.iterrows():
            cur.execute("SELECT genre_id FROM genres WHERE genre_name = ?", (r['genre_name'],))
            gid_row = cur.fetchone()
            if gid_row:
                cur.execute("INSERT OR IGNORE INTO movie_genres (movie_id, genre_id) VALUES (?, ?)",
                            (int(r['movie_id']), gid_row[0]))
        print(f"✓ Loaded {len(genres_df)} movie-genre relationships")

        # Ratings table
        cur.execute("SELECT COUNT(*) FROM ratings")
        if cur.fetchone()[0] == 0:
            ratings_df.rename(columns={'userId':'user_id','movieId':'movie_id'}, inplace=True)
            ratings_df.to_sql('ratings', conn, if_exists='append', index=False)
            print(f"✓ Loaded {len(ratings_df)} ratings")
        else:
            print("⚠ Ratings already present — skipping append")

        conn.commit()
    except Exception as e:
        conn.rollback()
        print("✗ Error loading data:", e)
        raise
    finally:
        conn.close()

# Stats & Queries
def print_stats():
    """Print table row counts and enriched movie count"""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    for t in ['movies','genres','movie_genres','ratings']:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            cnt = cur.fetchone()[0]
        except Exception:
            cnt = 0
        print(f"{t.upper()}: {cnt}")
    cur.execute("SELECT COUNT(*) FROM movies WHERE imdb_id IS NOT NULL")
    enriched = cur.fetchone()[0]
    print(f"Movies enriched with OMDb data: {enriched}")
    conn.close()

def run_queries(db_name, sql_file):
    """Execute all queries from queries.sql and print results"""
    print("\n" + "="*50)
    print("RUNNING QUERIES FROM queries.sql")
    print("="*50)
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    with open(sql_file, "r", encoding="utf-8") as f:
        sql_script = f.read()
    statements = [s.strip() for s in sql_script.split(';') if s.strip()]

    for i, stmt in enumerate(statements, 1):
        print(f"\n=== Query {i} ===")
        try:
            cur.execute(stmt)
            if cur.description:
                print("\t".join([desc[0] for desc in cur.description]))
                for row in cur.fetchall():
                    print("\t".join(str(x) if x is not None else "NULL" for x in row))
            else:
                conn.commit()
                print("✓ Query executed (no result set)")
        except Exception as e:
            print(f"✗ Error executing query {i}: {e}")

    conn.close()
    print("\n✓ All queries executed successfully")

# Main
def main():
    print("="*70)
    print("MOVIE DATA PIPELINE ETL (robust matching)")
    print("="*70)
    start = datetime.now()
    try:
        create_database()
        movies = extract_movies()
        ratings = extract_ratings()
        genres = transform_genres(movies)
        load_data(movies, ratings, genres)
        print_stats()
        run_queries(DB_NAME, "queries.sql")
        print("Completed in", datetime.now() - start)
    except Exception as e:
        print("ETL failed:", e)
        raise

if __name__ == "__main__":
    main()
