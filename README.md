# Movie Data Pipeline - ETL Project

## Overview

This project implements a complete ETL (Extract, Transform, Load) pipeline for movie data, combining local CSV files from the MovieLens dataset with enriched metadata from the OMDb API. The pipeline extracts movie and rating information, transforms and cleans the data, and loads it into a SQLite relational database for analytical queries.

### Key Features

- **Multi-source data integration**: Combines MovieLens CSV data with OMDb API enrichment
- **Robust title matching**: Advanced fuzzy matching algorithm with multiple fallback strategies
- **Idempotent operations**: Safe to run multiple times without creating duplicates
- **Comprehensive data model**: Normalized schema with proper relationships and indexes
- **Analytical queries**: Pre-built SQL queries for common movie analytics

---

## Project Structure

```
movie-data-pipeline/
├── etl.py              # Main ETL script
├── schema.sql          # Database schema definition
├── queries.sql         # Analytical SQL queries
├── requirements.txt    # Python dependencies
├── README.md           # This file
├── movies.csv          # MovieLens movies data (not included)
├── ratings.csv         # MovieLens ratings data (not included)
└── movies.db           # SQLite database (generated)
```

---

## Setup Instructions

### Prerequisites

- Python 3.8 or higher
- OMDb API key (free at http://www.omdbapi.com/)

### Step 1: Clone the Repository

```bash
git clone <your-repo-url>
cd movie-data-pipeline
```

### Step 2: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 3: Download MovieLens Dataset

1. Download the MovieLens "small" dataset from: https://grouplens.org/datasets/movielens/latest/
2. Extract the ZIP file
3. Copy `movies.csv` and `ratings.csv` to the project root directory

### Step 4: Configure OMDb API Key

1. Get a free API key from http://www.omdbapi.com/
2. Open `etl.py` and replace the `OMDB_API_KEY` value:
   ```python
   OMDB_API_KEY = "your_api_key_here"
   ```

### Step 5: Run the Pipeline

```bash
python etl.py
```

The script will:
- Create the database schema
- Extract and enrich movie data (with OMDb API calls)
- Transform genres into normalized format
- Load all data into SQLite database
- Execute analytical queries and display results

**Expected Runtime**: 5-10 minutes (depending on dataset size and API rate limits)

---

## Database Schema

### Entity-Relationship Design

The database uses a normalized schema with the following tables:

#### `movies`
- **movie_id** (PK): Unique identifier from MovieLens
- **title**: Cleaned movie title
- **release_year**: Extracted from title
- **imdb_id**: IMDB identifier from OMDb
- **plot**: Movie plot/description
- **director**: Director name(s)
- **box_office_dollars**: Box office earnings in dollars
- **runtime_mins**: Movie duration in mins
- **imdb_rating**: IMDB rating score
- **created_at**: Record creation timestamp

#### `genres`
- **genre_id** (PK, Auto-increment): Unique genre identifier
- **genre_name**: Genre name (e.g., "Action", "Comedy")

#### `movie_genres` (Junction Table)
- **movie_id** (FK → movies)
- **genre_id** (FK → genres)
- Composite primary key on (movie_id, genre_id)

#### `ratings`
- **rating_id** (PK, Auto-increment): Unique rating identifier
- **movie_id** (FK → movies): Movie being rated
- **user_id**: User who provided the rating
- **rating**: Rating value (0-5)
- **timestamp**: Unix timestamp of rating

### Indexes

Performance indexes are created on:
- `ratings.movie_id` and `ratings.user_id`
- `movie_genres.movie_id` and `movie_genres.genre_id`
- `movies.release_year` and `movies.director`

---

## Design Choices & Assumptions

### 1. Title Matching Strategy

**Challenge**: MovieLens titles often don't match OMDb exactly due to:
- Year suffixes: "Toy Story (1995)"
- Article positioning: "Godfather, The"
- Alternate titles in parentheses
- Diacritics and special characters

**Solution**: Implemented a multi-candidate fuzzy matching system:
1. Remove year suffixes and quotes
2. Move trailing articles (", The" → "The ")
3. Remove parenthetical alternate titles
4. Strip diacritics/accents
5. Try each candidate with and without year parameter
6. Implement retry logic for API failures

**Result**: Significantly higher match rate (~90%+) compared to naive title matching

### 2. API Rate Limiting

- Added 0.5-second delay between requests
- Implemented retry logic (2 attempts per request)
- Graceful fallback to default values when API fails
- Total runtime: ~5-10 minutes for full dataset

### 3. Idempotency

The ETL script is idempotent:
- `INSERT OR REPLACE` for movies (based on movie_id)
- `INSERT OR IGNORE` for genres and movie_genres
- Ratings only appended if table is empty

Running the script multiple times won't create duplicates.

### 4. Data Cleaning

- **Missing values**: Defaulted to "Unknown" (director), "Not Available" (plot), or NULL (numeric fields)
- **Data types**: Converted ratings to REAL, years to INTEGER
- **Genre parsing**: Split pipe-delimited genres, filtered out "(no genres listed)"
- **Title normalization**: Applied full cleaning pipeline before storage

### 5. Database Choice

**SQLite** was chosen because:
- Zero configuration required
- Embedded database (no server needed)
- Perfect for development and small-to-medium datasets
- Easy to share and version control
- SQL-compliant for analytical queries

---

## Challenges & Solutions

### Challenge 1: OMDb Title Matching Failures

**Issue**: Initial naive matching (direct title lookup) had ~40% failure rate

**Solution**: 
- Researched common title format differences
- Built incremental title normalization pipeline
- Generated multiple candidate titles per movie
- Tried both with/without year parameter
- Result: Improved match rate to 90%+

### Challenge 2: API Rate Limits

**Issue**: Free OMDb tier has rate limits; bulk requests could fail

**Solution**:
- Implemented exponential backoff with retries
- Added configurable delays between requests
- Graceful degradation (continue with default values on failure)
- Progress logging to track enrichment status

### Challenge 3: Genre Data Normalization

**Issue**: Genres stored as pipe-delimited string ("Action|Adventure|Sci-Fi")

**Solution**:
- Created normalized `genres` table with unique genre names
- Designed `movie_genres` junction table for many-to-many relationship
- Enables efficient genre-based queries without string parsing

### Challenge 4: Data Quality Issues

**Issue**: Inconsistent data (missing values, malformed dates, encoding issues)

**Solution**:
- Comprehensive NULL handling with sensible defaults
- Type validation and conversion
- Unicode normalization (diacritics, special characters)
- Data validation constraints in schema (e.g., rating CHECK constraint)

---

## Analytical Queries

The `queries.sql` file contains four analytical queries:

1. **Highest Rated Movie**: Finds the single movie with the highest average rating (minimum 1 rating)

2. **Top 5 Genres by Rating**: Calculates average rating per genre, ordered by rating then total number of ratings

3. **Most Prolific Director**: Identifies the director with the most movies in the dataset (from OMDb data)

4. **Average Rating by Year**: Shows how average movie ratings have evolved over time

All queries include proper JOINs, aggregations, and sorting for meaningful insights.

---

## Production Improvements

If scaling this pipeline for production use, I would implement:

### Infrastructure
- **Cloud database**: Migrate to PostgreSQL or MySQL for better concurrency and scalability
- **Containerization**: Dockerize the application for consistent deployment
- **Orchestration**: Use Apache Airflow or Prefect for workflow management and scheduling
- **Cloud storage**: Store CSV files in S3/GCS instead of local filesystem

### Data Quality
- **Data validation**: Implement schema validation (e.g., Great Expectations, Pandera)
- **Monitoring**: Add data quality metrics and alerting
- **Logging**: Structured logging with correlation IDs
- **Error handling**: Dead letter queue for failed API calls to retry later

### Performance
- **Parallel processing**: Use multiprocessing/threading for API calls
- **Batch API requests**: Group requests if API supports batch endpoints
- **Caching**: Cache API responses (Redis/Memcached) to avoid redundant calls
- **Incremental loading**: Only process new/changed records instead of full refresh

### Code Quality
- **Unit tests**: Test individual functions (especially title matching logic)
- **Integration tests**: Test end-to-end pipeline with sample data
- **Configuration management**: Externalize config (environment variables, config files)
- **Type hints**: Add comprehensive type annotations for better IDE support

### Security
- **Secrets management**: Store API keys in AWS Secrets Manager / HashiCorp Vault
- **Access control**: Implement database user roles and permissions
- **Audit logging**: Track all data modifications with user attribution

### Observability
- **Metrics**: Track API success rates, processing times, record counts
- **Dashboards**: Grafana/Datadog dashboards for pipeline health
- **Alerting**: PagerDuty integration for failures

---

## Running the Queries Manually

To run queries separately after ETL completion:

```bash
# Using sqlite3 CLI
sqlite3 movies.db < queries.sql

# Or interactive mode
sqlite3 movies.db
sqlite> .read queries.sql
```



## Troubleshooting

### Common Issues

**Issue**: `ModuleNotFoundError: No module named 'pandas'`
- **Solution**: Run `pip install -r requirements.txt`

**Issue**: `OMDb API returns 401 Unauthorized`
- **Solution**: Verify your API key is correct in `etl.py`

**Issue**: `FileNotFoundError: 'movies.csv'`
- **Solution**: Ensure MovieLens CSV files are in the project root directory

**Issue**: Script runs slowly
- **Solution**: This is expected due to API rate limiting (0.5s delay per movie). Adjust `REQUEST_DELAY` if needed, but be mindful of API limits.

**Issue**: Database locked error
- **Solution**: Close any other programs accessing `movies.db` (DB Browser for SQLite, etc.)

---



## Acknowledgments

- **MovieLens Dataset**: GroupLens Research (University of Minnesota)
- **OMDb API**: Open Movie Database
