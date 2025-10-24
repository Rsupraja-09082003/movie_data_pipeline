-- Analytical Queries for Movie Database

-- Query 1: Which movie has the highest average rating?
-- This query groups ratings by movie, calculates average, and finds the top-rated movie
SELECT 
    m.title,
    m.release_year,
    AVG(r.rating) as avg_rating,
    COUNT(r.rating) as rating_count
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.movie_id, m.title, m.release_year
ORDER BY avg_rating DESC, rating_count DESC
LIMIT 1;


-- Query 2: What are the top 5 movie genres that have the highest average rating?
-- This joins movies, genres, and ratings to calculate average rating per genre
SELECT 
    g.genre_name,
    ROUND(AVG(r.rating), 2) as avg_rating,
    COUNT(DISTINCT m.movie_id) as movie_count,
    COUNT(r.rating) as total_ratings
FROM genres g
JOIN movie_genres mg ON g.genre_id = mg.genre_id
JOIN movies m ON mg.movie_id = m.movie_id
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY g.genre_id, g.genre_name
ORDER BY avg_rating DESC, total_ratings DESC
LIMIT 5;


-- Query 3: Who is the director with the most movies in this dataset?
-- This counts movies per director from OMDb enriched data
SELECT 
    director,
    COUNT(*) as movie_count,
    GROUP_CONCAT(title, ', ') as movies
FROM movies
WHERE director IS NOT NULL 
  AND director != 'N/A'
GROUP BY director
ORDER BY movie_count DESC, director ASC
LIMIT 1;


-- Query 4: What is the average rating of movies released each year?
-- This groups by release year and calculates average rating per year
SELECT 
    m.release_year,
    ROUND(AVG(r.rating), 2) as avg_rating,
    COUNT(DISTINCT m.movie_id) as movie_count,
    COUNT(r.rating) as total_ratings
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
WHERE m.release_year IS NOT NULL
GROUP BY m.release_year
ORDER BY m.release_year DESC;


