
SELECT
    subscription_id,
    plan_name,
    monthly_price,
    total_payments,
    successful_payments,
    unsuccessful_payments,
    successful_payment_amount
FROM gold.subscription_analytics
ORDER BY successful_payment_amount DESC
LIMIT 10;





CREATE TABLE gold.user_engagement AS
WITH viewing_summary AS (
    SELECT
        user_id,
        COUNT(*) AS total_views,
        COUNT(DISTINCT movie_id) AS unique_movies_watched,
        COALESCE(SUM(watch_duration_minutes), 0)
            AS total_watch_minutes
    FROM viewing_events
    GROUP BY user_id
),
rating_summary AS (
    SELECT
        user_id,
        ROUND(AVG(rating), 2) AS average_rating_given,
        COUNT(*) AS total_ratings_given
    FROM ratings
    GROUP BY user_id
)
SELECT
    u.user_id,
    u.name,
    u.city,
    COALESCE(v.total_views, 0) AS total_views,
    COALESCE(v.unique_movies_watched, 0)
        AS unique_movies_watched,
    COALESCE(v.total_watch_minutes, 0)
        AS total_watch_minutes,
    COALESCE(r.average_rating_given, 0)
        AS average_rating_given,
    COALESCE(r.total_ratings_given, 0)
        AS total_ratings_given
FROM users u
LEFT JOIN viewing_summary v
    ON u.user_id = v.user_id
LEFT JOIN rating_summary r
    ON u.user_id = r.user_id;



SELECT *
FROM gold.user_engagement
ORDER BY total_watch_minutes DESC
LIMIT 10;



CREATE TABLE gold.daily_viewing_trends AS
SELECT
    CAST(watch_date AS DATE) AS viewing_date,
    COUNT(*) AS total_views,
    COUNT(DISTINCT user_id) AS unique_viewers,
    COUNT(DISTINCT movie_id) AS unique_movies_watched,
    COALESCE(SUM(watch_duration_minutes), 0)
        AS total_watch_minutes,
    ROUND(AVG(watch_duration_minutes), 2)
        AS average_watch_minutes
FROM viewing_events
GROUP BY CAST(watch_date AS DATE);



SELECT *
FROM gold.daily_viewing_trends
ORDER BY viewing_date;




SELECT
    viewing_date,
    total_views,
    unique_viewers,
    unique_movies_watched,
    total_watch_minutes,
    average_watch_minutes
FROM gold.daily_viewing_trends
ORDER BY total_views DESC
LIMIT 10;




SELECT
    movie_id,
    title,
    genre,
    total_views,
    unique_viewers,
    average_rating,
    total_watch_minutes
FROM gold.movie_analytics
ORDER BY total_views DESC
LIMIT 10;




SELECT
    genre,
    SUM(total_views) AS total_views,
    COUNT(*) AS total_movies,
    ROUND(AVG(average_rating), 2) AS average_rating
FROM gold.movie_analytics
GROUP BY genre
ORDER BY total_views DESC;




SELECT
    plan_name,
    COUNT(*) AS total_subscriptions,
    SUM(total_payments) AS total_payments,
    SUM(successful_payments) AS successful_payments,
    SUM(unsuccessful_payments) AS unsuccessful_payments,
    SUM(successful_payment_amount) AS successful_revenue
FROM gold.subscription_analytics
GROUP BY plan_name
ORDER BY successful_revenue DESC;





SELECT
    city,
    COUNT(*) AS total_users,
    SUM(total_views) AS total_views,
    SUM(total_watch_minutes) AS total_watch_minutes,
    ROUND(AVG(total_watch_minutes), 2)
        AS average_watch_minutes_per_user
FROM gold.user_engagement
GROUP BY city
ORDER BY total_watch_minutes DESC;




SELECT
    COUNT(*) AS total_payment_attempts,
    COUNT(*) FILTER (
        WHERE LOWER(payment_status) = 'success'
    ) AS successful_payments,
    COUNT(*) FILTER (
        WHERE LOWER(payment_status) <> 'success'
           OR payment_status IS NULL
    ) AS unsuccessful_payments,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE LOWER(payment_status) = 'success'
        ) / NULLIF(COUNT(*), 0),
        2
    ) AS payment_success_rate_percent
FROM payments;




SELECT
    m.movie_id,
    m.title,
    m.genre,
    COUNT(r.rating_id) AS total_ratings,
    ROUND(AVG(r.rating), 2) AS average_rating
FROM movies m
JOIN ratings r
    ON m.movie_id = r.movie_id
GROUP BY
    m.movie_id,
    m.title,
    m.genre
HAVING COUNT(r.rating_id) >= 2
ORDER BY average_rating DESC, total_ratings DESC
LIMIT 10;





SELECT
    user_id,
    name,
    city,
    total_views,
    unique_movies_watched,
    total_watch_minutes
FROM gold.user_engagement
ORDER BY
    unique_movies_watched DESC,
    total_watch_minutes DESC
LIMIT 10;




SELECT
    s.plan_name,
    COUNT(p.payment_id) AS total_payment_attempts,
    COUNT(p.payment_id) FILTER (
        WHERE LOWER(p.payment_status) = 'success'
    ) AS successful_payments,
    COUNT(p.payment_id) FILTER (
        WHERE LOWER(p.payment_status) <> 'success'
           OR p.payment_status IS NULL
    ) AS unsuccessful_payments,
    ROUND(
        100.0 * COUNT(p.payment_id) FILTER (
            WHERE LOWER(p.payment_status) <> 'success'
               OR p.payment_status IS NULL
        ) / NULLIF(COUNT(p.payment_id), 0),
        2
    ) AS failure_rate_percent
FROM subscriptions s
JOIN payments p
    ON s.subscription_id = p.subscription_id
GROUP BY s.plan_name
ORDER BY failure_rate_percent DESC;



SELECT
    DATE_TRUNC('month', watch_date)::DATE AS viewing_month,
    COUNT(*) AS total_views,
    COUNT(DISTINCT user_id) AS unique_viewers,
    COUNT(DISTINCT movie_id) AS unique_movies_watched,
    SUM(watch_duration_minutes) AS total_watch_minutes
FROM viewing_events
GROUP BY DATE_TRUNC('month', watch_date)
ORDER BY total_views DESC;





SELECT
    movie_id,
    title,
    genre,
    total_views,
    average_rating
FROM gold.movie_analytics
WHERE total_views >= 3
  AND average_rating > 0
  AND average_rating < 2.5
ORDER BY total_views DESC, average_rating ASC;




SELECT
    subscription_id,
    user_id,
    plan_name,
    start_date,
    end_date,
    monthly_price
FROM subscriptions
WHERE end_date < CURRENT_DATE
ORDER BY end_date DESC
LIMIT 20;



SELECT
    u.user_id,
    u.name,
    u.city,
    u.signup_date
FROM users u
LEFT JOIN viewing_events v
    ON u.user_id = v.user_id
WHERE v.viewing_id IS NULL
ORDER BY u.signup_date;



SELECT
    s.subscription_id,
    s.user_id,
    s.plan_name,
    s.monthly_price,
    COUNT(p.payment_id) AS total_payment_attempts,
    COUNT(p.payment_id) FILTER (
        WHERE LOWER(p.payment_status) = 'success'
    ) AS successful_payments
FROM subscriptions s
LEFT JOIN payments p
    ON s.subscription_id = p.subscription_id
GROUP BY
    s.subscription_id,
    s.user_id,
    s.plan_name,
    s.monthly_price
HAVING COUNT(p.payment_id) FILTER (
    WHERE LOWER(p.payment_status) = 'success'
) = 0
ORDER BY total_payment_attempts DESC;




SELECT
    m.movie_id,
    m.title,
    m.genre,
    m.release_year
FROM movies m
LEFT JOIN viewing_events v
    ON m.movie_id = v.movie_id
WHERE v.viewing_id IS NULL
ORDER BY m.release_year DESC, m.title;




SELECT
    plan_name,
    COUNT(*) AS total_subscriptions,
    SUM(monthly_price) AS expected_monthly_revenue,
    ROUND(AVG(monthly_price), 2) AS average_monthly_price,
    MIN(monthly_price) AS minimum_monthly_price,
    MAX(monthly_price) AS maximum_monthly_price
FROM subscriptions
GROUP BY plan_name
ORDER BY expected_monthly_revenue DESC;





SELECT
    table_name,
    column_name,
    data_type
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN (
      'users',
      'subscriptions',
      'viewing_events',
      'ratings'
  )
ORDER BY table_name, ordinal_position;




