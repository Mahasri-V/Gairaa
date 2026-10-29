
-- ============================================================
-- MOVIE STREAMING PROJECT
-- TASK 4: SCHEMA DESIGN VERIFICATION
-- Database: movie_streaming_db
-- All queries below are READ-ONLY
-- ============================================================


-- 1. VERIFY SIX BASE TABLES AND FOUR GOLD TABLES

SELECT
    table_schema,
    COUNT(*) AS tables_found
FROM information_schema.tables
WHERE table_type = 'BASE TABLE'
  AND (
      (table_schema = 'public'
       AND table_name IN (
           'users', 'movies', 'subscriptions',
           'payments', 'viewing_events', 'ratings'
       ))
      OR
      (table_schema = 'gold'
       AND table_name IN (
           'movie_analytics', 'subscription_analytics',
           'user_engagement', 'daily_viewing_trends'
       ))
  )
GROUP BY table_schema
ORDER BY table_schema;


-- 2. REVIEW ALL BASE TABLE COLUMNS AND DATA TYPES

SELECT
    table_name,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'public'
ORDER BY table_name, ordinal_position;


-- 3. VERIFY PRIMARY KEYS

SELECT
    tc.table_name,
    tc.constraint_name,
    kcu.column_name AS primary_key_column
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
   AND tc.table_schema = kcu.table_schema
   AND tc.table_name = kcu.table_name
WHERE tc.table_schema = 'public'
  AND tc.constraint_type = 'PRIMARY KEY'
ORDER BY tc.table_name;


-- 4. VERIFY FOREIGN KEYS

SELECT
    tc.table_name AS child_table,
    kcu.column_name AS foreign_key_column,
    ccu.table_name AS referenced_table,
    ccu.column_name AS referenced_column,
    tc.constraint_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
   AND tc.table_schema = kcu.table_schema
   AND tc.table_name = kcu.table_name
JOIN information_schema.constraint_column_usage ccu
    ON tc.constraint_name = ccu.constraint_name
   AND tc.table_schema = ccu.table_schema
WHERE tc.table_schema = 'public'
  AND tc.constraint_type = 'FOREIGN KEY'
ORDER BY tc.table_name;


-- 5. VERIFY SIX NUMERIC / DATE QUALITY RULES

SELECT
    'invalid_movie_release_year' AS check_name,
    COUNT(*) AS issue_count
FROM public.movies
WHERE release_year IS NOT NULL
  AND (release_year < 1888 OR release_year > 2026)

UNION ALL

SELECT
    'invalid_movie_duration',
    COUNT(*)
FROM public.movies
WHERE duration_minutes IS NOT NULL
  AND duration_minutes <= 0

UNION ALL

SELECT
    'negative_payment_amount',
    COUNT(*)
FROM public.payments
WHERE amount IS NOT NULL
  AND amount < 0

UNION ALL

SELECT
    'invalid_rating',
    COUNT(*)
FROM public.ratings
WHERE rating IS NOT NULL
  AND (rating < 1 OR rating > 5)

UNION ALL

SELECT
    'negative_viewing_duration',
    COUNT(*)
FROM public.viewing_events
WHERE watch_duration_minutes IS NOT NULL
  AND watch_duration_minutes < 0

UNION ALL

SELECT
    'negative_subscription_price',
    COUNT(*)
FROM public.subscriptions
WHERE monthly_price IS NOT NULL
  AND monthly_price < 0;


-- 6. VERIFY SIX RELATIONSHIP-INTEGRITY RULES

SELECT
    'subscriptions_without_user' AS check_name,
    COUNT(*) AS issue_count
FROM public.subscriptions s
LEFT JOIN public.users u
    ON s.user_id = u.user_id
WHERE s.user_id IS NULL OR u.user_id IS NULL

UNION ALL

SELECT
    'payments_without_subscription',
    COUNT(*)
FROM public.payments p
LEFT JOIN public.subscriptions s
    ON p.subscription_id = s.subscription_id
WHERE p.subscription_id IS NULL
   OR s.subscription_id IS NULL

UNION ALL

SELECT
    'viewing_events_without_user',
    COUNT(*)
FROM public.viewing_events v
LEFT JOIN public.users u
    ON v.user_id = u.user_id
WHERE v.user_id IS NULL OR u.user_id IS NULL

UNION ALL

SELECT
    'viewing_events_without_movie',
    COUNT(*)
FROM public.viewing_events v
LEFT JOIN public.movies m
    ON v.movie_id = m.movie_id
WHERE v.movie_id IS NULL OR m.movie_id IS NULL

UNION ALL

SELECT
    'ratings_without_user',
    COUNT(*)
FROM public.ratings r
LEFT JOIN public.users u
    ON r.user_id = u.user_id
WHERE r.user_id IS NULL OR u.user_id IS NULL

UNION ALL

SELECT
    'ratings_without_movie',
    COUNT(*)
FROM public.ratings r
LEFT JOIN public.movies m
    ON r.movie_id = m.movie_id
WHERE r.movie_id IS NULL OR m.movie_id IS NULL;


-- 7. VERIFY FOUR GOLD ANALYTICS TABLES

SELECT
    table_name,
    'EXISTS' AS verification_status
FROM information_schema.tables
WHERE table_schema = 'gold'
  AND table_type = 'BASE TABLE'
  AND table_name IN (
      'movie_analytics',
      'subscription_analytics',
      'user_engagement',
      'daily_viewing_trends'
  )
ORDER BY table_name;


-- ============================================================
-- END OF TASK 4 SCHEMA DESIGN VERIFICATION
-- ============================================================
