
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv

PROJECT_DIR = Path(__file__).resolve().parent.parent

load_dotenv(PROJECT_DIR / ".env")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": int(os.getenv("DB_PORT", "5432")),
}

EXPECTED_COUNTS = {
    "movie_analytics": 199,
    "subscription_analytics": 240,
    "user_engagement": 300,
    "daily_viewing_trends": 357,
}


GOLD_QUERIES = {

    "movie_analytics": """
        INSERT INTO gold.movie_analytics (
            movie_id,
            title,
            genre,
            release_year,
            total_views,
            unique_viewers,
            average_rating,
            total_watch_minutes
        )
        SELECT
            m.movie_id,
            m.title,
            m.genre,
            m.release_year,
            COALESCE(v.total_views, 0),
            COALESCE(v.unique_viewers, 0),
            r.average_rating,
            COALESCE(v.total_watch_minutes, 0)
        FROM public.movies AS m

        LEFT JOIN (
            SELECT
                movie_id,
                COUNT(*) AS total_views,
                COUNT(DISTINCT user_id) AS unique_viewers,
                SUM(watch_duration_minutes) AS total_watch_minutes
            FROM public.viewing_events
            GROUP BY movie_id
        ) AS v
            ON m.movie_id = v.movie_id

        LEFT JOIN (
            SELECT
                movie_id,
                ROUND(AVG(rating), 2) AS average_rating
            FROM public.ratings
            GROUP BY movie_id
        ) AS r
            ON m.movie_id = r.movie_id
    """,


    "subscription_analytics": """
        INSERT INTO gold.subscription_analytics (
            subscription_id,
            user_id,
            plan_name,
            start_date,
            end_date,
            monthly_price,
            total_payments,
            successful_payment_amount,
            successful_payments,
            unsuccessful_payments
        )
        SELECT
            s.subscription_id,
            s.user_id,
            s.plan_name,
            s.start_date,
            s.end_date,
            s.monthly_price,
            COALESCE(p.total_payments, 0),
            COALESCE(p.successful_payment_amount, 0),
            COALESCE(p.successful_payments, 0),
            COALESCE(p.unsuccessful_payments, 0)
        FROM public.subscriptions AS s

        LEFT JOIN (
            SELECT
                subscription_id,
                COUNT(*) AS total_payments,

                SUM(amount) FILTER (
                    WHERE payment_status = 'Success'
                ) AS successful_payment_amount,

                COUNT(*) FILTER (
                    WHERE payment_status = 'Success'
                ) AS successful_payments,

                COUNT(*) FILTER (
                    WHERE payment_status = 'Failed'
                ) AS unsuccessful_payments

            FROM public.payments
            GROUP BY subscription_id
        ) AS p
            ON s.subscription_id = p.subscription_id
    """,


    "user_engagement": """
        INSERT INTO gold.user_engagement (
            user_id,
            name,
            city,
            total_views,
            unique_movies_watched,
            total_watch_minutes,
            average_rating_given,
            total_ratings_given
        )
        SELECT
            u.user_id,
            u.name,
            u.city,
            COALESCE(v.total_views, 0),
            COALESCE(v.unique_movies_watched, 0),
            COALESCE(v.total_watch_minutes, 0),
            r.average_rating_given,
            COALESCE(r.total_ratings_given, 0)
        FROM public.users AS u

        LEFT JOIN (
            SELECT
                user_id,
                COUNT(*) AS total_views,
                COUNT(DISTINCT movie_id)
                    AS unique_movies_watched,
                SUM(watch_duration_minutes)
                    AS total_watch_minutes
            FROM public.viewing_events
            GROUP BY user_id
        ) AS v
            ON u.user_id = v.user_id

        LEFT JOIN (
            SELECT
                user_id,
                ROUND(AVG(rating), 2)
                    AS average_rating_given,
                COUNT(*) AS total_ratings_given
            FROM public.ratings
            GROUP BY user_id
        ) AS r
            ON u.user_id = r.user_id
    """,


    "daily_viewing_trends": """
        INSERT INTO gold.daily_viewing_trends (
            viewing_date,
            total_views,
            unique_viewers,
            unique_movies_watched,
            total_watch_minutes,
            average_watch_minutes
        )
        SELECT
            watch_date::date AS viewing_date,
            COUNT(*) AS total_views,
            COUNT(DISTINCT user_id) AS unique_viewers,
            COUNT(DISTINCT movie_id)
                AS unique_movies_watched,
            SUM(watch_duration_minutes)
                AS total_watch_minutes,
            ROUND(AVG(watch_duration_minutes), 2)
                AS average_watch_minutes
        FROM public.viewing_events
        GROUP BY watch_date::date
    """,
}



def validate_configuration():
    """Check that required database settings are available."""

    required_variables = [
        "DB_HOST",
        "DB_NAME",
        "DB_USER",
        "DB_PASSWORD",
    ]

    missing_variables = [
        variable
        for variable in required_variables
        if not os.getenv(variable)
    ]

    if missing_variables:
        raise RuntimeError(
            "Missing database settings in .env: "
            + ", ".join(missing_variables)
        )

    if DB_CONFIG["dbname"] != "movie_streaming_db":
        raise RuntimeError(
            "DB_NAME must be movie_streaming_db. "
            "Gold refresh cancelled to protect other databases."
        )


def validate_source_tables(cursor):
    """Confirm that the six source tables contain data."""

    cursor.execute("""
        SELECT
            (SELECT COUNT(*) FROM public.users),
            (SELECT COUNT(*) FROM public.movies),
            (SELECT COUNT(*) FROM public.subscriptions),
            (SELECT COUNT(*) FROM public.payments),
            (SELECT COUNT(*) FROM public.viewing_events),
            (SELECT COUNT(*) FROM public.ratings)
    """)

    counts = cursor.fetchone()

    source_tables = [
        "users",
        "movies",
        "subscriptions",
        "payments",
        "viewing_events",
        "ratings",
    ]

    print("\nSOURCE TABLE VALIDATION")
    print("-" * 50)

    for table_name, count in zip(source_tables, counts):
        print(f"{table_name}: {count} rows")

        if count == 0:
            raise RuntimeError(
                f"Source table {table_name} is empty. "
                "Gold refresh cancelled."
            )


def validate_gold_tables(cursor):
    """Ensure the four existing Gold tables are present."""

    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'gold'
          AND table_type = 'BASE TABLE'
          AND table_name IN (
              'movie_analytics',
              'subscription_analytics',
              'user_engagement',
              'daily_viewing_trends'
          )
    """)

    existing_tables = {
        row[0] for row in cursor.fetchall()
    }

    expected_tables = set(GOLD_QUERIES.keys())

    if existing_tables != expected_tables:
        missing = expected_tables - existing_tables

        raise RuntimeError(
            "Gold tables missing: "
            + ", ".join(sorted(missing))
        )



def refresh_gold_tables(cursor):
    """
    Delete old Gold rows and insert freshly calculated rows.

    This function runs inside one transaction. If an error occurs,
    the transaction is rolled back by the connection context.
    """

    print("\nREFRESHING GOLD TABLES")
    print("-" * 50)

    for table_name, insert_sql in GOLD_QUERIES.items():

        print(f"\nRefreshing gold.{table_name}...")

        cursor.execute(
            f"DELETE FROM gold.{table_name}"
        )

        cursor.execute(insert_sql)

        cursor.execute(
            f"SELECT COUNT(*) FROM gold.{table_name}"
        )

        actual_count = cursor.fetchone()[0]
        expected_count = EXPECTED_COUNTS[table_name]

        print(f"Expected rows: {expected_count}")
        print(f"Actual rows:   {actual_count}")

        if actual_count != expected_count:
            raise RuntimeError(
                f"Row-count validation failed for "
                f"gold.{table_name}. "
                f"Expected {expected_count}, got {actual_count}."
            )

        print(f"PASS: gold.{table_name}")


def main():

    print("MOVIE STREAMING GOLD ANALYTICS PIPELINE")


    validate_configuration()

    print("\nConnecting to PostgreSQL...")

    with psycopg.connect(**DB_CONFIG) as connection:

        with connection.cursor() as cursor:

            validate_source_tables(cursor)
            validate_gold_tables(cursor)

            refresh_gold_tables(cursor)

    print("SUCCESS: ALL GOLD TABLES REFRESHED")



if __name__ == "__main__":
    main()
