-- Transformation script for dim_dates

-- First, let's create a function to determine if a date is a holiday
-- This example includes multiple holidays; adjust based on your region and specific holiday requirements.
CREATE OR REPLACE FUNCTION is_holiday(date_to_check DATE) RETURNS BOOLEAN AS $$
BEGIN
    RETURN (
        -- New Year's Day
        (EXTRACT(MONTH FROM date_to_check) = 1 AND EXTRACT(DAY FROM date_to_check) = 1)
        -- Christmas Day
        OR (EXTRACT(MONTH FROM date_to_check) = 12 AND EXTRACT(DAY FROM date_to_check) = 25)
        -- Independence Day (July 4)
        OR (EXTRACT(MONTH FROM date_to_check) = 7 AND EXTRACT(DAY FROM date_to_check) = 4)
        -- Labor Day (first Monday of September)
        OR (EXTRACT(MONTH FROM date_to_check) = 9 AND EXTRACT(DOW FROM date_to_check) = 1 AND EXTRACT(DAY FROM date_to_check) <= 7)
        -- Thanksgiving (fourth Thursday of November)
        OR (EXTRACT(MONTH FROM date_to_check) = 11 AND EXTRACT(DOW FROM date_to_check) = 4 AND EXTRACT(DAY FROM date_to_check) >= 22 AND EXTRACT(DAY FROM date_to_check) <= 28)
        -- Memorial Day (last Monday of May)
        OR (EXTRACT(MONTH FROM date_to_check) = 5 AND EXTRACT(DOW FROM date_to_check) = 1 AND EXTRACT(DAY FROM date_to_check) > 24)
        -- Veterans Day (November 11)
        OR (EXTRACT(MONTH FROM date_to_check) = 11 AND EXTRACT(DAY FROM date_to_check) = 11)
        -- Martin Luther King Jr. Day (third Monday of January)
        OR (EXTRACT(MONTH FROM date_to_check) = 1 AND EXTRACT(DOW FROM date_to_check) = 1 AND EXTRACT(DAY FROM date_to_check) BETWEEN 15 AND 21)
        -- Presidents' Day (third Monday of February)
        OR (EXTRACT(MONTH FROM date_to_check) = 2 AND EXTRACT(DOW FROM date_to_check) = 1 AND EXTRACT(DAY FROM date_to_check) BETWEEN 15 AND 21)
        -- Easter Sunday (can vary; example set to April 9 here for illustration)
        OR (EXTRACT(MONTH FROM date_to_check) = 4 AND EXTRACT(DAY FROM date_to_check) = 9)
        -- Good Friday (two days before Easter Sunday, adjust according to actual Easter)
        OR (EXTRACT(MONTH FROM date_to_check) = 4 AND EXTRACT(DAY FROM date_to_check) = 7)
        -- Halloween (October 31)
        OR (EXTRACT(MONTH FROM date_to_check) = 10 AND EXTRACT(DAY FROM date_to_check) = 31)
        -- Columbus Day (second Monday of October)
        OR (EXTRACT(MONTH FROM date_to_check) = 10 AND EXTRACT(DOW FROM date_to_check) = 1 AND EXTRACT(DAY FROM date_to_check) BETWEEN 8 AND 14)
        -- New Year's Eve (December 31)
        OR (EXTRACT(MONTH FROM date_to_check) = 12 AND EXTRACT(DAY FROM date_to_check) = 31)
        -- St. Patrick's Day (March 17)
        OR (EXTRACT(MONTH FROM date_to_check) = 3 AND EXTRACT(DAY FROM date_to_check) = 17)
        -- Valentine's Day (February 14)
        OR (EXTRACT(MONTH FROM date_to_check) = 2 AND EXTRACT(DAY FROM date_to_check) = 14)
    );
END;
$$ LANGUAGE plpgsql;

-- Now, let's populate the dim_dates table
INSERT INTO final.dim_dates (
    date,
    year,
    month,
    day,
    quarter,
    is_weekend,
    is_holiday
)
SELECT
    d::date AS date,
    EXTRACT(YEAR FROM d) AS year,
    EXTRACT(MONTH FROM d) AS month,
    EXTRACT(DAY FROM d) AS day,
    EXTRACT(QUARTER FROM d) AS quarter,
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    is_holiday(d::date) AS is_holiday
FROM generate_series(
    (SELECT MIN(order_purchase_timestamp)::date FROM stg.orders),
    (SELECT MAX(order_estimated_delivery_date)::date FROM stg.orders),
    '1 day'::interval
) d
ON CONFLICT (date) DO NOTHING;
