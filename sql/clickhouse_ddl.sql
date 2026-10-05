-- ==========================================================
-- Flight & Airport Big Data Analytics Platform
-- ClickHouse DDL: All 8 Analytical Tables + Connectivity Test
-- Database: flight_analytics
-- ==========================================================

CREATE DATABASE IF NOT EXISTS flight_analytics;
USE flight_analytics;

-- Table 1: Airline Performance (Q1, Q6 rates, Q8 YoY)
CREATE TABLE IF NOT EXISTS agg_airline_performance (
    Year UInt16,
    Reporting_Airline String,
    Airline_Name String,
    Total_Flights UInt32,
    OnTime_Flights UInt32,
    Delayed_Flights UInt32,
    Delay_Rate_Pct Float32,
    Avg_Dep_Delay_Min Float32,
    Avg_Arr_Delay_Min Float32,
    Cancelled_Flights UInt32,
    Cancellation_Rate_Pct Float32
) ENGINE = MergeTree()
ORDER BY (Year, Reporting_Airline);

-- Table 2: Airport Performance (Q2, Q6 rates)
CREATE TABLE IF NOT EXISTS agg_airport_performance (
    Airport_Code String,
    Airport_City String,
    Airport_State String,
    Total_Departures UInt32,
    Total_Arrivals UInt32,
    Dep_Delay_Rate_Pct Float32,
    Arr_Delay_Rate_Pct Float32,
    Avg_Dep_Delay_Min Float32,
    Cancelled_Departures UInt32
) ENGINE = MergeTree()
ORDER BY Airport_Code;

-- Table 3: Monthly Delay Causes Breakdown (Q4)
CREATE TABLE IF NOT EXISTS agg_delay_causes_monthly (
    Year UInt16,
    Month UInt8,
    Total_Delay_Minutes UInt64,
    Carrier_Delay_Min UInt64,
    Weather_Delay_Min UInt64,
    NAS_Delay_Min UInt64,
    Security_Delay_Min UInt64,
    Late_Aircraft_Delay_Min UInt64,
    Carrier_Pct Float32,
    Weather_Pct Float32,
    NAS_Pct Float32,
    Security_Pct Float32,
    Late_Aircraft_Pct Float32
) ENGINE = MergeTree()
ORDER BY (Year, Month);

-- Table 4: Hourly Delay Probability & Duration (Q3)
CREATE TABLE IF NOT EXISTS agg_hourly_delays (
    Dep_Hour UInt8,
    Total_Flights UInt32,
    Delayed_Flights UInt32,
    Delay_Probability_Pct Float32,
    Avg_Delay_Minutes Float32
) ENGINE = MergeTree()
ORDER BY Dep_Hour;

-- Table 5: Route Traffic & Delay Analysis (Q7)
CREATE TABLE IF NOT EXISTS agg_route_traffic (
    Origin String,
    Dest String,
    Route_Name String,
    Flight_Count UInt32,
    Avg_AirTime_Min Float32,
    Distance_Miles UInt32,
    Route_Delay_Rate_Pct Float32
) ENGINE = MergeTree()
ORDER BY (Origin, Dest);

-- Table 6: Calendar Seasonality & Day of Week (Q5, Q8)
CREATE TABLE IF NOT EXISTS agg_calendar_delays (
    Year UInt16,
    Month UInt8,
    Day_Of_Week UInt8,
    Total_Flights UInt32,
    Delayed_Flights UInt32,
    Cancelled_Flights UInt32,
    Delay_Rate_Pct Float32,
    Avg_Arr_Delay_Min Float32
) ENGINE = MergeTree()
ORDER BY (Year, Month, Day_Of_Week);

-- Table 7: Cancellation Reasons Breakdown (Q6 reasons)
CREATE TABLE IF NOT EXISTS agg_cancellation_reasons (
    Year UInt16,
    Reporting_Airline String,
    Cancellation_Code String,
    Cancellation_Reason String,
    Cancelled_Flights UInt32
) ENGINE = MergeTree()
ORDER BY (Year, Reporting_Airline, Cancellation_Code);

-- Table 8: PySpark ML Delay Risk Predictions (Power BI Page 4)
CREATE TABLE IF NOT EXISTS ml_delay_predictions (
    FlightDate Date,
    Reporting_Airline String,
    Origin String,
    Dest String,
    Dep_Hour UInt8,
    Actual_Delayed UInt8,
    Predicted_Delayed UInt8,
    Delay_Probability Float32,
    Risk_Category String
) ENGINE = MergeTree()
ORDER BY (FlightDate, Reporting_Airline);

-- Connectivity Test Table (for testing Power BI connection)
CREATE TABLE IF NOT EXISTS connectivity_test (
    id UInt8,
    note String
) ENGINE = MergeTree()
ORDER BY id;

INSERT INTO connectivity_test VALUES (1, 'power bi test');
