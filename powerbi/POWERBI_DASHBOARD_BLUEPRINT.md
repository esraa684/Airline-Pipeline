# Power BI Dashboard Blueprint & DAX Specification
## Flight & Airport Big Data Analytics Platform
### 4-Page Executive Dashboard Specification

---

## 📌 Data Source Configuration
All data is imported directly from the pre-aggregated Big Data outputs in [`sample_output/`](../sample_output/):

| Table Name in Power BI | Source CSV File | Rows | Primary Key / Grain |
| :--- | :--- | :---: | :--- |
| **`AirlinePerformance`** | `sample_output/agg_airline_performance.csv` | 16 | `Year`, `Reporting_Airline` |
| **`AirportPerformance`** | `sample_output/agg_airport_performance.csv` | 335 | `Airport_Code` |
| **`DelayCauses`** | `sample_output/agg_delay_causes_monthly.csv` | 2 | `Year`, `Month` |
| **`HourlyDelays`** | `sample_output/agg_hourly_delays.csv` | 24 | `Dep_Hour` |
| **`RouteTraffic`** | `sample_output/agg_route_traffic.csv` | 5,667 | `Origin`, `Dest` |
| **`CalendarDelays`** | `sample_output/agg_calendar_delays.csv` | 8 | `Year`, `Month`, `Day_Of_Week` |
| **`CancellationReasons`**| `sample_output/agg_cancellation_reasons.csv` | 43 | `Year`, `Reporting_Airline`, `Cancellation_Code` |
| **`MLPredictions`** | `sample_output/ml_delay_predictions.csv` | 5,000 | Individual sampled test flights |

---

## 📐 Essential DAX Measures

```dax
// 1. Total Flights Analyzed
Total Flights = SUM(AirlinePerformance[Total_Flights])

// 2. Total Delayed Flights
Delayed Flights = SUM(AirlinePerformance[Delayed_Flights])

// 3. System-Wide Delay Rate (%)
System Delay Rate = 
DIVIDE(
    SUM(AirlinePerformance[Delayed_Flights]),
    SUM(AirlinePerformance[Delayed_Flights]) + SUM(AirlinePerformance[OnTime_Flights]),
    0
) * 100

// 4. On-Time Reliability Rate (%)
On-Time Reliability = 100 - [System Delay Rate]

// 5. Total Cancelled Flights
Total Cancelled = SUM(AirlinePerformance[Cancelled_Flights])

// 6. Cancellation Rate (%)
Cancellation Rate = 
DIVIDE(
    SUM(AirlinePerformance[Cancelled_Flights]),
    SUM(AirlinePerformance[Total_Flights]),
    0
) * 100

// 7. High-Risk Flight Count (ML Model)
High Risk Flights = 
CALCULATE(
    COUNTROWS(MLPredictions),
    MLPredictions[Risk_Category] = "High"
)
```

---

## 📊 Detailed 4-Page Layout & Visual Setup

---

### Page 1: Executive Operations Overview
* **Objective:** Give C-level aviation executives an immediate operational health check.
* **Header:** "U.S. Domestic Aviation Operations & On-Time Performance (2022–2025)"
* **KPI Cards (Top Row):**
  1. **Card 1:** `[Total Flights]` (Formatted in Millions/Thousands)
  2. **Card 2:** `[On-Time Reliability]` (Target: >75%, Green text)
  3. **Card 3:** `Average Arrival Delay` (Minutes per late flight: 24.8 min)
  4. **Card 4:** `[Cancellation Rate]` (Target: <3%)
* **Visual 1 (Left Half - Clustered Bar Chart):**
  - **Title:** "Airline Delay Rate Ranking (%)"
  - **Y-Axis:** `AirlinePerformance[Airline_Name]` (or `Reporting_Airline`)
  - **X-Axis:** `AirlinePerformance[Delay_Rate_Pct]`
  - **Sort:** Descending by Delay Rate (Highlights AA at 29.37% and B6 at 29.03%)
* **Visual 2 (Right Half - Bar Chart / Map):**
  - **Title:** "Top 10 Congested Airports by Departure Delay Rate"
  - **Y-Axis:** `AirportPerformance[Airport_City]`
  - **X-Axis:** `AirportPerformance[Dep_Delay_Rate_Pct]`
  - **Tooltip:** `Total_Departures`, `Avg_Dep_Delay_Min`
* **Slicers:**
  - `AirlinePerformance[Year]`
  - `AirlinePerformance[Reporting_Airline]`

---

### Page 2: Delay Root Causes & Seasonality
* **Objective:** Uncover *why* and *when* flights get delayed or cancelled.
* **Visual 1 (Top Left - Donut Chart):**
  - **Title:** "Delay Root Cause Share (% of Total Delay Minutes)"
  - **Values:** `Carrier_Delay_Min`, `Weather_Delay_Min`, `NAS_Delay_Min`, `Security_Delay_Min`, `Late_Aircraft_Delay_Min` from `DelayCauses`.
  - **Colors:**
    - Late Aircraft (Ripple): Red (#D9534F) — 38.95%
    - Carrier Operations: Orange (#F0AD4E) — 32.62%
    - NAS (Air Traffic Control): Blue (#0275D8) — 17.92%
    - Severe Weather: Purple (#5BC0DE) — 10.27%
* **Visual 2 (Top Right - Stacked Bar Chart):**
  - **Title:** "Flight Cancellation Drivers"
  - **Y-Axis:** `CancellationReasons[Cancellation_Reason]`
  - **X-Axis:** `CancellationReasons[Cancelled_Flights]`
  - **Finding:** Weather (59.3%) vs Carrier (37.9%).
* **Visual 3 (Bottom Left - Line & Area Chart):**
  - **Title:** "Hourly Delay Cascade (Compounding Delay Curve)"
  - **X-Axis:** `HourlyDelays[Dep_Hour]` (0 to 23)
  - **Y-Axis:** `HourlyDelays[Delay_Probability_Pct]`
  - **Finding:** Starts low at 6 AM (~11%) and peaks at 6–8 PM (~28.8%).
* **Visual 4 (Bottom Right - Column Chart):**
  - **Title:** "Weekly Delay Surge by Day of Week"
  - **X-Axis:** `CalendarDelays[Day_Of_Week]` (1=Mon ... 7=Sun)
  - **Y-Axis:** `CalendarDelays[Delay_Rate_Pct]`
  - **Finding:** Highest on Tuesday (26.9%) and Friday (25.7%); lowest on Thursday (18.3%).

---

### Page 3: Route Corridor Intelligence
* **Objective:** Analyze domestic city pairs, sky highway throughput, and corridor reliability.
* **Visual 1 (Top Row - Table / Matrix):**
  - **Title:** "High-Traffic Sky Corridors & Reliability"
  - **Columns:**
    1. `RouteTraffic[Route_Name]` (e.g. OGG-HNL, LAX-SFO)
    2. `RouteTraffic[Flight_Count]` (Total flights in corridor)
    3. `RouteTraffic[Route_Delay_Rate_Pct]` (Color coded data bar)
    4. `RouteTraffic[Distance_Miles]`
    5. `RouteTraffic[Avg_AirTime_Min]`
  - **Sort:** Descending by `Flight_Count`.
* **Visual 2 (Bottom Left - Scatter Plot):**
  - **Title:** "Corridor Distance vs. Delay Rate"
  - **X-Axis:** `RouteTraffic[Distance_Miles]`
  - **Y-Axis:** `RouteTraffic[Route_Delay_Rate_Pct]`
  - **Size:** `RouteTraffic[Flight_Count]`
* **Visual 3 (Bottom Right - Clustered Column Chart):**
  - **Title:** "Top 10 Most Delayed High-Volume Routes"
  - **X-Axis:** `RouteTraffic[Route_Name]` (Filtered where `Flight_Count > 500`)
  - **Y-Axis:** `RouteTraffic[Route_Delay_Rate_Pct]`
* **Slicers:**
  - `RouteTraffic[Origin]` (Dropdown search)
  - `RouteTraffic[Dest]` (Dropdown search)

---

### Page 4: AI/ML Delay Risk Predictor
* **Objective:** Display pre-departure flight risk ratings generated by PySpark MLlib.
* **Header:** "Pre-Departure AI Delay Risk Scoring (Leak-Free ML Model)"
* **KPI Cards (Top Row):**
  1. **Model AUC-ROC Score:** `0.66` (Industry Standard for Schedule-Only Features)
  2. **Model Accuracy:** `78.4%`
  3. **High-Risk Flights Scored:** `1,280` flights
  4. **Low-Risk Flights Scored:** `2,450` flights
* **Visual 1 (Left - Donut Chart):**
  - **Title:** "Flight Distribution by Predicted Risk Category"
  - **Legend:** `MLPredictions[Risk_Category]`
  - **Values:** Count of `MLPredictions[Risk_Category]`
  - **Colors:**
    - Low Risk (<15%): Green (#5CB85C)
    - Medium Risk (15–30%): Yellow (#F0AD4E)
    - High Risk (≥30%): Red (#D9534F)
* **Visual 2 (Right - Detailed Flight Inspection Table):**
  - **Title:** "Individual Flight Pre-Departure Risk Scorer"
  - **Columns:**
    1. `FlightDate`
    2. `Reporting_Airline`
    3. `Origin` $\rightarrow$ `Dest`
    4. `Dep_Hour` (Scheduled Departure Hour)
    5. `Delay_Probability` (e.g. 0.3845 $\rightarrow$ formatted as 38.5%)
    6. `Risk_Category` (High / Medium / Low)
    7. `Predicted_Delayed` (1 or 0)
    8. `Actual_Delayed` (Ground truth validation)
* **Slicers:**
  - `MLPredictions[Risk_Category]` (Buttons: Low | Medium | High)
  - `MLPredictions[Reporting_Airline]`
  - `MLPredictions[Dep_Hour]`
