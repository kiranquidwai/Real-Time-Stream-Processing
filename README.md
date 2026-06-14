# Real-Time Stream Processing Assignment — for Smart Power Grid

## Objective

This project has built a Spark structured Streaming pipeline for a smart power grid, whose goal is to detect residential zones whose electricity consumption unexpectedly exceeds the industrial average.

For this assignment, the pipeline has used the UCI Household Power Consumption dataset which has simulated the smart-meter streaming by assigning synthetic meter IDs to the original household readings.

## Dataset

Dataset used:

UCI Individual Household Electric Power Consumption Dataset
https://archive.ics.uci.edu/dataset/235/individual+household+electric+power+consumption

The original dataset file is:

```text
household_power_consumption.txt
```
The original dataset file is:

household_power_consumption.txt

Place it inside:

data/

Expected path:

data/household_power_consumption.txt

The dataset is semicolon-separated, with columns such as:

```text
Date;Time;Global_active_power;Global_reactive_power;Voltage;Global_intensity;Sub_metering_1;Sub_metering_2;Sub_metering_3
```

This project has used `Global_active_power` as the electricity consumption value.

## Project Structure

```text
assignemnt_6/
├── pipeline.py
├── make_batches.py
├── README.md
├── Result_Screenshot.png
├── static/
│   └── zone_mapping.csv

```

## Static Zone Mapping

The original UCI dataset does not include meter IDs, zone IDs, or zone types. To satisfy the assignment requirement, the project creates synthetic meter IDs and joins them with a static mapping file.

The static file is:

```text
static/zone_mapping.csv
```

## Window Type

The pipeline uses a sliding window:

```text
Window size: 5 minutes
Slide interval: 1 minute
```

## Why Sliding Window?

I have used the sliding window for this dataset because it is appropriate for continous change of power consumption with time. Here, 5-minute window has given enough readings to calculate an  average, while the 1-minute slide has allowed the system to update frequently.

Compared with a tumbling window, the sliding window detects changes earlier because it does not wait for one full non-overlapping period to finish.

The pipeline required state during the window aggregation. Spark must keep records that belong to each active 5-minute window so it can calculate the average consumption per zone. Also, state is  required when comparing residential consumption with the industrial average for the same time window. Whereas watermarking is used on the event timestamp to limit how long Spark keeps old window state.

## Alert Condition

The alert condition is:

```text
Residential average consumption > Industrial average consumption
```

When this condition is true, the pipeline outputs a grid anomaly alert.

The alert output includes:

```text
window_start
window_end
zone_id
zone_type
avg_consumption
industrial_avg
```

## Requirements

Install the required Python packages:

```bash
pip install pyspark==3.5.0 pandas
```

If running locally on Windows, Java must also be installed.

Recommended:

```text
Java 17
Python 3.8+
PySpark 3.5.0
```

## How to Run the Project

### Step 1: Prepare the Dataset

Download the UCI Household Power Consumption dataset and place the file here:

```text
data/household_power_consumption.txt
```

### Step 2: Generate Streaming Batch Files

Run:

```bash
python make_batches.py
```

It will read the original dataset, and will create the synthetic meter IDs, and generate batch CSV files.

### Step 3: Run the Spark Streaming Pipeline

Run:

```bash
python pipeline.py
```

### Step 4: Observe Alert Output

If a residential zone's average consumption exceeds the industrial average in the same sliding window, it will be printed in an alert table.


```text
===== GRID ANOMALY ALERTS =====

```
This confirms that the alert condition has fired successfully.
