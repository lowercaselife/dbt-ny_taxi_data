# NYC Taxi Data dbt Project

A dbt learning project demonstrating data transformation and modeling best practices using New York City taxi trip data (green and yellow cab services).

## Project Overview

This project extracts, cleans, and transforms raw taxi trip data into analytics-ready dimensional and fact tables. It demonstrates core dbt concepts including:

- **Data Staging**: Cleaning and standardizing raw data
- **Intermediate Transforms**: Combining multiple source tables
- **Mart Models**: Creating business-ready fact and dimension tables
- **Source Definitions**: Documenting raw data sources
- **Macros**: Reusable SQL logic
- **Seeds**: Loading reference/lookup data
- **Testing & Documentation**: Data quality validation

## Project Structure

```
├── models/
│   ├── staging/              # Raw data cleaning & standardization
│   │   ├── stg_green_tripdata.sql    # Green cab trips staging
│   │   ├── stg_yellow_tripdata.sql   # Yellow cab trips staging
│   │   └── sources.yml               # Source definitions
│   ├── intermediate/         # Intermediate transforms
│   │   └── int_trips_unioned.sql     # Union of green and yellow trips
│   ├── marts/                # Analytics-ready tables
│   │   ├── fct_trips.sql             # Fact table for trips
│   │   ├── dim_vendors.sql           # Vendor dimensions
│   │   ├── dim_zones.sql             # Location/zone dimensions
│   │   └── reporting/                # Reporting models
│   │       └── monthly_revenue_per_location.sql
│   └── example/              # Example models (template files)
├── macros/                   # Reusable SQL functions
│   └── get_vendor_names.sql
├── seeds/                    # Reference data (CSV files)
│   ├── payment_type_lookup.csv
│   └── taxi_zone_lookup.csv
├── tests/                    # Data quality tests
├── analyses/                 # Ad-hoc analysis queries
└── dbt_project.yml          # Project configuration
```

## Data Flow

```
Raw Sources (green_tripdata, yellow_tripdata)
         ↓
Staging Models (stg_green_tripdata, stg_yellow_tripdata)
         ↓
Intermediate Models (int_trips_unioned)
         ↓
Mart Models (fct_trips, dim_vendors, dim_zones)
         ↓
Reporting/Analytics
```

## Key Models

### Staging Layer

- **stg_green_tripdata**: Cleans and casts green cab raw data
- **stg_yellow_tripdata**: Cleans and casts yellow cab raw data

### Intermediate Layer

- **int_trips_unioned**: Combines green and yellow trip data into a unified dataset

### Marts Layer

- **fct_trips**: Fact table containing trip transaction details
- **dim_vendors**: Vendor dimension table
- **dim_zones**: Zone/location dimension table
- **monthly_revenue_per_location**: Reporting model with aggregated revenue metrics

## Getting Started

### Prerequisites

- dbt installed (`pip install dbt-core dbt-[your-warehouse]`)
- Access to your data warehouse with NYC taxi data
- Connection configured in `profiles.yml`

### Running the Project

```bash
# Install dependencies
dbt deps

# Run all models
dbt run

# Run tests
dbt test

# Run specific models
dbt run --select stg_green_tripdata

# Build (run + test)
dbt build
```

## Data Sources

The project assumes two source tables in your warehouse:

- `nytaxi.green_tripdata`: Green cab trip records
- `nytaxi.yellow_tripdata`: Yellow cab trip records

Reference data loaded via seeds:

- `payment_type_lookup.csv`: Payment method codes
- `taxi_zone_lookup.csv`: NYC zone/location reference

## Learning Concepts

This project demonstrates:

1. **Layered Approach**: Staging → Intermediate → Marts architecture
2. **Type Casting**: Converting data types in staging layer
3. **CTEs (Common Table Expressions)**: Complex query logic in intermediate models
4. **Macros**: Reusable logic (see `get_vendor_names` macro)
5. **Source Definitions**: Documenting lineage with `source()` function
6. **Ref Function**: Building dependency graph between models with `ref()`
7. **Seeds**: Loading static reference data

## Resources

- [dbt Documentation](https://docs.getdbt.com/docs/introduction)
- [dbt Best Practices](https://docs.getdbt.com/guides)
- [dbt Community](https://getdbt.com/community)
- [Discourse Q&A](https://discourse.getdbt.com/)
