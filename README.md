 ## **AVIATION ELT**

<br>

### Project Summary

This project demonstrates an Analytical Engineering approach to aviation data using a modern data stack, covering Extraction, Loading, 
and Transformation (ELT). It leverages tools like Airbyte for data ingestion and dbt for data transformation to provide actionable insights 
for business decision-making.

### Role of an Analytic Engineer
Analytical Engineers bridge the gap between Data Engineering and Data Analysis, facilitating data interpretation and actionable insights for 
business needs. They use data modeling techniques and tools like Snowflake and dbt to ensure data is ready for analysis and decision-making.


### Project Overview

This project involves analyzing an airline's operational data from the [booking database](https://postgrespro.com/docs/postgrespro/10/demodb-bookings.html), 
focusing on ticket sales, bookings, flights, and more. The goal is to derive insights that inform route optimization, customer retention, and operational efficiencies.

<br>

<img width="400" alt="oltp_erd" src="https://github.com/TayoPraise/SQL-Projects/assets/107925747/c45ea73d-0eea-4d1b-b036-4a9279e0a9f3">

<br>

### Business Requirements

The project aims to help the airline understand and utilize its data to:
- Optimize routes and reduce costs.
- Enhance customer acquisition and retention.
- Improve operational efficiency.

After interviews and research into the industry, an 
[Enterprise Bus Metrix](https://docs.google.com/spreadsheets/d/1lqwUuz06USIu8CvluBkkQd7l8LA_fK3xdKMMbEdSFqs/edit?gid=1207146075#gid=1207146075) was developed 
to structure the project and guide the creation of Dimensional Models, aligning with the business requirements.

### Data Modeling:

#### Conceptual Model:
  This is a high-level representation of key data entities and their relationships, used for communicating with stakeholders and defining data requirements.
  
<br>
<img width="350" alt="conceptual_model" src="https://github.com/TayoPraise/SQL-Projects/assets/107925747/bcb472e0-6a57-4d08-a5aa-fd8f5b388088">

#### Logical Model:
  The logical model is a detailed specification of data entities, attributes, and relationships, serving as a blueprint for database design without considering 
  physical implementation.
  
<br>
<img width="400" alt="logical_model" src="https://github.com/TayoPraise/SQL-Projects/assets/107925747/00cd54b4-a2e6-4684-a24b-125b4feb3220">

#### Physical Model:
  The physical model translates the logical model into a technical blueprint for actual database creation. It specifies the exact schema, including tables, columns, and indexes 
  
<br>
<img width="350" alt="physical_model" src="https://github.com/TayoPraise/SQL-Projects/assets/107925747/74c0b6c7-1955-4542-9a15-eb5ea0425340">




### Project Tools & Architecture

In this project, I made use of cloud-based data tools in the modern data stack such as;

- Data Architecture: Designed using [Draw.io](https://draw.io/)

<img width="700" alt="image" src="https://github.com/TayoPraise/SQL-Projects/assets/107925747/a4c5282c-19b8-4ef5-ba47-d8da2d6e8277">

<br>
<br>

- Data Ingestion: Using [Airbyte](https://docs.airbyte.com/):
  Airbyte simplifies data extraction from various sources and loading into Snowflake, ensuring data integrity and availability for transformation and analysis.

<br> 
<br> 


<img width="400" alt="airbyte_jobs" src="https://github.com/TayoPraise/SQL-Projects/assets/107925747/89046c5e-2a2e-4e50-98e7-1330786ee2dd">

<br> 
<br> 

- Data Transformation: Using the data build tool ([dbt](https://docs.getdbt.com/)):
  dbt automates data cleaning, transformation, and testing. It ensures data quality through modular SQL practices, documentation, and testing capabilities, making
  data ready for analysis and reporting.

<br>
<br> 


<img width="400" alt="dbt_run" src="https://github.com/TayoPraise/aviation_elt/assets/107925747/3f69faad-db3a-4895-ab0d-9a7e164874a0">
<img width="400" alt="dbt_test" src="https://github.com/TayoPraise/aviation_elt/assets/107925747/71907c20-a9ed-47ff-9b58-c37db80e8879">

<br>
<br> 


- Data Warehousing - [Snowflake](https://www.snowflake.com/en/):
  Snowflake is a cloud-based data warehousing platform that offers scalable storage and compute power for efficiently managing and analyzing large volumes of data.

<br>

### Data Pipeline Layers:

- #### src_aviation:
    This source layer is a replica of the source database (OLTP system). It's the initial layer where the Airbyte run ingests the data.
    It's untouched and unprocessed.  
  <img width="353" alt="src_aviation" src="https://github.com/TayoPraise/SQL-Projects/assets/107925747/a56db4e8-65f1-4db8-861d-d127f253e661">

  
- #### [stg_aviation](models/staging):
    Data cleaning and transformation were primarily conducted in the staging layer. Tasks included:
     - Text cleaning to select English values from JSON columns containing both English and Russian content.
     - Creation of surrogate keys for tables due to lengthy and complex natural keys that were not integers.
     - Separation of the customer table from the fact table (tickets) to accommodate customer dimension metrics.
 <br>
 
<img width="353" alt="stg_aviation" src="https://github.com/TayoPraise/SQL-Projects/assets/107925747/2a771e0a-ce9e-4298-b903-e786e248e24c">

- #### [dwh_aviation](models/warehouse):
    The data warehouse layer implemented dimensional modeling using the Star schema. It focused on aggregating key metrics:
     - Financial metrics like cumulative_revenue.
     - Operational metrics such as age_in_fleet, flight_to_next_maintenance, and number_of_maintenance.
     - Additionally, dbt tests were developed in this layer to validate data integrity.
  <br>
 
<img width="353" alt="dwh_aviation" src="https://github.com/TayoPraise/SQL-Projects/assets/107925747/771137ab-c159-490c-9ab0-341da053985f">

- #### [obt_aviation](models/analytics_obt):
    The analytics layer consolidated business scenarios into a single table, enabling the creation of models to address executive queries and provide
    key insights. This layer minimized the need for complex joins, streamlining dashboard creation in business intelligence tools.
 <br>
 
<img width="353" alt="obt_aviation" src="https://github.com/TayoPraise/SQL-Projects/assets/107925747/4c60e43d-8138-49df-9264-15d307400243">

<br>


### Business Questions
- #### [Route Metrics](models/analytics_obt/obt_route_metrics.sql):
    The Operating and Finance teams sought insights into the most frequently booked routes, focusing on ticket volumes and cumulative revenue generated
    by each flight class. To achieve this, I extracted and integrated data from the Fact_Tickets, Fact_Flights, and Dim_Airport tables, providing a
    comprehensive analysis of route performance and financial contributions by flight class.

- #### [Customer Loyalty Metrics & Customer Route Metrics](models/analytics_obt/obt_customer_route_metrics.sql):
    The Marketing team seeks to design campaigns based on passenger travel patterns, including preferred routes, ticket classes, and trip frequency, to
    encourage loyalty through offers like complimentary flights for frequent flyers. The Finance team uses these metrics to assess cumulative revenue and
    calculate passenger lifetime value.
  
    Data from Fact_Tickets, Fact_Flights, and Dim_Customer tables were used to support these analyses.

- #### [Aircraft Maintenance Metrics & Aircraft Flight Metrics](models/analytics_obt/obt_aircraft_metrics.sql):
    The Engineering and Operations teams focus on closely monitoring aircraft to ensure optimal fleet performance. Following [FAA](https://en.wikipedia.org/wiki/Aircraft_maintenance_checks)
    recommendations that each aircraft undergo an A-check every 200-300 flights, I developed a tracking system that counts down from 300 with each flight.
    This system enables the Engineering team to anticipate maintenance needs and plan accordingly, while the Operations team can identify which aircraft are
    available for flights or undergoing maintenance.

    Additionally, the Finance team is interested in tracking the revenue generated by each aircraft across different flight classes, allowing for an assessment
    of financial performance and resource allocation within the fleet.
    To gather these insights, I utilized data from the Fact_Tickets, Fact_Flights, and Dim_Aircraft tables, ensuring a comprehensive view of aircraft maintenance
    schedules and financial contributions.
  
  ### Conclusion
    This aviation analytics project has successfully utilized modern data engineering techniques to extract, transform, and load (ETL) data from operational databases
    into a structured data warehouse. By implementing Airbyte for seamless data ingestion, dbt for efficient data transformation, and Snowflake for scalable data storage
    and querying, we've enabled the airline to derive actionable insights. These insights include optimizing route selection, enhancing customer retention strategies, and
    improving operational efficiencies. This project highlights the critical role of analytical engineering in translating raw data into meaningful business outcomes, driving
    informed decision-making, and strategic planning within the aviation industry.

  ### Resources consulted:
   - https://www.linkedin.com/pulse/airline-route-profitability-vs-network-comparison-revisited-mittal/
   - https://www.linkedin.com/pulse/building-kpis-airlines-divya-mittal/
   - https://en.wikipedia.org/wiki/Aircraft_maintenance_checks
   - https://youtube.com/playlist?list=PLFvr6RxK-URDpo4k4PvvkTttYaKT-jqag&si=Eip4S1FxUuDGPOcn

