## Question 1: dlt Version

```bash
!pip install dlt[duckdb]
!dlt --version
```

![alt text](https://github.com/Yaxin12/Data_Engineer/blob/main/Workshop%201_Ingestion%20with%20dlt/image/Screenshot%202025-02-15%20114704.png)

## Question 2: Define & Run the Pipeline (NYC Taxi API)
Use dlt to extract all pages of data from the API.

Steps:

1️⃣ Use the @dlt.resource decorator to define the API source.

2️⃣ Implement automatic pagination using dlt's built-in REST client.

3️⃣ Load the extracted data into DuckDB for querying.