## Question 1: dlt Version

```bash
!pip install dlt[duckdb]
!dlt --version
```

![alt text](https://github.com/Yaxin12/Data_Engineer/blob/main/Workshop%201_Ingestion%20with%20dlt/image/1.png)

## Question 2: Define & Run the Pipeline (NYC Taxi API)
Use dlt to extract all pages of data from the API.

Steps:

1️⃣ Use the @dlt.resource decorator to define the API source.

2️⃣ Implement automatic pagination using dlt's built-in REST client.

3️⃣ Load the extracted data into DuckDB for querying.

```bash
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator


# Define the API resource for NYC taxi data
@dlt.resource(name="rides")   # <--- The name of the resource (will be used as the table name)
def ny_taxi():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        )
    )

    for page in client.paginate("data_engineering_zoomcamp_api"):    # <--- API endpoint for retrieving taxi ride data
        yield page   # <--- yield data to manage memory


# define new dlt pipeline
pipeline = dlt.pipeline(destination="duckdb")


# run the pipeline with the new resource
load_info = pipeline.run(ny_taxi, write_disposition="replace")
print(load_info)


# explore loaded data
pipeline.dataset(dataset_type="default").rides.df()
```

```bash
import duckdb
from google.colab import data_table

# Enable better dataframe display in Colab
data_table.enable_dataframe_formatter()

# Connect to the generated DuckDB database
conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

# Set search path to the dataset
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")

# Show all available tables
tables_df = conn.sql("SHOW TABLES").df()
print("Tables in the dataset:")
print(tables_df)

# Describe the schema of the rides table
describe_df = conn.sql("DESCRIBE TABLE rides").df()
print("Schema of the rides table:")
print(describe_df)

# Fetch first 10 records to verify the data
data_df = conn.sql("SELECT * FROM rides LIMIT 10").df()
print("First 10 records:")
print(data_df)

# Get the number of tables created
num_tables = len(tables_df)
print(f"Number of tables created: {num_tables}")
```
![alt text](https://github.com/Yaxin12/Data_Engineer/blob/main/Workshop%201_Ingestion%20with%20dlt/image/2.png)

### How many tables were created?
- [ ] 2
- [x] 4
- [ ] 6
- [ ] 8