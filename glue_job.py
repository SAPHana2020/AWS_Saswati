import boto3

# Create a Glue client
glue_client = boto3.client('glue')

# Define the Glue job parameters
job_name = 'CSVToParquetJob'
script_location = 's3://mybucketsaswati/CapstoneScript/csv_to_parquet.py'
csv_path = 's3://mybucketsaswati/covid-19-testing-data/dataset/states_current/'
parquet_path = 's3://mybucketsaswati/CSVtoParquet/'

# Create the Glue job
response = glue_client.create_job(
    Name=job_name,
    Role='arn:aws:iam::653200375372:role/AWS_Glue_Job_Saswati',
    Command={
        'Name': 'glueetl',
        'ScriptLocation': script_location,
    },
    DefaultArguments={
        '--input_path': csv_path,
        '--output_path': parquet_path,
        '--TempDir': 's3://mybucketsaswati/temp/',
    },
    ExecutionProperty={
        'MaxConcurrentRuns': 1,
    },
    Timeout=2880,
    MaxCapacity=5.0,
)

# Start the Glue job
glue_client.start_job_run(JobName=job_name)