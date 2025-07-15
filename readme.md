### Analyze Dataproc Serverless Jobs in BigQuery

使用BigQuery分析Dataproc serverless任务

#### Pull jobs from your Google Cloud project

update the script according to your project, e.g. `project_id`, `region`

```shell
./get_dataproc_batches.sh > data.json
```

#### Build the data processor and produce the analytical data

```shell
go build main.go
```

now you should see `main` executable in the project directory

```shell
./main --input data.json --output data_new.json
```

#### Load data into BigQuery

Go to the [BigQuery Console](https://console.cloud.google.com/bigquery)

1. create the dataset if there is none
2. create table by `upload` file `data_new.json`, let BigQuery auto detect the
   schema

#### Do the analysis

https://github.com/cloudymoma/dataproc_serverless_analysis/blob/main/dataproc_serverless_analysis.ipynb

Click `Run in Colab Enterprise` then import the notebook into BigQuery.

Update the `TABLE_ID` accordingly to which table you uploaded to from previous
step.

*(optional)* you may need to change the code accordingly to your job
configurations.
