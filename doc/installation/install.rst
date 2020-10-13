conda env create -n pipeline-v2 -f environment.yaml

#pip install apache-airflow[postgres,redis,celery,crypto]==1.10.12 \
#--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-1.10.12/constraints-3.6.txt"