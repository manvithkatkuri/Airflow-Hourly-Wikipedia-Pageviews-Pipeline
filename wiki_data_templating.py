from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import datetime
from urllib import request
from airflow.providers.postgres.operators.postgres import PostgresOperator


dag = DAG(
    dag_id="wiki_pages",
    start_date=datetime.datetime(2024, 12, 31),
    #schedule_interval="@hourly",  # Ensures hourly schedule
    template_searchpath="/opt/airflow/users/wiki"
)

def _get_data(year, month, day, hour, output_path, **_):
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    request.urlretrieve(url, output_path)

get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={
        "year": "{{ (execution_date - macros.timedelta(days=1)).year }}",
        "month": "{{ (execution_date - macros.timedelta(days=1)).month }}",
        "day": "{{ (execution_date - macros.timedelta(days=1)).day }}",
        "hour": "{{ execution_date.hour }}",  
        "output_path": "/opt/airflow/users/wiki/wikiwikipageviews.gz",
    },
    dag=dag,
)

unzip_data = BashOperator(
    task_id="unzip_data",
    bash_command=(
        "gzip -d /opt/airflow/users/wiki/wikiwikipageviews.gz"
    )
)


def _extract_data(pagenames, execution_date, **_):
    result = dict.fromkeys(pagenames, 0)
    with open("/opt/airflow/users/wiki/wikiwikipageviews", mode="r") as f:
        for line in f:
            domain_code, page_title, page_views, _ =line.split(" ")

            if(domain_code == 'en' and page_title in pagenames):
                result[page_title]=page_views
    
    with open("/opt/airflow/users/wiki/postgres_query.sql", "w") as f:
        for page_title, page_views in result.items():
            f.write(
                "INSERT INTO pageview_counts VALUES ("
                f"'{page_title}', {page_views}, '{execution_date}'"
                ");\n"
            )



        

extract_data=PythonOperator(
    task_id="extract_data",
    python_callable=_extract_data,
    op_kwargs={
        "pagenames": {"Google", "Amazon", "Apple", "Microsoft", "Facebook"}
    },
    dag=dag,
)


write_to_postgres=PostgresOperator(
    task_id="write_to_postgres",
    postgres_conn_id="Postgres",
    sql="postgres_query.sql",
    dag=dag,

)


get_data >> unzip_data >> extract_data >> write_to_postgres
