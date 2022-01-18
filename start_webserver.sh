
eval "$(conda shell.bash hook)"
conda activate airflow
source ./airflow_env.sh

nohup airflow webserver --stderr $HOME/airflow/logs/webserver/stderr.log --stdout $HOME/airflow/logs/webserver/stdout.log --pid $HOME/airflow/webserver.pid > $HOME/airflow/logs/webserver/nohup_stdout.log &