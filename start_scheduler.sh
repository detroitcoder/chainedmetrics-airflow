
eval "$(conda shell.bash hook)"
conda activate airflow
source ./airflow_env.sh

nohup airflow scheduler --log-file $HOME/airflow/logs/scheduler/scheduler.log --pid $HOME/airflow/scheduler.pid > $HOME/airflow/logs/scheduler/nohup_stdout.log &