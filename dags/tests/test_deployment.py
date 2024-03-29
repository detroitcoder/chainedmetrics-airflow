import pytest
from airflow.models import DagBag

@pytest.fixture()
def dagbag():
    return DagBag(include_examples=False)


def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="deploy_binary_markets")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 1