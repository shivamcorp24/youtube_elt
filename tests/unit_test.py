def test_dags_integrity(dagBag):

    # 1.
    assert dagBag.import_errors == {}, f"Import errors found: {dagBag.import_errors}"
    print("============")
    print(dagBag.import_errors)

    # 2.
    expected_dag_ids = ["produce_json", "update_db", "data_quality"]
    loaded_dag_ids = list(dagBag.dags.keys())

    print("============")
    print(dagBag.dags.keys())

    for dag_id in expected_dag_ids:
        assert dag_id in loaded_dag_ids, f"DAG {dag_id} is missing"

    # 3.
    assert dagBag.size() == 3
    print("============")
    print(dagBag.size())

    # 4.
    expected_task_counts = {
        "produce_json": 5,
        "update_db": 3,
        "data_quality": 2,
    }

    print("============")

    for dag_id, dag in dagBag.dags.items():
        expected_count = expected_task_counts[dag_id]
        actual_count = len(dag.tasks)

        assert (
            expected_count == actual_count
        ), f"DAG {dag_id} has {actual_count} tasks, expected {expected_count}."

        print(dag_id, len(dag.tasks))