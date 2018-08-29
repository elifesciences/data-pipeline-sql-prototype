class TestDataPipelineDag:
    def test_can_build_dag(self):
        from dags.data_pipeline_dag import dag
        assert dag
