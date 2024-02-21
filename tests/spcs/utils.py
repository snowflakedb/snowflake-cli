def assert_mock_execute_is_called_once_with_query(mock_execute, query):
    mock_execute.assert_called_once()

    def convert_to_single_line(q):
        return (" ".join(q.split())).lower()

    assert convert_to_single_line(
        mock_execute.mock_calls[0].args[0]
    ) == convert_to_single_line(query)
