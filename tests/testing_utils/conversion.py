import json


def get_output(capsys):
    captured = capsys.readouterr()
    return captured.out


def get_output_as_json(capsys):
    return json.loads(get_output(capsys))
