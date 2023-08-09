def replace_text_in_file(file_path: str, to_replace: str, replacement: str) -> None:
    with open(file_path, "r") as file:
        text = file.read()
    text = text.replace(to_replace, replacement)
    with open(file_path, "w") as file:
        file.write(text)
