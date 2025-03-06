class SecretType:
    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return "SecretType(***)"

    def __str__(self):
        return "***"

    def __bool__(self):
        return self.value is not None and self.value != ""
