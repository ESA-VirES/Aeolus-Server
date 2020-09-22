class InvalidFieldError(Exception):
    code = 'NoSuchField'

    def __init__(self, name, location):
        self.locator = name
        super().__init__("No such field '%s' (%s)" % (name, str(location)))
