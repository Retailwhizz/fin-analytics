
codes = {
    401: "Unauthorized",
    404: "Not found",
    500: "Internal server error"
}


class RestError(Exception):
    pass


class ConsoleError(Exception):
    pass
