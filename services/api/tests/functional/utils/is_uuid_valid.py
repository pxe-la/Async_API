def is_valid_uuid(uuid_to_test, version=4):
    """
    Check if a string is a valid UUID.

    :param uuid_to_test: The string to check.
    :param version: The UUID version to check against (default is 4).
    :return: True if the string is a valid UUID, False otherwise.
    """
    import uuid

    try:
        val = uuid.UUID(uuid_to_test, version=version)
    except ValueError:
        return False
    return str(val) == uuid_to_test
