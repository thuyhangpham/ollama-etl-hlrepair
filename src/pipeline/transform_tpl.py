def transform(data: dict) -> dict:
    """
    Transform the input data.
    The input data is expected to be a dictionary with specific keys.
    This function extracts the 'id', 'language', and 'version' fields from the input data.
    If the 'version' field is not present, it defaults to 1.0.

    :param data: The input data to be transformed.
    :return: The transformed data.
    """
    if not isinstance(data, dict):
        raise ValueError("Input data must be a dictionary.")
 
    transformed_data = {
        "id": data["id"],
        "language": data["language"],
        "version": data.get("version", 1.0),
    }
    
    return transformed_data