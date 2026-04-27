import yaml

def read_yaml(path: str) -> dict:
    """
    Read a YAML file and return the contents as a dictionary.
    
    Args:
        path: The path to the YAML file.

    Returns:
        A dictionary containing the contents of the YAML file.

    Raises:
        FileNotFoundError: If the file does not exist.
        yaml.YAMLError: If the file is not valid YAML.  
    """

    try:
        with open(path, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {path}")
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Error parsing YAML file: {path}") from e
    except Exception as e:
        raise Exception(f"Error reading YAML file: {path}") from e