####################################################
# tools
####################################################


def get_temperature_current(location: str, unit: str = "celsius"):
    """Get current temperature at a location.

    Args:
        location: The location to get the temperature for, in the format "City, State, Country".
        unit: The unit to return the temperature in. Defaults to "celsius". (choices: ["celsius", "fahrenheit"])

    Returns:
        the temperature, the location, and the unit in a dict
    """
    result = {
        "temperature": 26,
        "location": location,
        "unit": unit,
    }
    return result


def get_temperature_tomorrow(location: str, unit: str = "celsius"):
    """Get tomorrow's temperature at a location.

    Args:
        location: The location to get the temperature for, in the format "City, State, Country".
        unit: The unit to return the temperature in. Defaults to "celsius". (choices: ["celsius", "fahrenheit"])

    Returns:
        the temperature, the location, and the unit in a dict
    """
    result = {
        "temperature": 23,
        "location": location,
        "unit": unit,
    }
    return result


def get_function_by_name(name):
    if name == "get_temperature_current":
        return get_temperature_current
    elif name == "get_temperature_tomorrow":
        return get_temperature_tomorrow
    else:
        return None


TOOLS = {
    "get_temperature_current": {
        "type": "function",
        "function": {
            "name": "get_temperature_current",
            "description": "Get current temperature at a location.",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": 'The location to get the temperature for, in the format "City, State, Country".',
                    },
                    "unit": {
                        "type": "string",
                        "enum": ["celsius", "fahrenheit"],
                        "description": 'The unit to return the temperature in. Defaults to "celsius".',
                    },
                },
                "required": ["location"],
            },
        },
    },
    "get_temperature_tomorrow": {
        "type": "function",
        "function": {
            "name": "get_temperature_tomorrow",
            "description": "Get tomorrow's temperature at a location.",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": 'The location to get the temperature for, in the format "City, State, Country".',
                    },
                    "unit": {
                        "type": "string",
                        "enum": ["celsius", "fahrenheit"],
                        "description": 'The unit to return the temperature in. Defaults to "celsius".',
                    },
                },
                "required": ["location"],
            },
        },
    },
}
