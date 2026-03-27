####################################################
# variable providers
####################################################

import time as _time


def _get_time():
    return _time.strftime("%H:%M")


def _get_date():
    return _time.strftime("%Y-%m-%d")


def _get_weekday():
    return ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"][_time.localtime().tm_wday]


VARIABLE_PROVIDERS = {
    "time": _get_time,
    "date": _get_date,
    "weekday": _get_weekday,
}


def resolve_variables(text, static_vars=None):
    """Replace {{key}} macros in text with values from providers and static vars.

    Args:
        text: The text containing {{key}} macros.
        static_vars: Optional dict of static key-value pairs from config.

    Returns:
        Text with macros replaced. Unknown macros are left as-is.
    """
    import re
    merged = dict(VARIABLE_PROVIDERS)
    if static_vars:
        for k, v in static_vars.items():
            merged[k] = lambda val=v: val

    def replace(match):
        key = match.group(1)
        if key in merged:
            provider = merged[key]
            return str(provider())
        return match.group(0)  # leave unknown macros as-is

    return re.sub(r"\{\{(\w+)\}\}", replace, text)


####################################################
# tools
####################################################


def get_weather(location: str):
    """Get current weather at a location.

    Args:
        location: The city name, e.g. "Beijing", "Tokyo", "Shanghai".

    Returns:
        Weather information including temperature, description, humidity, wind.
    """
    try:
        import requests
        r = requests.get(
            f"http://wttr.in/{location}?format=j1&lang=zh",
            timeout=10,
            headers={"User-Agent": "curl/7.0"},
        )
        data = r.json()
        cur = data["current_condition"][0]
        desc_key = next((k for k in cur if k.startswith("lang")), None)
        desc = cur[desc_key][0]["value"] if desc_key else cur.get("weatherDesc", [{}])[0].get("value", "")
        return {
            "location": location,
            "time": _time.strftime("%Y-%m-%d %H:%M"),
            "temperature": f"{cur['temp_C']}°C",
            "feels_like": f"{cur['FeelsLikeC']}°C",
            "description": desc,
            "humidity": f"{cur['humidity']}%",
            "wind": f"{cur['windspeedKmph']}km/h",
        }
    except Exception as e:
        return {"error": str(e)}


def web_search(query: str, max_results: int = 3):
    """Search the web for information. Use this when you need to look up current events, facts, or anything you're not sure about.

    Args:
        query: The search query string.
        max_results: Maximum number of results to return. Defaults to 3.

    Returns:
        A list of search results with title and snippet.
    """
    try:
        from ddgs import DDGS
        results = list(DDGS().text(query, max_results=max_results, backend="lite"))
        if not results:
            return {"results": [], "message": "No results found."}
        return {
            "results": [
                {"title": r["title"], "snippet": r["body"][:200]}
                for r in results
            ]
        }
    except Exception as e:
        return {"error": str(e)}


def get_function_by_name(name):
    functions = {
        "get_weather": get_weather,
        "web_search": web_search,
    }
    return functions.get(name, None)


TOOLS = {
    "get_weather": {
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "Get current weather at a location.",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": 'The city name, e.g. "Beijing", "Tokyo", "Shanghai".',
                    },
                },
                "required": ["location"],
            },
        },
    },
    "web_search": {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": "Search the web for current information. Use this when you need to look up facts, news, events, or anything you're not certain about.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The search query.",
                    },
                },
                "required": ["query"],
            },
        },
    },
}
