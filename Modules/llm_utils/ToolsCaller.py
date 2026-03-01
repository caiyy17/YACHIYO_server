import json
from .Tools import TOOLS, get_function_by_name


class ToolsCaller:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.tool_names = config.get("tool_names", [])
        self.tools = []
        self.logger.info(f"Tools: {TOOLS}")
        self.logger.info(f"Tool names: {self.tool_names}")
        for tool_name in self.tool_names:
            if tool_name in TOOLS:
                self.tools.append(TOOLS[tool_name])
            else:
                self.logger.info(f"Tool {tool_name} not found in TOOLS.")
        self.reset()

    def reset(self):
        self.functions = {}
        self.current_tool_cal_id = "0"
        self.result = []

    def update_tool_call(self, tool_call):
        if tool_call.id is not None and tool_call.id not in self.functions:
            self.current_tool_cal_id = tool_call.id
            self.functions[self.current_tool_cal_id] = {"name": "", "arguments": ""}
        if tool_call.function is not None:
            if tool_call.function.name is not None:
                self.functions[self.current_tool_cal_id]["name"] += (
                    tool_call.function.name
                )
            if tool_call.function.arguments is not None:
                self.functions[self.current_tool_cal_id]["arguments"] += (
                    tool_call.function.arguments
                )

    def tool_calls_list(self):
        tool_calls = []
        for key, value in self.functions.items():
            tool_calls.append({"id": key, "function": value, "type": "function"})
        return tool_calls

    def tool_calls_result(self):
        results = []
        for key, value in self.functions.items():
            tool_name = value["name"]
            arguments = value["arguments"]
            self.logger.info(f"Calling tool {tool_name} with arguments: {arguments}")
            function = get_function_by_name(tool_name)
            if function is not None:
                try:
                    if arguments == "":
                        arguments = "{}"
                    args_dict = json.loads(arguments)
                    result = function(**args_dict)
                    self.logger.info(
                        f"Tool {tool_name} called with result: {result}"
                    )
                    results.append(
                        {
                            "role": "tool",
                            "name": tool_name,
                            "content": f"{result}",
                            "tool_call_id": key,
                        }
                    )
                except json.JSONDecodeError:
                    self.logger.info(f"Failed to parse arguments: {arguments}")
                    results.append(
                        {
                            "role": "tool",
                            "name": tool_name,
                            "content": f"Error: failed to parse arguments: {arguments}",
                            "tool_call_id": key,
                        }
                    )
                except Exception as e:
                    self.logger.info(f"Error calling {tool_name}: {str(e)}")
                    results.append(
                        {
                            "role": "tool",
                            "name": tool_name,
                            "content": f"Error: {str(e)}",
                            "tool_call_id": key,
                        }
                    )
            else:
                self.logger.info(f"Function {tool_name} not found.")
                results.append(
                    {
                        "role": "tool",
                        "name": tool_name,
                        "content": f"Error: tool '{tool_name}' not found.",
                        "tool_call_id": key,
                    }
                )
        return results
