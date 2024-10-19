import json

def text_to_json_string(input_file, output_file):
    # 读取输入文件内容
    with open(input_file, 'r', encoding='utf-8') as file:
        content = file.read()

    # 将内容转为 JSON 格式的字符串
    json_string = json.dumps(content, ensure_ascii=False)

    # 将 JSON 字符串写入输出文件
    with open(output_file, 'w', encoding='utf-8') as file:
        file.write(json_string)

    print(f"JSON 格式的字符串已写入 {output_file}")

# 示例用法
input_file = "utils/input.txt"
output_file = "utils/output.txt"
text_to_json_string(input_file, output_file)