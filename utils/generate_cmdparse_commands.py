#!/usr/bin/env python3

import json
import os


def load_commands(commands_dir):
    commands = []
    for filename in sorted(os.listdir(commands_dir)):
        if not filename.endswith('.json'):
            continue
        filepath = os.path.join(commands_dir, filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        commands.append(data)
    return commands


def generate_command_def(output_path, commands):
    lines = [
        "/* ================================================================",
        " * utils/generate_cmdparse_commands.py auto build , don't modify! ",
        " * ================================================================ */",
        "",
        "static cmdParseCommandDef cmd_parse_commands[] = {",
    ]

    for cmd in commands:
        lines.append(f'    {{"{cmd["name"]}", {cmd["count"]}, {cmd["parse"]} }},')

    lines.extend([
        "    {NULL, NULL, NULL}",
        "};",
        "",
    ])

    content = "\n".join(lines) + "\n"

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"Generated {output_path} with {len(commands)} commands.")


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    commands_dir = os.path.join(script_dir, "..", "xredis", "commands")
    output = os.path.join(script_dir, "..", "xredis", "xredis_commands.def")

    commands = load_commands(commands_dir)
    generate_command_def(output, commands)
