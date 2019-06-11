#!/usr/bin/env python3

import click

from bubuku import cli

_HEADER = """# Bubuku command line interface

Bubuku provides a command line tool `bubuku-cli` which should be used directly on the instance. Available commands:

"""


def generate_command_docs(name, command, md_file, parent_ctx=None):
    ctx = click.Context(command, info_name=name, parent=parent_ctx)
    sub_commands = getattr(command, "commands", {})

    # generate docs only for actual commands (not command groups)
    if len(sub_commands) == 0:
        cmd_path = ctx.command_path.split()
        cmd_path.pop(0)
        md_file.write("#### {}\n".format(" ".join(cmd_path)))
        md_file.write("```\n{}\n```\n\n".format(ctx.get_help()))
    else:
        # if command has sub-commands - recursively generate docs for all sub-commands
        for sub_cmd_name, sub_command in sub_commands.items():
            generate_command_docs(sub_cmd_name, sub_command, md_file, ctx)


if __name__ == '__main__':
    print("Generating 'cli.md'...")
    
    with open("cli.md", "w") as md_file:
        md_file.write(_HEADER)
        generate_command_docs("bubuku-cli", cli.cli, md_file)

    print("Done")
