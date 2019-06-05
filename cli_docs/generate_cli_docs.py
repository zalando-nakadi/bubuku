#!/usr/bin/env python3

import sys

import click
from pkg_resources import iter_entry_points


def generate_docs(name, filename):
    console_scripts = [ep for ep in iter_entry_points('console_scripts', name=name)]

    if len(console_scripts) < 1:
        raise click.ClickException('"{0}" is not an installed console script.'.format(name))

    # read all lines from original file
    original_lines = []
    with open(filename, "r") as md_file:
        original_lines = md_file.readlines()

    with open(filename, "w") as md_file:

        # copy header from original file
        for line in original_lines:
            md_file.write(line)
            if "AUTOGEN_START" in line:
                break

        # generate docs for cli
        for entry_point in console_scripts:
            cli = entry_point.resolve()
            generate_command_docs(name, cli, md_file)


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
    print("Generating docs for bubuku-cli (based on bubuku version that is currently installed on the system)...")
    generate_docs("bubuku-cli", "cli.md")
    print("Done (updated 'cli.md' with latest docs)")
