#!/usr/bin/env python
"""
Script to generate a template YAML file for the ukb-parquet-from-config CLI tool.

This script generates a template YAML file that can be used as a configuration file
for the ukb-parquet-from-config CLI tool. The template YAML file contains default
settings and placeholders for user-defined parameters.

Usage:
    Run this script with the destination path where you want to store the
    template YAML file.
    Example:
        $ python generate_template.py ~/path/to/store/template.yaml

Functions:
    - generate_template(dest): Generate the template YAML file.
    - main_cli(): Main function to run the script, used in pyproject.toml.

Author:
    Naomi Hindriks
"""
import os
import shutil
import argparse

from importlib import resources as impresources
from . import templates


def _parse_args():
    """
    Parse command-line arguments for the script.

    Returns:
        argparse.Namespace: The parsed command-line arguments.
    """
    arg_parser = argparse.ArgumentParser(
        description=(
            "Get a template yaml to use for the ukb-parquet-from-config cli tool."
        )
    )
    arg_parser.add_argument(
        dest="dest", help="Path to store the template YAML file.", type=str
    )

    return arg_parser.parse_args()


def _get_abs_path(path):
    """
    Get the absolute path of a file or directory.

    Args:
        path (str): The path to be resolved.

    Returns:
        str: The absolute path.
    """
    return os.path.abspath(os.path.expanduser(path))


def _path_is_safe(dest):
    """
    Check if the destination path is safe to write to.

    Args:
        dest (str): The destination path.

    Returns:
        bool: True if the destination path does not exist, False otherwise.
    """
    if os.path.isfile(dest):
        return False
    else:
        return True


def _copy_template(dest_file):
    """
    Copy the template YAML file to the specified destination.

    Args:
        dest_file (str): The destination path where the template YAML file will
        be copied.
    """
    template_yaml = impresources.files(templates) / "TEMPLATE_config.yaml"
    shutil.copyfile(template_yaml, dest_file)


def generate_template(dest):
    """
    Generate the template YAML file.

    This function generates a template YAML file that can be used as a configuration
    file for the ukb-parquet-from-config CLI tool.

    Args:
        dest (str): The destination path where the template YAML file will be saved.

    Raises:
        ValueError: If the destination file already exists.
    """
    abs_dest = _get_abs_path(dest)

    if not _path_is_safe(abs_dest):
        raise ValueError(f"Destination file ({dest}) already exists")
    else:
        _copy_template(abs_dest)


def main_cli():
    """
    Main function to run the script as a command-line interface.
    """
    args = _parse_args()
    generate_template(args.dest)


if __name__ == "__main__":
    main_cli()
