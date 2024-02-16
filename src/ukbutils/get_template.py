#!/usr/bin/env python
"""
Script to generate a template YAML file for the ukb-parquet-from-config CLI tool.
"""
import os
import shutil
import argparse
from importlib import resources as impresources

from ukbutils import templates


def parse_args():
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
        dest="dest", 
        help="Path to store the template YAML file.", 
        type=str
    )
    
    return arg_parser.parse_args()


def get_abs_path(path):
    """
    Get the absolute path of a file or directory.

    Args:
        path (str): The path to be resolved.

    Returns:
        str: The absolute path.
    """
    return os.path.abspath(os.path.expanduser(path))
    

def path_is_safe(dest):
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
    


def copy_template(dest_file):
    """
    Copy the template YAML file to the specified destination.

    Args:
        dest_file (str): The destination path where the template YAML file will be copied.
    """
    template_yaml = (impresources.files(templates) / "TEMPLATE_config.yaml")
    shutil.copyfile(template_yaml, dest_file)
    


def main_cli():
    """
    Main function to run the script.
    """
    args = parse_args()
    
    abs_dest = get_abs_path(args.dest)

    if os.path.isfile(abs_dest):
        raise ValueError(f"Destination file ({args.dest}) already exists")
    else:
        copy_template(abs_dest)


if __name__ == "__main__":
    main_cli()
    

