#!/usr/bin/env python

"""
UK Biobank TSV to Parquet Converter Script

This script converts UK Biobank TSV data to Parquet format based on a provided configuration file.
It uses the `ukb_tsv_to_parquet` module for the conversion process.

Usage:
    python ukb_config_based_converter.py <ukb_file> <html_data_dict> <config_file> [--log-dir <logdir>] [--log-file-name <logfile>] [--log-level <loglevel>]

Parameters:
    <ukb_file> (str): Path to the UK Biobank TSV file.
    <html_data_dict> (str): Path to the HTML data dictionary file (created using the `ukbconv` program with the option `docs`).
    <config_file> (str): Path to the YAML configuration file.

Optional Arguments:
    --log-dir <logdir> (str): Directory to store the logfile. Default is 'logs/python/create_parquet_from_config/' in the current working directory.
    --log-file-name <logfile> (str): Name for the log file. If not provided, a default name is generated based on input file names and the current date and time.
    --log-level <loglevel> (str): Set the level for the log file. Available options are: 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'. Default is INFO.

Example:
    python ukb_config_based_converter.py data.tsv data_dict.html config.yaml --log-dir logs/ --log-file-name mylog.log --log-level DEBUG
"""

import logging
import signal
import argparse
import sys
import os
import yaml
from yaml.loader import SafeLoader
import datetime
from pathlib import Path

# Custom scripts
from ukbutils.UKB_data_dict import UKB_DataDict
import ukbutils.ukb_tsv_to_parquet as tsv2parquet 
import ukbutils.utils as utils

LOG_LEVELS = list(logging.getLevelNamesMapping().keys())
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_FILLEMODE = "w"


REQUIRED_FIELDS = ["out_path"]
FALLBACK_VALUES = {
    "nrows": 0,
    "npartitions": 0,
    "tab_offset": 0,
    "force": False,
    "dtype_dict": tsv2parquet.DEFAULT_DTYPE_DICT,
    "max_categories": tsv2parquet.DEFAULT_MAX_CATEGORIES,
    "categorical_type": tsv2parquet.DEFAULT_CATEGORICAL_COLUMNS,
    "encoding": tsv2parquet.DEFAULT_ENCODING,
    "settings": {}
}


def parse_args():
    """
    Parse command-line arguments for the script.

    Returns:
        argparse.Namespace: An object containing parsed command-line arguments.

    This function uses argparse to define and parse command-line arguments for the script.
    It expects three required arguments: 'ukb_file', 'html_data_dict', and 'config_file'.
    Optional arguments include '--log-dir', '--log-file-name', and '--log-level'.
    The default log directory is 'logs/python/create_parquet_from_config/' in the current working directory.
    The log file is named based on input files and the current date and time if not provided.
    The log level defaults to 'INFO' but can be set to 'DEBUG', 'WARNING', 'ERROR', or 'CRITICAL'.
    """
    arg_parser = argparse.ArgumentParser(
        description="Convert UK Biobank TSV data to Parquet format based on a configuration file."
    )
    arg_parser.add_argument(
        dest = "ukb_file",
        help = "Path to the UK Biobank TSV file.",
        type = argparse.FileType("r") 
    )
    arg_parser.add_argument(
        dest = "html_data_dict",
        help = "Path to the HTML data dictionary file (can be created using the `ukbconv` program with the option `docs`).",
        type = argparse.FileType("r")
    )
    arg_parser.add_argument(
        dest = "config_file", 
        help = "Path to the YAML configuration file.",
        type = argparse.FileType("r")
    )

    log_dir = Path.cwd() / "logs/python/create_parquet_from_config"
    
    arg_parser.add_argument(
        "--log-dir", 
        dest = "logdir",
        default = log_dir,
        help = "Directory to store the logfile. The default value is the current working directory /logs/python/create_parquet_from_config/"
    )
    arg_parser.add_argument(
        "--log-file-name", 
        dest = "logfile",        
        help = "Name for the log file. If not provided, the script will generate a log file name based on the input file name and the current date and time in the format 'create_parquet_from_config_{ukb_file}_{config_file}_{date_time}.log', where {ukb_file} is the name of the input UK biobank tsv file (without extension), {config_file} is the name of the used config file and {date_time} is the current date and time in the format 'YYYY-MM-DD_HH:MM:SS'."
    )
    arg_parser.add_argument(
        "--log-level", 
        choices = LOG_LEVELS, 
        dest = "loglevel",
        default = "INFO",
        help = "Set the level for the log file. Available options are: 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'. The default level is INFO."
    )
    
    return arg_parser.parse_args()


def set_logfile(args):
    """
    Set up the logfile based on command-line arguments.

    Args:
        args (argparse.Namespace): Parsed command-line arguments.

    Sets up the logfile based on the provided command-line arguments.
    If 'logfile' is not specified, a default name is generated using input file names and the current date and time.
    The log directory is created if it doesn't exist.
    Configures the logging module with the specified log file, level, format, and file mode.
    """
    logfile_name = args.logfile
    if not logfile_name:
        logfile_name = "create_parquet_from_config_{ukb_file}_{config_file}_{date_time}.log".format(
            ukb_file = os.path.splitext(os.path.basename(args.ukb_file.name))[0],
            config_file = os.path.splitext(os.path.basename(args.config_file.name))[0],
            date_time = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        )

    rel_dir_name = os.path.relpath(args.logdir)
    if not os.path.exists(rel_dir_name):
        os.makedirs(rel_dir_name)
    
    rel_log_file_path = os.path.join(rel_dir_name, logfile_name)

    # set config for logging
    try:
        logging.basicConfig(
            filename = rel_log_file_path, 
            level = args.loglevel, 
            format = LOG_FORMAT,
            filemode = LOG_FILLEMODE
        )
        logging.info(
            f"Done setting up logging: log filepath = {logfile_name}, and log level is set to {args.loglevel}."
        )
    except Exception as e:
        raise ValueError("Not able to create logfile") from e


def read_config(config_file):
    """
    Read configurations from a YAML file.

    Args:
        config_file (str): Path to the YAML configuration file.

    Returns:
        dict: Dictionary of configurations.
    """    
    config_dict = {}
    with open(config_file) as f:
        data = yaml.load_all(f, Loader=SafeLoader)
        for d in data:
            d_keys = list(d.keys())
            
            if len(d_keys) != 1:
                raise ValueError("Each configuration in the configuration file must contain a single top-level key, but found {count} keys: {keys}".format(
                    count=len(d_keys),
                    keys=", ".join(d_keys)
                ))
            elif d_keys[0] in config_dict:
                raise ValueError(f"Duplicate configuration name found: '{d_keys[0]}'. Please make sure each configuration in the YAML file has a unique name.")
            
            if d_keys[0].upper() == "TEMPLATE":
                continue

            config_dict.update(d)

    if len(config_dict.keys()) == 0:
        raise ValueError(f"No configurations found in the given config file ({config}), please note the configuration called template (case-insensitive) will be ignored.")
    else:
        logging.info(f"Finished processing the configuration file {config_file}, found {len(config_dict.keys())} configurations, with the following names: {list(config_dict.keys())}")
        
    return config_dict


def dir_exists(path):
    """
    Check if a directory exists.

    Args:
        path (str): The path to the directory.

    Returns:
        bool: True if the directory exists, False otherwise.
    """
    return os.path.exists(path)


def dir_has_write_access(path):
    """
    Check if a directory has write access.

    Args:
        path (str): The path to the directory.

    Returns:
        bool: True if the directory has write access, False otherwise.
    """
    return os.access(path, os.W_OK)
    

def find_first_existing_parent(path):
    """
    Find the first existing parent directory of a given path.

    Args:
        path (str): The path for which to find the existing parent.

    Returns:
        str: The path of the first existing parent directory. If the path does not exist,
             the function returns the current working directory.
    """
    while not os.path.exists(path):
        path = os.path.dirname(path)
        if not path:
            return os.getcwd()
    return path


def is_dir_empty(path):
    """
    Check if a directory is empty.

    Args:
        path (str): The path to the directory.

    Returns:
        bool: True if the directory is empty, False otherwise.
    """
    return not os.listdir(path)


def check_if_output_dir_is_ok(path, force):
    """
    Check if the output directory is suitable for use.

    Args:
        path (str): The path to the directory.
        force (bool): The force flag indicating whether to overwrite existing content.

    Returns:
        bool: True if the directory is suitable; False otherwise.

    The directory is considered unsuitable if:
    1. It has no write access.
    2. If it exists and not empty AND the force flag is not set
    3. If it doesn't exist the first found parent directory has no write access.

    In case of unsuitability, warning messages are logged to provide context.
    """
    dir_is_ok = True

    if dir_exists(path) and not dir_has_write_access(path):
        log_message = f"Given directory ({path}) has no write access"
        logging.warning(log_message)
        dir_is_ok = False

    if dir_exists(path):
        
        if not (is_dir_empty(path) or force):
            log_message = f"Directory not empty, but force flag not set to force overwriting content of directory: {path}"
            logging.warning(log_message)
            dir_is_ok = False
        
    else:
        existing_parent = find_first_existing_parent(path)
        if not dir_has_write_access(existing_parent):
            logging.warning(f"Given directory can not be created due to no write access in parent directory '{existing_parent}'.")
            dir_is_ok = False

    return dir_is_ok


def apply_fallback_values(config_dict, fallback_values):
    """
    Apply fallback values to the configuration.

    Args:
        config_dict (dict): Dictionary of configurations.
        fallback_values (dict): Fallback values for configurations.
    """
    config_dict = config_dict.copy()
    for config in config_dict:
        for key, fallback_value in fallback_values.items():
            config_dict[config][key] = config_dict[config].get(key, fallback_value)
    return config_dict


def check_required_fields_present(config, required_fields, config_name):
    """
    Check if required fields are present in the given configuration.

    Args:
        config (dict): Configuration dictionary.
        required_fields (list): List of required fields.
        config_name (str): Name of the configuration.

    Returns:
        bool: True if all required fields are present, False otherwise.
    """
    any_missing_field = False
    for required_field in required_fields:
        if required_field not in config:
            logging.warning(f"Required configuration key '{required_field}' not found in {config_name} configuration")
            any_missing_field = True
        
    return not any_missing_field


def get_valid_configs_dict(config_dict):
    """
    Check if the configurations and their settings are valid.

    Args:
        config_dict (dict): Dictionary of configurations.
    """
    valid_config_dict = config_dict.copy()
    failed_configs = set()

    logging.info("Starting to loop through configurations")
    for i, (config_name, config) in enumerate(config_dict.items()):
        logging.info(f"Start processing configuration with index {i}, called {config_name}")

        if not check_required_fields_present(config, REQUIRED_FIELDS, config_name):
            failed_configs.add(config_name)

        force = config.get("force", False)
        overwrite = config.get("settings", {}).get("overwrite", False)
        if not check_if_output_dir_is_ok(config["out_path"], force):
            failed_configs.add(config_name)

    if len(failed_configs) == len(config_dict):
        # All configs failed
        error_msg = "All configurations are invalid. Please check the provided settings."
        logging.critical(error_msg)
        raise ValueError(error_msg)
    elif len(failed_configs):
        # Some configs failed, but not all
        warning_msg = f"Processing completed with warnings. Configurations {failed_configs} are invalid and will be ignored."
        logging.warning(warning_msg)
    else:
        # All configs processed successfully
        success_msg = "All configurations processed without issues."
        logging.info(success_msg)

    for failed_config in failed_configs:
        del valid_config_dict[failed_config]

    return valid_config_dict



def get_config_dict(config_path):
    """
    Get configurations from a YAML file and check their validity.

    Args:
        config (str): Path to the YAML configuration file.

    Returns:
        dict: Dictionary of valid configurations.
    """
    raw_config_dict = read_config(config_path)
    valid_config_dict = get_valid_configs_dict(raw_config_dict)
    config_dict = apply_fallback_values(valid_config_dict, FALLBACK_VALUES)
    return config_dict


def get_data_dict(html_file):
    """
    Create an instance of UKB_DataDict from a UK Biobank data dictionary HTML file.

    Args:
        html_file (str): Path to the UK Biobank data dictionary HTML file.

    Returns:
        UKB_DataDict: An instance of the UKB_DataDict class.

    This function initializes a UKB_DataDict instance by parsing the provided HTML file.
    The UKB_DataDict object is ready to parse and extract information from the HTML data dictionary.

    Raises:
        ValueError: If initialization of UKB_DataDict instance fails.
    """
    try:
        data_dict = UKB_DataDict(html_file)
        logging.info(f"Initiated UKB_DataDict instance with UK Biobank data dictionary html file from path {html_file}, object ready to parse HTML.")
    except Exception as e:
        raise ValueError(f"Initiating UKB_DataDict instance with UK Biobank data dictionary html file from path {html_file} failed") from e
    return data_dict 


def convert_config_to_parquet(file_in, data_dict, config, col_names):
    
    
    missing_types = tsv2parquet.get_missing_types_in_dtype_dict(config["dtype_dict"], data_dict, config["categorical_type"])
    if missing_types["Type"] or missing_types["Encoding_type"]:
        raise ValueError(f"Some types found in data_dict are not present in dtype_dict, types not found are: {missing_types}")
    # missing_types = tsv2parquet.get_missing_types_in_dtype_dict(config["dtype_dict"], data_dict, config["categorical_type"])
    # if missing_types:
    #     raise ValueError(f"Some types found in data_dict are not present in dtype_dict, types not found are: {missing_types}")

    tsv2parquet.convert_to_parquet(
        tsv_file_in = file_in,
        out_dir = config["out_path"],
        data_dict = data_dict,
        column_names = col_names,
        nrows = config["nrows"],
        npartitions = config["npartitions"],
        dtype_dict = config["dtype_dict"],
        settings = config["settings"],
        offset = config["tab_offset"],
        force = config["force"],
        max_categories = config["max_categories"],
        encoding = config["encoding"]
    )


def main(tsv_filename, html_filename, config_filename):

    config_dict = get_config_dict(config_filename)
    data_dict = get_data_dict(html_filename)

    try:
        logging.info(f"Trying to retrieve main table from the data dictionary ({html_filename})")
        data_dict.main_table
        logging.info(f"Done retrieving main table from the data dictionary ({html_filename})")
    except Exception as e:
        raise ValueError(f"Could not retrieve main table from data dictionary file ({html_filename})") from e
    
    col_names = tsv2parquet.get_column_names(tsv_filename, data_dict)

    for key in config_dict:
        dtype_dict = config_dict[key]["dtype_dict"]    
        convert_config_to_parquet(
            file_in = tsv_filename,
            data_dict = data_dict,
            config = config_dict[key],
            col_names = col_names
        )


def main_cli():
    # Function to run when program termination signal is recieved, will write a line to logfile
    signal.signal(signal.SIGTERM, utils.terminate_signal_handler)
   
    # Parse arguments
    args = parse_args()

    # Set log file
    set_logfile(args)

    logging.info("Parsed following args: {args}".format(
        args = ", ".join([f"\n\t{arg}: {getattr(args, arg)}" for arg in vars(args)])
    ))

    try:
        main(
            tsv_filename = args.ukb_file.name,
            html_filename = args.html_data_dict.name,
            config_filename = args.config_file.name
        )
    except Exception as e:
        utils.exit_script(
            f"An exception occured during the execution of the {__file__} script.", 
            log_function=logging.exception, 
            status=1, 
            exit_message=f"An exception while converting UKB files from config {args.config_file.name}, see {logging.getLoggerClass().root.handlers[0].baseFilename}"
        )

    logging.info("Script done running, exiting...")
    sys.exit(0)


if __name__ == "__main__":
    main_cli()
