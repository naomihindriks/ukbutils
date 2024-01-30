#!/usr/bin/env python
"""
Converts UK Biobank TSV files to Parquet format:
Reads a UK Biobank TSV file along with its data dictionary (an HTML file, which can be created using the `ukbconv` program with the option `docs`), performs data type conversions, and writes the data to Parquet files.

TODO: 
    1) Change script to enable utilizing distributed computing on a Slurm cluster using Dask.
        - Use Dask Distributed
        - Create a Dask cluster (SLURMCluster) and client for parallel processing. (https://jobqueue.dask.org/en/latest/generated/dask_jobqueue.SLURMCluster.html)
        - Modify Dask DataFrame operations for distributed computing.
        - Add cli options to specify amount of workers and cluster resources`
    2) Some way to give column names as input and only convert those coolumns to parquet.

Usage:
    python convert_tsv_to_parquet.py <ukb_file> <out_dir> <data_dict> [-e ENCODING] [-n NROWS] [-r REPARTITION] [-m MAX_CATEGORIES] [-d DTYPE_DICT ...] [-s SETTINGS ...] [-t TAB_OFFSET] [-f] [--log-dir LOGDIR] [--log-file-name LOGFILE] [--log-level LOGLEVEL]

Arguments:
    <ukb_file>: Path to the UK Biobank TSV file.
    <out_dir>: Directory path to store the Parquet file(s).
    <data_dict>: HTML file containing the UK Biobank data dictionary. (Note: It can be created using the `ukbconv` program with the option `docs`.)


Options:
    -e, --encoding ENCODING: File encoding needed to read the UK Biobank TSV file (default: windows-1252).
    -n, --nrows NROWS: Number of rows of the original data to keep when writing to Parquet file.
    -r, --repartition REPARTITION: Number of partitions to repartition the Dask dataframe (after potential row splicing depending on value in nrows).
    -m, --max_categories MAX_CATEGORIES: Maximum number of levels that can be present in a categorical column where the dtype will be set to categorical (if the number of categories in a column exceeds the specified 'max_categories' threshold, the dtype for that column will be set to the default dtype for that data type, and the column will no longer be treated as categorical). (default: 256).
    -d, --dtype_dict DTYPE_DICT: Specify dtype options in the format 'key=value'. Multiple options can be specified, separated by whitespace.
    -s, --settings SETTINGS: Settings passed to the Dask to_parquet method in the format 'named_arg=value'. Multiple options can be specified, separated by whitespace.
    -t, --tab_offset TAB_OFFSET: Number of extra tabs expected at the start or end of each data row in the TSV file. Use a positive number to indicate tabs at the end of a data row or a negative number to indicate tabs at the start of data rows (default: 0).
    -f, --force: Force the operation, overwriting existing files if needed.
    --log-dir LOGDIR: Directory to store the logfile (default: current working directory/logs/python/convert_tsv_to_parquet).
    --log-file-name LOGFILE: Name for the log file. If not provided, the script generates a log file name based on the input file name and the current date and time.
    --log-level LOGLEVEL: Set the level for the log file (default: INFO). Available options are: 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'.

Author:
    Naomi Hindriks
"""

import os
import logging
import sys
import signal
import argparse
import datetime
import json
import dask.dataframe as dd
import yaml
from pathlib import Path
import subprocess as sp

# Custom scripts
from UKB_data_dict import UKB_DataDict
import convert_type
import utils

LOG_LEVELS = list(logging.getLevelNamesMapping().keys())
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_FILLEMODE = "w"

DEFAULT_DTYPE_DICT = {
    # Conversions for types (in data dict Type column)
    "Integer": "float",
    "Continuous": "float", 
    "Text": "string",
    "Date": "datetime64[ns]",
    "Time": "datetime64[ns]",
    "Compound": "string",
    "Curve": "string",
    # Conversion for categories (in data dict Ecoding_type column) if there are more than x amount of categories.
    "Real": "float",
    "ERROR": "datetime64[ns]",
    "String": "string"
}

DEFAULT_MAX_CATEGORIES = 256

DEFAULT_ENCODING = "windows-1252"


def parse_args():
    """
    Parse command-line arguments for the script.

    Returns:
        argparse.Namespace: The parsed command-line arguments.
    """
    arg_parser = argparse.ArgumentParser(
        description="Converts UK Biobank TSV files to Parquet format using Dask dataframes."
    )
    arg_parser.add_argument(
        dest = "ukb_file",
        help = "path to UK Biobank tsv file",
        type = argparse.FileType("r") 
    )

    arg_parser.add_argument(
        dest = "out_dir",
        help = "Directory path to store the parquet file(s)",
        type = Path
    )

    arg_parser.add_argument(
        dest = "data_dict",
        help = "Html file containing the UK Biobank data dictionary",
        type = argparse.FileType("r")
    )

    arg_parser.add_argument(
        "-e",
        "--encoding",
        help = "file encoding needed to read in 'ukb_file'",
        type=str,
        default=DEFAULT_ENCODING
    )

    arg_parser.add_argument(
        "-n",
        "--nrows",
        help = "Number of rows of the original data to keep to write to parquet file",
        required = False,
        type = int
    )
    
    arg_parser.add_argument(
        "-r",
        "--repartition",
        help = "Number of partitions to repartition to when reshaping the dask dataframe (repartitioning will happen after potential row splicing depending on value in nrows)",
        required = False,
        type = int
    )
    arg_parser.add_argument(
        "-m",
        "--max_categories",
        help = "Maximum levels that can be present in a categorical column where the dtype will be set to categorical.",
        type = int,
        default = DEFAULT_MAX_CATEGORIES
    )

    arg_parser.add_argument(
        "-d",
        "--dtype_dict",
        nargs='*', 
        help = "The values specified here will be added (or overwrite) the value found in the default dtype dict, shoud be specified in the format 'key=value', multiple values can be specified seperated by a whitespace"
    )
    
    arg_parser.add_argument(
        "-s",
        "--settings", 
        nargs='*', 
        help="Settings will be passed to the dasks to_parquet method, expected in the format 'named_arg=value', multiple values can be specified seperated by a whitespace"
    )
    
    arg_parser.add_argument(
        "-t",
        "--tab_offset",
        dest = "offset",
        type = int,
        help = "Number of extra tabs excpected at the start or end of each data row in the TSV file. Extra tabs will be added to the header before reading in the UK Biobank file to make sure the header and data columns are alligned correctly. The file will remain unchanged. Use a positive number to indicate the tabs at the end of a data row or a negative numbers to indicate tabs at the end of data rows.",
        default = 0,
        required = False
    )
    arg_parser.add_argument(
        "-f",
        "--force",
        dest = "force", 
        help = "",
        required = False,
        action='store_true'
    ),

    log_dir = Path.cwd() / "logs/python/convert_tsv_to_parquet"
    
    arg_parser.add_argument(
        "--log-dir", 
        dest = "logdir",
        default = log_dir,
        help = "Directory to store the logfile. The default value is the current working directory /logs/python/convert_tsv_to_parquet/"
    )
    arg_parser.add_argument(
        "--log-file-name", 
        dest = "logfile",
        help = "Name for the log file. If not provided, the script will generate a log file name based on the input file name and the current date and time in the format 'convert_tsv_to_parquet_{ukb_file}_{out_dir}_{date_time}.log', where {ukb_file} is the name of the input UK biobank tsv file (without extension), {out_dir} is the name of the directory path where output is stored and {date_time} is the current date and time in the format 'YYYY-MM-DD_HH:MM:SS'."
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
    """
    logfile_name = args.logfile

    if not logfile_name:
        logfile_name = "convert_tsv_to_parquet_{ukb_file}_{out_dir}_{date_time}.log".format(
            ukb_file = os.path.splitext(os.path.basename(args.ukb_file.name))[0],
            out_dir = os.path.splitext(os.path.basename(args.out_dir))[0],
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
    

def add_offset_tabs_to_col_names(col_names, offset):
    """
    Adds  extra column names (in the format 'Empty column <i>' where i is an index number unique to each added column name) to col_names list to compensate for extra tabs present in the data based on the given offset.

    Args:
        col_names (list): List of column names.
        offset (int): Number of column names to add to the column names list.

    Returns:
        list: Updated column names with added column names.

    Note:
        - If `offset` is a positive integer, it adds the specified number of extra column names to the start of the `col_names` list.
        - If `offset` is a negative integer, it adds the specified number of extra column names to the end of the `col_names` list.
        - If `offset` is zero, no extra column names are added.

    Returns:
        list: Updated column names with added column names.
    """
    if offset > 0:
        logging.info(f"Positive offset: adding {offset} placeholders to the start of column names.")
        col_names =  [f"Empty column {x}" for x in range(offset)] + col_names
    elif offset < 0:
        logging.info(f"Negative offset: adding {offset} placeholders to the end of column names.")
        col_names =  col_names + [f"Empty column {x}" for x in range(abs(offset))]
    else:
        logging.info(f"No offset: skipping offset step")
    return(col_names)
    

def get_types_parse_dates_vars(use_columns, data_dict, dtype_dict, max_num_categories):
    """
    Determine data types for columns to use in the dask dataframe read_table function.
    
    Args:
        use_columns (list): List of column names to be used.
        data_dict (UKB_DataDict): Instance of UKB_DataDict.
        dtype_dict (dict): Dictionary mapping UK Biobank data types to with data types to use in read_table function.
        max_num_categories (int): Maximum number of categories for categorical columns.
            If the number of unique categories in a column exceeds this limit,
            the dtype will be set according to the values of the category levels,
            rather than treating it as a categorical column.

    Returns:
        tuple: Tuple containing:
            - types (dict): A dictionary where keys are column names from `use_columns`
              and values are the corresponding data types to be used in the dask dataframe.
            - parse_dates_columns (list): A list of column names to be parsed as dates.
    """
    logging.info("Determining dtypes for columns to use in the dask dataframe read_table function")
    
    types = {
        col_name: convert_type.get_type_for_dask_dict(col_name, data_dict, dtype_dict, max_num_categories)
        for col_name in use_columns 
        if not (data_dict.main_table[
                data_dict.main_table["UDI"] == col_name
            ]["Type"].item() in ["Date", "Time"]
        )
    }
    logging.info(f"Initial types dict has length {len(types)}")


    logging.info("Finding dates/times columns that need to be parsed.")
    parse_dates_columns = [col_name for col_name in use_columns
                           if data_dict.main_table[
                                   data_dict.main_table["UDI"] == col_name
                                  ]["Type"].item() in ["Date", "Time"]
                           or types[col_name] == dtype_dict["Date"]
                           or types[col_name] == dtype_dict["Time"]
                          ]
    logging.info(f"{len(parse_dates_columns)} date/time columns found")    


    logging.info("Removing dates/times from types dicts, that could have been introduced in types list because originally stored as categoricals.")
    types = {key: value for key, value in types.items()
            if not (
                value == dtype_dict["Date"]
                or value == dtype_dict["Time"]
            )}
    logging.info(f"Resulting types dict of length {len(types)}")

    return(types, parse_dates_columns)
    

def get_dask_dataframe(tsv_file, col_names, offset, data_dict, dtype_dict, max_categories, encoding):
    """
    Load data into dask dataframe based on given parameters.

    Args:
    tsv_file (str): Path to the UK Biobank tsv file.
        col_names (list): List of column names to be used.
        offset (int): Number of offset tabs.
            If offset is a positive integer, extra columns with placeholder names
            will be added to the start of 'col_names'.
            If offset is a negative integer, extra columns will be added to the end of 'col_names'.
            If offset is zero, no additional columns will be added.
        data_dict (UKB_DataDict): Instance of UKB_DataDict, providing metadata about the columns.
        dtype_dict (dict): Dictionary mapping UK Biobank data types to data types for dask dataframe.
        max_categories (int): Maximum number of categories for categorical columns.
            If the number of unique categories in a column exceeds this limit,
            the dtype will be set according to the values of the category levels,
            rather than treating it as a categorical column.
        encoding (str): File encoding needed to read in the 'tsv_file'.

    Returns:
        dask.dataframe.core.DataFrame: Dask dataframe containing the data from the tsv_file file.
    """
    logging.info("Starting process to load data into dask dataframe.")
    col_names_with_offset = add_offset_tabs_to_col_names(col_names, offset)
        
    types, parse_dates_columns = get_types_parse_dates_vars(col_names, data_dict, dtype_dict, max_categories)
    logging.info("Succesfuly create dtypes dict and date/time list")

    logging.info(f"Calling the dask read_table function for {tsv_file}, encoding set to {encoding}")
    ddf = dd.read_table(
        urlpath = tsv_file,
        encoding = encoding,
        header = 0,
        usecols = col_names,
        names = col_names_with_offset,
        dtype = types,
        parse_dates = parse_dates_columns
    )

    # Capture and log info on loaded dataframe
    with utils.capture() as result_string:
        ddf.info()
    logging.info(f'Sucesfully loaded data as dask dataframe. Dataframe info:\n{result_string["stdout"]}')
    
    return(ddf)


def reshape_ddf(ddf, nrows, npartitions):
    """
    Reshape the dask dataframe based on nrows and npartitions.

    Args:
        ddf (dask.dataframe.core.DataFrame): Dask dataframe.
        nrows (int): Number of rows to keep from the dataframe.
            Rows from index 0 to index nrows-1 will be retained.
            If 'nrows' is None or 0, no slicing of rows will occur.
        npartitions (int): Number of partitions for the reshaped dataframe.
            If 'npartitions' is None or 0, no repartitioning will occur.
            The dataframe will be repartitioned after potential row slicing.

    Returns:
        dask.dataframe.core.DataFrame: Reshaped dask dataframe.
    """
    if nrows:
        logging.info(f"Slicing dataframe, keeping first {nrows} rows")
        try:
            ddf = ddf.loc[0:nrows-1]
            
            logging.info(f"Dataframe sliced")
        except Exception as e:
            raise TypeError("Could not slice dataframe to nrows.") from e
    
    if npartitions:
        try:
            logging.info(f'Try repartitioning dataframe to {npartitions} partitions')
            ddf = ddf.repartition(npartitions=npartitions)
            logging.info(f'Repartitioned dataframe to {npartitions} partitions')
        except Exception as e:
            raise TypeError(f"Could not repartition dataframe to {ddf.npartitions} repartions.") from e
    return ddf


def create_readme(dir_path, config, description=None):
    """
    Create a README file to add to the folder where the newly created parquet file(s) will be stored,
    used for documenting the configuration settings.

    Args:
        name (str): Name of the configuration.
        dir_path (str): Directory path.
        config (dict): Configuration dictionary containing settings and parameters used for the conversion.
        description (str, optional): Additional description or notes about the conversion process to be added to the README file.
        
    Note:
        The README file includes information about the script that was used for conversion,
        the configuration settings, and any additional notes about the process.
    """
    now = datetime.datetime.now().strftime("%Y-%m-%d (year-month-day) (%H:%M:%S)")
    # TODO add logging messgae
    logging.info("Starting to write info to README file.")
    with open(os.path.join(dir_path, "README.txt"), "w") as read_me_f:
        read_me_f.write(f"PARQUET created with {os.path.basename(__file__)}\n\n")
        read_me_f.write(f"Created on {now}\n\n")
        if description:
            read_me_f.write(f"Description:\n{description}\n\n")
        read_me_f.write("The following configuartion was used to create this directory of parquet files:\n")
        read_me_f.write(yaml.dump(config))
        logging.info("Done writing to README file.")


def write_to_parquet(ddf, out_path, settings, force):
    """
    Write the Dask dataframe to Parquet format.

    Args:
        ddf (dask.dataframe.core.DataFrame): Dask dataframe containing the data to be written.
        out_path (str): Directory path where the Parquet file(s) will be stored.
        settings (dict): A dictionary contianing kwargs to be passed to the Dask `to_parquet` method.
        force (bool): Flag to force the operation if the specified directory is not empty.
            If True, existing files in the directory may be overwritten or modified, depending kwargs in settings.

    Note:
        The `to_parquet` method is used to write the Dask dataframe to Parquet format.
        The 'force' flag allows overwriting existing files if the specified directory is not empty.

    Raises:
        OSError: If the specified directory is not empty, and the 'force' flag is not set.

    Returns:
        None
    """
    # If not empty and not force -> abort
    dir_exists = os.path.exists(out_path)
    if dir_exists:
        dir_content = os.listdir(out_path)
        if dir_content and not force:
            raise OSError(f"Directory ({out_path}) not empty. If you wish to use directory anyway use the --force flag (be aware that files might be overwritten).")
        elif dir_content: 
            logging.info("Directory not empty, forcing operation, files might be overwritten or edited (depending on the configuration settings)")

    logging.info(f"Trying to write dataframe to parquet format with following settings:\n{settings}")
    ddf.to_parquet(
        out_path,
        **settings
    )

    logging.info(f"Done writing dataframe to parquet format with following settings:\n{settings}")


def get_column_names(tsv_file, data_dict):
    """
    Retrieve column names from the input TSV file.

    Args:
        tsv_file (str): Path to the UK Biobank tsv file.
        data_dict (UKB_DataDict): Instance of UKB_DataDict.

    Returns:
        list: List of column names.
    """
    logging.info(f"Trying to retrieve column names from the input tsv file with path ({tsv_file})")
    # try:
    head_process = sp.run(["head", "-n1", tsv_file], capture_output=True)
    column_names = head_process.stdout.decode().strip().split("\t")

    for i, column_name in enumerate(column_names):
        data_dict_for_column = data_dict.main_table[
                    data_dict.main_table["UDI"] == column_name
                ]
        len_data_dict_for_column = len(data_dict_for_column.index)

        if len_data_dict_for_column < 1:
            raise ValueError(f"Column {i}, with column name: \"{repr(column_name)}\" can not be found in the given data dictionary ({data_dict.path_to_html}). Please make sure the data dictionary matches the given UK Biobank file.")
        elif len_data_dict_for_column > 1:
            raise ValueError(f"Column {i}, with column name: \"{repr(column_name)}\" shows up {len_data_dict_for_column} times in column names, please make sure to give a valid data dictionary file with a single row for every column found in the UK Biobank file.")
            
    logging.info(f"Retrieved column names, {len(column_names)} column names found.")

    return(column_names)


def convert_to_parquet(tsv_file_in, out_dir, data_dict, column_names, nrows=0, npartitions=0, dtype_dict=DEFAULT_DTYPE_DICT, settings={}, offset=0, force=False, max_categories=DEFAULT_MAX_CATEGORIES, encoding=DEFAULT_ENCODING):
    """
    Convert the UK Biobank data from a TSV file to Parquet format with user-defined conget_missing_types_in_dtype_dictfigurations.

    Args:
        tsv_file_in (str): Path to the UK Biobank TSV file.
        out_dir (str): Directory path to store the Parquet file(s).
        data_dict (UKB_DataDict): Instance of the UKB_DataDict, representing the data dictionary in HTML format.
        column_names (list): List of column names extracted from the TSV file.
        nrows (int, optional): Number of rows to keep from the original data for writing get_missing_types_in_dtype_dicto the Parquet file. If None or 0 it will keep all rows. Defaults to 0.
        npartitions (int, optional): Number of partitions to repartition the Dask dataframe. If None or 0 no repartitioning will take place. Defaults to 0.
        dtype_dict (dict, optional): Dictionary mapping UK Biobank data types to corresponding Dask data types (dtypes). Defaults to DEFAULT_DTYPE_DICT.
        settings (dict, optional): Additional settings to pass to the Dask to_parquet method. Defaults to an empty dictionary.
        offset (int, optional): Number of extra tabs expected at the start or end of each data row in the TSV file. Defaults to 0.
        force (bool, optional): Flag to force the operation if the output directory is not empty. Defaults to False.
        max_categories (int, optional): Maximum levels that can be present in a categorical column where the dtype will be set to categorical. Defaults to DEFAULT_MAX_CATEGORIES.
        encoding (str, optional): File encoding needed to read the TSV file. Defaults to DEFAULT_ENCODING ().

    Note:
        - The data dictionary HTML file can be created using the `ukbconv` program with the 'docs' option. This program is available for download from the UK Biobank website, and its usage is explained in the Data Access Guide (https://biobank.ndph.ox.ac.uk/showcase/exinfo.cgi?src=AccessingData).
        - If the output directory is not empty and the force flag is not set, the script will raise an OSError.

    Returns:
        None
    """

    ddf = get_dask_dataframe(
        tsv_file = tsv_file_in,
        col_names = column_names,
        offset = offset,
        data_dict = data_dict,
        dtype_dict = dtype_dict,
        max_categories = max_categories,
        encoding = encoding
    )


    ddf = reshape_ddf(ddf, nrows=nrows, npartitions=npartitions)

    write_to_parquet(
        ddf = ddf,
        out_path=out_dir,
        settings=settings,
        force = force
    )
    
    create_readme(
        out_dir,
        config = {
            "ukb_file": str(tsv_file_in),
            "out_dir": str(out_dir),
            "data_dict": data_dict.path_to_html,
            "encoding": encoding,
            "nrows": nrows,
            "repartition": npartitions,
            "max_categories": max_categories,
            "tab_offset": offset,
            "force": force,
            
            "dtype_dict": dtype_dict,
            "settings": settings
        }
    )
    

def data_dict_unique_types(data_dict):
    """
    Retrieve the unique data types present in the UK Biobank data dictionary.

    Args:
        data_dict (UKB_DataDict): Instance of the UKB_DataDict, representing the data dictionary in HTML format.

    Note:
        This function is designed to identify the data types that appear in the data dictionary. The resulting set of data types should be considered when configuring the dtype_dict for conversion, ensuring that all necessary mappings are provided.

    Returns:
        set: A set containing unique data types found in the data dictionary.
    """
    ukb_types_in_data_dict = data_dict.main_table["Type"].unique().tolist()
    ukb_encodingtypes_in_data_dict = data_dict.main_table[
        ~data_dict.main_table["Encoding_type"].isna()
    ]["Encoding_type"].unique().tolist()
    needed_types = set(ukb_types_in_data_dict + ukb_encodingtypes_in_data_dict)
    return needed_types


def add_dtype_arg_to_dict(dtype_arg, dtype_dict):
    """
    Add or update a key-value pair in the dtype_dict based on the provided dtype argument.

    Args:
        dtype_arg (str): The dtype argument in the format 'key=value'.
        dtype_dict (dict): The dictionary mapping UK Biobank data types to desired data types.

    Raises:
        ValueError: If the dtype_arg format is incorrect.

    Note:
        This function modifies the given dtype_dict by adding or updating a key-value pair based on the dtype_arg.
        If the specified key already exists in dtype_dict, its value will be overwritten with the new value.


    Returns:
        None
    """
    split_arg = dtype_arg.split("=")
    if not len(split_arg) == 2:
        raise ValueError(f"Could not split value ({dtype_arg}) given for dtype_dict by '='. Make sure to give dtype options in the format 'key=value', and if multiple options are given seperate them by whitespace.")

    key, value = split_arg[0], split_arg[1]

    if key in dtype_dict.keys():
        logging.info(f"Overwriting default dtype value for {key}")

    dtype_dict[key] = value
    logging.info(f"dtype_dict[{key}] set to {value}")


def get_needed_types(unique_types):
    needed_types = [type for type in unique_types if not (type.startswith("Categorical") or type == "Sequence")]
    return needed_types

def get_missing_types_in_dtype_dict(dtype_dict, data_dict):
    unique_types = data_dict_unique_types(data_dict)
    needed_types = get_needed_types(unique_types)
    types_not_present = set()

    for type in needed_types:
        if not type in dtype_dict.keys():
            types_not_present.add(type)

    return(types_not_present)


def parse_dtype_dict_args(dtype_args, data_dict):
    """
    Parse dtype arguments and create a dictionary mapping UK Biobank data types to desired dtype.

    Args:
        dtype_args (list): List of dtype arguments in the format 'key=value'.
        data_dict (UKB_DataDict): An instance of the UKB_DataDict class containing information about the data dictionary.

    Raises:
        ValueError: If duplicated dtypes are found in the given dtype_args.
    
    Returns:
        dict: Dictionary mapping data types to desired data types.
    """
    dtype_dict = DEFAULT_DTYPE_DICT

    dype_keys = [arg.split("=")[0] for arg in dtype_args]
    dtype_key_set = set(dype_keys)
    if not len(dype_keys) == len(dtype_key_set):
        raise ValueError("Duplicated dtypes found in given dtype_dict, make sure to only specify each dtype once.")
    
    for dtype_arg in dtype_args:
        add_dtype_arg_to_dict(dtype_arg, dtype_dict)

    # TODO Add to logging message
    logging.info("Using following dtype dict to convert data types")
    return(dtype_dict)


def parse_settings_args(settings_args):
    """
    Parse settings arguments and create a dictionary.

    Args:
        settings_args (list): List of settings arguments in the format 'key=value'.

    Raises:
        ValueError: If the settings option format is incorrect. This occurs when:
            - The settings option is not in the format 'key=value'.
            - Multiple settings options are given, but they are not separated by whitespace.
        
    Returns:
        dict: Dictionary containing parsed settings.
    """
    settings = {}
    if settings_args:
        for settings_arg in settings_args:
            split_arg = settings_arg.split("=")
            if not len(split_arg) == 2:
                raise ValueError(f"Could not split value the settings option ({settings_arg}) by '='. Make sure to give settings options in the format 'key=value', and if multiple options are given seperate them by whitespace.")
            settings[split_arg[0]] = split_arg[1]
    return settings


def main(args):
    """
    Main function to convert UK Biobank TSV file to Parquet format based on the given configuration.

    Args:
        args: Command-line arguments parsed by argparse.
        
    """
    ukb_file_name = args.ukb_file.name
    data_dict_name = args.data_dict.name
    
    logging.info(f"Starting main function to convert {ukb_file_name} to {ukb_file_name} parquet format.")

    # check if ukb_file file exists
    if not (os.path.isfile(ukb_file_name) and os.access(ukb_file_name, os.R_OK)):
        raise IOError(f"File {ukb_file_name} doesn't exist or isn't readable")

    if not (os.path.isfile(data_dict_name) and os.access(data_dict_name, os.R_OK)):
       raise IOError(f"File {data_dict_name} doesn't exist or isn't readable") 

    data_dict = UKB_DataDict(data_dict_name)
    try:
        logging.info(f"Trying to retrieve main table from the data dictionary ({data_dict_name})")
        data_dict.main_table
        logging.info(f"Done retrieving main table from the data dictionary ({data_dict_name})")
    except Exception as e:
        raise ValueError(f"Could not retrieve main table from data dictionary file ({data_dict_name})") from e
    column_names = get_column_names(ukb_file_name, data_dict)

    if args.dtype_dict:
        dtype_dict = parse_dtype_dict_args(args.dtype_dict, data_dict)
    else:
        dtype_dict = DEFAULT_DTYPE_DICT

    missing_types = get_missing_types_in_dtype_dict(dtype_dict, data_dict)
    if missing_types:
        raise ValueError(f"Some types found in data_dict are not present in dtype_dict, types not found are: {missing_types}")
    
    settings = parse_settings_args(args.settings)
    print(f"settings: {settings}")
    
    convert_to_parquet(
        tsv_file_in = ukb_file_name,
        out_dir = args.out_dir, 
        data_dict = data_dict, 
        dtype_dict = dtype_dict,
        settings=settings,
        column_names = column_names, 
        nrows =args.nrows,
        npartitions = args.repartition,
        offset = args.offset, 
        force = args.force,
        max_categories = args.max_categories,
        encoding = args.encoding
    )


if __name__ == "__main__":
    # Function to run when program termination signal is recieved (for example process is killed because of slurm time contraint),it will write a line to logfile
    signal.signal(signal.SIGTERM, utils.terminate_signal_handler)

    # Parse arguments
    args = parse_args()

    # Set log file
    set_logfile(args)

    logging.info("Parsed following args: {args}".format(
        args = ", ".join([f"\n\t{arg}: {getattr(args, arg)}" for arg in vars(args)])
    ))

    try:
        main(args)
    except Exception as e:
        utils.exit_script(
            f"An exception occured during the execution of the {__file__} script.", 
            log_function=logging.exception, 
            status=1, 
            exit_message=f"An exception while converting {args.ukb_file} to {args.out_dir} parquet file, see {logging.getLoggerClass().root.handlers[0].baseFilename}"
        )

    logging.info("Script done running, exiting...")
    sys.exit(0)