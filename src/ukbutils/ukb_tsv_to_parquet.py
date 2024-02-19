#!/usr/bin/env python
"""
Converts UK Biobank TSV files to Parquet format:
Reads a UK Biobank TSV file along with its data dictionary (an HTML file,
which can be created using the `ukbconv` program with the option `docs`),
performs data type conversions, and writes the data to Parquet files.

TODO:
    1) Change script to enable utilizing distributed computing on a Slurm
        cluster using Dask:
            - Use Dask Distributed
                - Allow for Localcluster with given amount of workers or
                SLURMCluster (dask jobqueue -> https://jobqueue.dask.org/en/latest/generated/dask_jobqueue.SLURMCluster.html)   # noqa: E501
            - Find good way to comunicate url where dask dashboard can be found
                - Or let user give options for this
            - Modify Dask DataFrame operations for distributed computing.
            - Add cli options to specify amount of workers and cluster resources
            - Let user specify cluster options or give address of running cluster?
    2) Allow specifying column names as input and convert only those columns
        to parquet.
    3) Improve the README created in the convertion process so the config part
        will be more readable
    4) There is code for generation of pyarrow schema based on the UK Biobank 
        datatypes,but this code is not used at the moment. Incoorparate using 
        pyarrow schema when writing to parquet, give user the option to use specify 
        schema in cli

Usage:
    python ukb_tsv_to_parquet.py \
        <ukb_file> <out_dir> <data_dict> \
        [-e ENCODING] \
        [-n NROWS] \
        [-r REPARTITION] \
        [-m MAX_CATEGORIES] \
        [-dt DTYPE_DICT_TYPE ...] \
        [-de DTYPE_DICT_ENCODING ...] \
        [-s SETTINGS ...] \
        [-t TAB_OFFSET] \
        [-f] \
        [--log-dir LOGDIR] \
        [--log-file-name LOGFILE] \
        [--log-level LOGLEVEL]

Arguments:
    <ukb_file>: Path to the UK Biobank TSV file.
    <out_dir>: Directory path to store the Parquet file(s).
    <data_dict>: HTML file containing the UK Biobank data dictionary. (Note:
        It can be created using the `ukbconv` program with the option `docs`.)

Options:
    -e, --encoding ENCODING: File encoding needed to read the UK Biobank
        TSV file (default: windows-1252).
    -n, --nrows NROWS: Number of rows of the original data to keep when writing
        to Parquet file.
    -r, --repartition REPARTITION: Number of partitions to repartition the
        Dask dataframe (after potential row splicing depending on value in
        nrows).
    -m, --max_categories MAX_CATEGORIES: Maximum number of levels that can
        be present in a categorical column where the dtype will be set to
        categorical (if the number of categories in a column exceeds the
        specified 'max_categories' threshold, the dtype for that column will
        be set to the default dtype for that data type, and the column will
        no longer be treated as categorical). (default: 256).
    -dt, --dtype_dict DTYPE_DICT_TYPE: Specify dtype "Type" options in the
        format 'key=value'. Multiple options can be specified, separated by
        whitespace.
    -de, --dtype_dict_encoding DTYPE_DICT_ENCODING: Specify dtype
        "Encoding_type" options in the format 'key=value'. Multiple options
        can be specified, separated by whitespace.
    -s, --settings SETTINGS: Settings passed to the Dask to_parquet method
        in the format 'named_arg=value'. Multiple options can be specified,
        separated by whitespace.
    -t, --tab_offset TAB_OFFSET: Number of extra tabs expected at the start
        or end of each data row in the TSV file. Use a positive number to
        indicate tabs at the end of a data row or a negative number to
        indicate tabs at the start of data rows (default: 0).
    -f, --force: Force the operation, overwriting existing files if needed.
    --log-dir LOGDIR: Directory to store the logfile (default: current working
        directory/logs/python/ukb_tsv_to_parquet).
    --log-file-name LOGFILE: Name for the log file. If not provided, the
        script generates a log file name based on the input file name and
        the current date and time.
    --log-level LOGLEVEL: Set the level for the log file (default: INFO).
        Available options are: 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'.

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
import yaml
from pathlib import Path
import subprocess as sp
import pyarrow as pa

import dask
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster, get_client


# Custom scripts
from ukbutils.UKB_data_dict import UKB_DataDict
import ukbutils.utils as utils
import ukbutils.get_translation_dict as gtd

LOG_LEVELS = list(logging.getLevelNamesMapping().keys())
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_FILLEMODE = "w"

DEFAULT_DTYPE_DICT = {
    # Conversions for types (in data dict Type column)
    "Type": {
        "Sequence": "int",
        "Integer": "float",
        "Continuous": "float",
        "Text": "string",
        "Date": ["Date", "%Y-%m-%d"],
        "Time": ["Date", "%Y-%m-%d %H:%M:%S"],
        "Compound": "string",
        "Curve": "string",
    },
    # Conversion for categories (in data dict Ecoding_type column)
    # if there are more than x amount of categories.
    "Encoding_type": {
        "Integer": "float",
        "Real": "float",
        "ERROR": ["Date", "%Y-%m-%d"],
        "String": "string",
    },
}


DEFAULT_PA_SCHEMA_TYPE = {
    # Conversions for types (in data dict Type column)
    "Type": {
        "Sequence": pa.int64(),
        "Integer": pa.int64(),
        "Continuous": pa.float64(),
        "Text": pa.string(),
        "Date": pa.date64(),
        "Time": pa.timestamp("s"),
        "Compound": pa.string(),
        "Curve": pa.string(),
    },
    # Conversion for categories (in data dict Ecoding_type column)
    # if there are more than x amount of categories.
    "Encoding_type": {
        "Integer": pa.int64(),
        "Real": pa.float64(),
        "ERROR": pa.timestamp("s"),
        "String": pa.string(),
    },
}


DEFAULT_MAX_CATEGORIES = 256
DEFAULT_CATEGORICAL_COLUMNS = ["Categorical (single)", "Categorical (multiple)"]

DEFAULT_ENCODING = "windows-1252"

VALID_DTYPE_KEYWORDS = ["date"]
# EXCEEDING_MAX_CAT_KEYWORD = "EXCEEDING MAX LEVELS OF CATEGORY"


def parse_args():
    """
    Parse command-line arguments for the script.

    Returns:
        argparse.Namespace: The parsed command-line arguments.
    """
    arg_parser = argparse.ArgumentParser(
        description=(
            "Converts UK Biobank TSV files to Parquet format using" " Dask dataframes."
        )
    )
    arg_parser.add_argument(
        dest="ukb_file", help="path to UK Biobank tsv file", type=argparse.FileType("r")
    )

    arg_parser.add_argument(
        dest="out_dir", help="Directory path to store the parquet file(s)", type=Path
    )

    arg_parser.add_argument(
        dest="data_dict",
        help="Html file containing the UK Biobank data dictionary",
        type=argparse.FileType("r"),
    )

    arg_parser.add_argument(
        "-e",
        "--encoding",
        help="file encoding needed to read in 'ukb_file'",
        type=str,
        default=DEFAULT_ENCODING,
    )

    arg_parser.add_argument(
        "-n",
        "--nrows",
        help=(
            "Number of rows of the original data to keep to write to" " parquet file"
        ),
        required=False,
        type=int,
    )

    arg_parser.add_argument(
        "-r",
        "--repartition",
        help=(
            "Number of partitions to repartition to when reshaping the dask"
            " dataframe (repartitioning will happen after potential row"
            " splicing depending on value in nrows)"
        ),
        required=False,
        type=int,
    )

    arg_parser.add_argument(
        "-m",
        "--max_categories",
        help=(
            "Maximum levels that can be present in a categorical column"
            " where the dtype will be set to categorical."
        ),
        type=int,
        default=DEFAULT_MAX_CATEGORIES,
    )
    arg_parser.add_argument(
        "-c",
        "--categorical-type",
        nargs="+",
        dest="categorical_cols",
        help="Columns to treat as categoricals.",
        default=DEFAULT_CATEGORICAL_COLUMNS,
    )

    arg_parser.add_argument(
        "-dt",
        "--dtype_dict_type",
        nargs="*",
        help=(
            "The values specified here will be added (or overwrite) the"
            " value found in the default dtype dict, shoud be specified in"
            " the format 'key=value', multiple values can be specified"
            " seperated by a whitespace"
        ),
    )

    arg_parser.add_argument(
        "-de",
        "--dtype_dict_encoding_type",
        nargs="*",
        help=(
            "The values specified here will be added (or overwrite) the"
            " value found in the default dtype dict, shoud be specified in"
            " the format 'key=value', multiple values can be specified"
            " seperated by a whitespace"
        ),
    )

    arg_parser.add_argument(
        "-s",
        "--settings",
        nargs="*",
        help=(
            "Settings will be passed to the dasks to_parquet method, expected"
            " in the format 'named_arg=value', multiple values can be specified"
            " seperated by a whitespace"
        ),
    )

    arg_parser.add_argument(
        "-t",
        "--tab_offset",
        dest="offset",
        type=int,
        help=(
            "Number of extra tabs excpected at the start or end of each"
            " data row in the TSV file. Extra tabs will be added to the"
            " header before reading in the UK Biobank file to make sure"
            " the header and data columns are alligned correctly. The file"
            " will remain unchanged. Use a positive number to indicate the"
            " tabs at the end of a data row or a negative numbers to indicate"
            " tabs at the end of data rows."
        ),
        default=0,
        required=False,
    )
    # TODO ADD HELP MESSAGE
    arg_parser.add_argument(
        "-f", "--force", dest="force", help="", required=False, action="store_true"
    ),

    log_dir = Path.cwd() / "logs/python/ukb_tsv_to_parquet"

    arg_parser.add_argument(
        "--log-dir",
        dest="logdir",
        default=log_dir,
        help=(
            "Directory to store the logfile. The default value is the"
            " current working directory /logs/python/ukb_tsv_to_parquet/"
        ),
    )
    arg_parser.add_argument(
        "--log-file-name",
        dest="logfile",
        help=(
            "Name for the log file. If not provided, the script will"
            " generate a log file name based on the input file name and the"
            " current date and time in the format"
            " 'ukb_tsv_to_parquet_{ukb_file}_{out_dir}_{date_time}.log',"
            " where {ukb_file} is the name of the input UK biobank tsv file"
            " (without extension), {out_dir} is the name of the directory"
            " path where output is stored and {date_time} is the current"
            " date and time in the format 'YYYY-MM-DD_HH:MM:SS'."
        ),
    )
    arg_parser.add_argument(
        "--log-level",
        choices=LOG_LEVELS,
        dest="loglevel",
        default="INFO",
        help=(
            "Set the level for the log file. Available options are:"
            " 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'. The default"
            " level is INFO."
        ),
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
        logfile_name = "ukb_tsv_to_parquet_{ukb_file}_{out_dir}_{date_time}.log".format(
            ukb_file=os.path.splitext(os.path.basename(args.ukb_file.name))[0],
            out_dir=os.path.splitext(os.path.basename(args.out_dir))[0],
            date_time=datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S"),
        )

    rel_dir_name = os.path.relpath(args.logdir)
    if not os.path.exists(rel_dir_name):
        os.makedirs(rel_dir_name)

    rel_log_file_path = os.path.join(rel_dir_name, logfile_name)

    # set config for logging
    try:
        logging.basicConfig(
            filename=rel_log_file_path,
            level=args.loglevel,
            format=LOG_FORMAT,
            filemode=LOG_FILLEMODE,
        )
        logging.info(
            f"Done setting up logging: log filepath = {logfile_name}, and log"
            f" level is set to {args.loglevel}."
        )
    except Exception as e:
        raise ValueError("Not able to create logfile") from e


def add_offset_tabs_to_col_names(col_names, offset):
    """
    Adds  extra column names (in the format 'Empty column <i>' where i is an
    index number unique to each added column name) to col_names list to compensate
    for extra tabs present in the data based on the given offset.

    Args:
        col_names (list): List of column names.
        offset (int): Number of column names to add to the column names list.

    Returns:
        list: Updated column names with added column names.

    Note:
        - If `offset` is a positive integer, it adds the specified number
            of extra column names to the start of the `col_names` list.
        - If `offset` is a negative integer, it adds the specified number
            of extra column names to the end of the `col_names` list.
        - If `offset` is zero, no extra column names are added.

    Returns:
        list: Updated column names with added column names.
    """
    if offset > 0:
        logging.info(
            f"Positive offset: adding {offset} placeholders to the"
            "start of column names."
        )
        col_names = [f"Empty column {x}" for x in range(offset)] + col_names
    elif offset < 0:
        logging.info(
            f"Negative offset: adding {offset} placeholders to the"
            "end of column names."
        )
        col_names = col_names + [f"Empty column {x}" for x in range(abs(offset))]
    else:
        logging.info("No offset: skipping offset step")
    return col_names


def get_dask_dataframe(
    tsv_file,
    col_names,
    offset,
    data_dict,
    dtype_dict,
    cat_cols,
    max_categories,
    encoding,
):
    """
    Load data into dask dataframe based on given parameters.

    Args:
    tsv_file (str): Path to the UK Biobank tsv file.
        col_names (list): List of column names to be used.
        offset (int): Number of offset tabs.
            If offset is a positive integer, extra columns with placeholder
            names will be added to the start of 'col_names'.
            If offset is a negative integer, extra columns will be added to
                the end of 'col_names'. If offset is zero, no additional
                columns will be added.
        data_dict (UKB_DataDict): Instance of UKB_DataDict, providing metadata
            about the columns.
        dtype_dict (dict): Dictionary mapping UK Biobank data types to data
            types for dask dataframe.
        max_categories (int): Maximum number of categories for categorical columns.
            If the number of unique categories in a column exceeds this limit,
            the dtype will be set according to the values of the category levels,
            rather than treating it as a categorical column.
        encoding (str): File encoding needed to read in the 'tsv_file'.

    Returns:
        dask.dataframe.core.DataFrame: Dask dataframe containing the data
        from the tsv_file file.
    """
    logging.info("Starting process to load data into dask dataframe.")
    col_names_with_offset = add_offset_tabs_to_col_names(col_names, offset)

    types = gtd.get_d_type_dict(
        col_names, data_dict, dtype_dict, cat_cols, max_categories
    )
    logging.info("Succesfuly create dtypes dict")

    date_format_dict = gtd.get_date_format_dict(
        col_names, data_dict, dtype_dict, max_categories
    )

    date_col_type_dict = {date_col: "string" for date_col in date_format_dict.keys()}
    types.update(date_col_type_dict)

    logging.info(
        f"Calling the dask read_table function for {tsv_file},"
        f" encoding set to {encoding}"
    )
    ddf = dd.read_table(
        urlpath=tsv_file,
        encoding=encoding,
        header=0,
        usecols=col_names,
        names=col_names_with_offset,
        dtype=types,
        # parse_dates=list(date_format_dict.keys()),
        # date_format=date_format_dict
    )

    for date_col in date_col_type_dict:
        logging.debug(
            f"Parsing column {date_col} to datetime format"
            f"{date_format_dict[date_col]}"
        )
        ddf[date_col] = dd.to_datetime(ddf[date_col], format=date_format_dict[date_col])

    # Capture and log info on loaded dataframe
    with utils.capture() as result_string:
        ddf.info()
    logging.info(
        "Sucesfully loaded data as dask dataframe. Dataframe"
        f" info:\n{result_string['stdout']}"
    )

    return ddf


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
            ddf = ddf.loc[0 : nrows - 1]  # noqa: E203

            logging.info("Dataframe sliced")
        except Exception as e:
            raise TypeError("Could not slice dataframe to nrows.") from e

    if npartitions:
        try:
            logging.info(f"Try repartitioning dataframe to {npartitions}" " partitions")
            ddf = ddf.repartition(npartitions=npartitions)
            logging.info(f"Repartitioned dataframe to {npartitions} partitions")
        except Exception as e:
            raise TypeError(
                "Could not repartition dataframe to" f" {ddf.npartitions} repartions."
            ) from e
    return ddf


def create_readme(dir_path, config, description=None):
    """
    Create a README file to add to the folder where the newly created parquet
    file(s) will be stored, used for documenting the configuration settings.

    Args:
        name (str): Name of the configuration.
        dir_path (str): Directory path.
        config (dict): Configuration dictionary containing settings and
            parameters used for the conversion.
        description (str, optional): Additional description or notes about
            the conversion process to be added to the README file.

    Note:
        The README file includes information about the script that was used
        for conversion, the configuration settings, and any additional notes
        about the process.
    """
    now = datetime.datetime.now().strftime("%Y-%m-%d (year-month-day) (%H:%M:%S)")
    # TODO add logging messgae

    now = datetime.datetime.now().strftime("%Y-%m-%d (year-month-day) (%H:%M:%S)")
    readme_path = os.path.join(dir_path, "README.txt")

    try:
        logging.info("Starting to write info to README file.")

        with open(os.path.join(dir_path, "README.txt"), "w") as read_me_f:
            read_me_f.write(f"PARQUET created with {os.path.basename(__file__)}\n\n")
            read_me_f.write(f" Created on {now}\n\n")
            if description:
                read_me_f.write(f"Description:\n{description}\n\n")
            read_me_f.write(
                "The following configuartion was used to create"
                " this directory of parquet files:\n"
            )
            read_me_f.write(yaml.dump(config))

        logging.info(f"README file written successfully at: {readme_path}")

    except Exception as e:
        logging.error(f"Error writing README file: {str(e)}")


def write_to_parquet(ddf, out_path, settings, force):
    """
    Write the Dask dataframe to Parquet format.

    Args:
        ddf (dask.dataframe.core.DataFrame): Dask dataframe containing the
            data to be written.
        out_path (str): Directory path where the Parquet file(s) will be stored.
        settings (dict): A dictionary contianing kwargs to be passed to the
            Dask `to_parquet` method.
        force (bool): Flag to force the operation if the specified directory
            is not empty. If True, existing files in the directory may be
            overwritten or modified, depending kwargs in settings.

    Note:
        The `to_parquet` method is used to write the Dask dataframe to
            Parquet format.
        The 'force' flag allows overwriting existing files if the specified
            directory is not empty.

    Raises:
        OSError: If the specified directory is not empty, and the 'force'
        flag is not set.

    Returns:
        None
    """
    # If not empty and not force -> abort
    dir_exists = os.path.exists(out_path)
    if dir_exists:
        dir_content = os.listdir(out_path)
        if dir_content and not force:
            raise OSError(
                f"Directory ({out_path}) not empty. If you wish to use"
                " directory anyway use the --force flag (be aware that files"
                " might be overwritten)."
            )
        elif dir_content:
            logging.info(
                "Directory not empty, forcing operation, files might be"
                "overwritten or edited (depending on the configuration settings)"
            )

    logging.info(
        "Trying to write dataframe to parquet format with following"
        f" settings:\n{settings}"
    )
    ddf.to_parquet(out_path, **settings)

    logging.info(
        "Done writing dataframe to parquet format with following"
        f" settings:\n{settings}"
    )


def get_column_names(tsv_file, data_dict):
    """
    Retrieve column names from the input TSV file.

    Args:
        tsv_file (str): Path to the UK Biobank tsv file.
        data_dict (UKB_DataDict): Instance of UKB_DataDict.

    Returns:
        list: List of column names.
    """
    logging.info(
        "Trying to retrieve column names from the input tsv file with path"
        f" ({tsv_file})"
    )

    head_process = sp.run(["head", "-n1", tsv_file], capture_output=True)
    column_names = head_process.stdout.decode().strip().split("\t")

    for i, column_name in enumerate(column_names):
        data_dict_for_column = data_dict.main_table[
            data_dict.main_table["UDI"] == column_name
        ]
        len_data_dict_for_column = len(data_dict_for_column.index)

        if len_data_dict_for_column < 1:
            raise ValueError(
                f'Column {i}, with column name: "{repr(column_name)}" can not'
                " be found in the given data dictionary ({data_dict.path_to_html})."
                " Please make sure the data dictionary matches the given"
                " UK Biobank file."
            )
        elif len_data_dict_for_column > 1:
            raise ValueError(
                f'Column {i}, with column name: "{repr(column_name)}" shows'
                f" up {len_data_dict_for_column} times in column names, please"
                " make sure to give a valid data dictionary file with a single"
                " row for every column found in the UK Biobank file."
            )

    logging.info(f"Retrieved column names, {len(column_names)} column names found.")

    return column_names


def convert_to_parquet(
    tsv_file_in,
    out_dir,
    data_dict,
    column_names,
    nrows=0,
    npartitions=0,
    dtype_dict=DEFAULT_DTYPE_DICT,
    settings={},
    offset=0,
    force=False,
    cat_cols=DEFAULT_CATEGORICAL_COLUMNS,
    max_categories=DEFAULT_MAX_CATEGORIES,
    encoding=DEFAULT_ENCODING,
):
    """
    Convert the UK Biobank data from a TSV file to Parquet format with
    user-defined conget_missing_types_in_dtype_dictfigurations.

    Args:
        tsv_file_in (str): Path to the UK Biobank TSV file.
        out_dir (str): Directory path to store the Parquet file(s).
        data_dict (UKB_DataDict): Instance of the UKB_DataDict, representing
            the data dictionary in HTML format.
        column_names (list): List of column names extracted from the TSV file.
        nrows (int, optional): Number of rows to keep from the original data
            for writing get_missing_types_in_dtype_dicto the Parquet file.
            If None or 0 it will keep all rows. Defaults to 0.
        npartitions (int, optional): Number of partitions to repartition the
            Dask dataframe. If None or 0 no repartitioning will take place.
            Defaults to 0.
        dtype_dict (dict, optional): Dictionary mapping UK Biobank data types
            to corresponding Dask data types (dtypes).
            Defaults to DEFAULT_DTYPE_DICT.
        settings (dict, optional): Additional settings to pass to the Dask
            to_parquet method. Defaults to an empty dictionary.
        offset (int, optional): Number of extra tabs expected at the start
            or end of each data row in the TSV file. Defaults to 0.
        force (bool, optional): Flag to force the operation if the output
            directory is not empty. Defaults to False.
        max_categories (int, optional): Maximum levels that can be present
            in a categorical column where the dtype will be set to categorical.
            Defaults to DEFAULT_MAX_CATEGORIES.
        encoding (str, optional): File encoding needed to read the TSV file.
            Defaults to DEFAULT_ENCODING ().

    Note:
        - The data dictionary HTML file can be created using the `ukbconv`
            program with the 'docs' option. This program is available for
            download from the UK Biobank website, and its usage is explained
            in the Data Access Guide
            (https://biobank.ndph.ox.ac.uk/showcase/exinfo.cgi?src=AccessingData).
        - If the output directory is not empty and the force flag is not set,
            the script will raise an OSError.

    Returns:
        None
    """
    ddf = get_dask_dataframe(
        tsv_file=tsv_file_in,
        col_names=column_names,
        offset=offset,
        data_dict=data_dict,
        dtype_dict=dtype_dict,
        cat_cols=cat_cols,
        max_categories=max_categories,
        encoding=encoding,
    )

    ddf = reshape_ddf(ddf, nrows=nrows, npartitions=npartitions)

    # schema = gtd.get_pa_schema(
    #     use_columns=column_names,
    #     data_dict=data_dict,
    #     pa_type_dict=DEFAULT_PA_SCHEMA_TYPE,
    #     cat_cols=cat_cols,
    #     max_num_categories=max_categories,
    # )
    # logging.info(schema)

    write_to_parquet(ddf=ddf, out_path=out_dir, settings=settings, force=force)

    create_readme(
        out_dir,
        config={
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
            "settings": settings,
        },
    )


def set_dtype_in_dtype_dict(dtype_dict, key, dtype_val):
    """
    Set the value for the specified key in the dtype_dict, overwriting the
    existing value if present.

    Args:
        dtype_dict (dict): Dictionary mapping data types to desired data types.
        key (str): Key for which the value needs to be set or overwritten.
        dtype_val (str or list): Desired data type value to be associated
            with the key.

    Returns:
        None
    """
    if key in dtype_dict.keys():
        logging.info(f"Overwriting default dtype value for {key}")

    dtype_dict[key] = dtype_val
    logging.info(f"dtype_dict[{key}] set to {dtype_val}")


def add_dtype_arg_to_dict(dtype_arg, dtype_dict):
    """
    Add or update a key-value pair in the dtype_dict based on the provided
    dtype argument.

    Args:
        dtype_arg (str): The dtype argument in the format 'key=value'.
        dtype_dict (dict): The dictionary mapping UK Biobank data types to
            desired data types.

    Raises:
        ValueError: If the dtype_arg format is incorrect.

    Note:
        This function modifies the given dtype_dict by adding or updating a
        key-value pair based on the dtype_arg. If the specified key already
        exists in dtype_dict, its value will be overwritten with the new value.

    Returns:
        None
    """
    split_arg = dtype_arg.split("=")
    if not len(split_arg) == 2:
        raise ValueError(f"Invalid dtype argument format: {dtype_arg}")

    key, value = split_arg[0], split_arg[1]

    if value.startswith("[") and value.endswith("]"):
        try:
            arg_list = json.loads(value)
        except Exception as e:
            raise ValueError(f"Failed to parse value as JSON list: {value}") from e

        if len(arg_list) == 2 and arg_list[0].lower() in VALID_DTYPE_KEYWORDS:
            set_dtype_in_dtype_dict(dtype_dict, key, arg_list)
        else:
            raise ValueError(f"Invalid value for {key}: {value}")

    else:
        set_dtype_in_dtype_dict(dtype_dict, key, value)


def get_required_type(data_dict, cat_cols, encoding_type=False):
    """
    Get the unique list of required data types based on the given data
    dictionary and categorical columns.

    Args:
        data_dict (UKB_DataDict): An instance of the UKB_DataDict class
            containing information about the data dictionary.
        cat_cols (list): List of categorical columns.
        encoding_type (bool, optional): If True, retrieves encoding types.
            If False (default), retrieves regular types.

    Returns:
        list: Unique list of required data types that need to be present in
        the dtype dict.
    """
    types_list = []
    if encoding_type:
        types_list = (
            data_dict.main_table.loc[
                data_dict.main_table["Type"].isin(cat_cols), "Encoding_type"
            ]
            .unique()
            .tolist()
        )
    else:
        types_list = data_dict.main_table["Type"].unique().tolist()
        types_list = [my_type for my_type in types_list if my_type not in cat_cols]

    return types_list


def create_missing_type_set(present_types, required_types):
    """
    Create a set of missing data types based on the given present types and
    required types.

    Args:
        present_types (iterable): Iterable containing present data types.
        required_types (iterable): Iterable containing required data types.

    Returns:
        set: Set of missing data types.
    """
    present_set = set(present_types)
    required_set = set(required_types)

    missing_types = required_set - present_set
    return missing_types


def get_missing_types_in_dtype_dict(dtype_dict, data_dict, cat_cols):
    """
    Get the missing data types in the dtype dictionary based on the given
    data dictionary and categorical columns.

    Args:
        dtype_dict (dict): Dictionary mapping data types to desired data types.
        data_dict (UKB_DataDict): An instance of the UKB_DataDict class
            containing information about the data dictionary.
        cat_cols (list): List of categorical columns.

    Returns:
        dict: Dictionary with two keys "Type" and "Encoding_type", for each
        of these the missing data types will be stored in a list.
    """
    required_types = get_required_type(data_dict, cat_cols, encoding_type=False)
    required_encoding_types = get_required_type(data_dict, cat_cols, encoding_type=True)

    types_not_present = {
        "Type": create_missing_type_set(dtype_dict["Type"].keys(), required_types),
        "Encoding_type": create_missing_type_set(
            dtype_dict["Encoding_type"].keys(), required_encoding_types
        ),
    }
    return types_not_present


def parse_dtype_dict_args(dtype_args, default_dict):
    """
    Parse dtype arguments and create a dictionary mapping data types to desired
    dtype.

    Args:
        dtype_args (list): List of dtype arguments in the format 'key=value'.
        default_dict (dict): Default dtype dictionary.

    Returns:
        dict: Dictionary mapping data types to desired data types.
    """
    dtype_dict = default_dict

    if dtype_args:
        dtype_arg_set = set()
        for dtype_arg in dtype_args:
            try:
                dtype_arg_set.add(dtype_arg)
                add_dtype_arg_to_dict(dtype_arg, dtype_dict)
            except ValueError as ve:
                logging.warning(f"Error parsing dtype argument '{dtype_arg}': {ve}")

        # Check for duplicated dtype keys
        if len(dtype_arg_set) < len(dtype_args):
            logging.warning(
                "Duplicated dtypes found in given dtype arguments.:"
                "Make sure to specify each dtype once."
            )

    logging.info(
        "Using the following dtype dict to convert data types:\n"
        f"{json.dumps(dtype_dict, indent=4)}"
    )
    return dtype_dict


def parse_settings_args(settings_args):
    """
    Parse settings arguments and create a dictionary.

    Args:
        settings_args (list): List of settings arguments in the format 'key=value'.

    Raises:
        ValueError: If the settings option format is incorrect. This occurs when:
            - The settings option is not in the format 'key=value'.
            - Multiple settings options are given, but they are not separated
              by whitespace.

    Returns:
        dict: Dictionary containing parsed settings.
    """
    settings = {}
    if settings_args:
        for settings_arg in settings_args:
            split_arg = settings_arg.split("=")
            if not len(split_arg) == 2:
                raise ValueError(
                    "Could not split value the settings option"
                    f" ({settings_arg}) by '='. Make sure to give settings"
                    " options in the format 'key=value', and if multiple"
                    " options are given seperate them by whitespace."
                )
            settings[split_arg[0]] = split_arg[1]
    return settings


def get_local_client(n_workers=2, port="8787"):
    # try:
    #     client = Client(f"tcp://localhost:{port}", timeout="2s")
    # except TimeoutError:

    #     pass

    try:
        client = get_client()
        logging.info(f"Found client: {client}")
    except ValueError:
        filtered_config_defaults = list(
            filter(lambda x: "distributed" in x.keys(), dask.config.defaults)
        )
        if len(filtered_config_defaults) == 1:
            distributed_defaults = filtered_config_defaults[0]
            default_dashboard_link = distributed_defaults["distributed"]["dashboard"][
                "link"
            ]
        else:
            raise ValueError("Dask default config does not have the correct format")

        default_dashboard_link = default_dashboard_link
        dask.config.set({"distributed.dashboard.link": default_dashboard_link})

        cluster = LocalCluster(n_workers=n_workers, dashboard_address=":8787")
        client = Client(cluster)

    logging.info(f"Using client: {client}")
    logging.info(f"Client dashboard_link set to: {client.dashboard_link}")
    return client


def main(args):
    """
    Main function to convert UK Biobank TSV file to Parquet format based on
    the given configuration.

    Args:
        args: Command-line arguments parsed by argparse.
    """
    ukb_file_name = args.ukb_file.name
    data_dict_name = args.data_dict.name

    logging.info(
        f"Starting main function to convert {ukb_file_name} to" " parquet format."
    )

    # check if ukb_file file exists
    if not (os.path.isfile(ukb_file_name) and os.access(ukb_file_name, os.R_OK)):
        raise IOError(f"File {ukb_file_name} doesn't exist or isn't readable")

    # check if data_dict file exists
    if not (os.path.isfile(data_dict_name) and os.access(data_dict_name, os.R_OK)):
        raise IOError(f"File {data_dict_name} doesn't exist or isn't readable")

    get_local_client()

    data_dict = UKB_DataDict(data_dict_name)
    try:
        logging.info(
            "Trying to retrieve main table from the data"
            f" dictionary ({data_dict_name})"
        )
        data_dict.main_table
        logging.info(
            "Done retrieving main table from the data dictionary" f" ({data_dict_name})"
        )
    except Exception as e:
        raise ValueError(
            "Could not retrieve main table from data"
            f" dictionary file ({data_dict_name})"
        ) from e
    column_names = get_column_names(ukb_file_name, data_dict)

    dtype_dict = {}
    for dtype_dict_type in ["Type", "Encoding_type"]:
        dtype_dict_args = eval(f"args.dtype_dict_{dtype_dict_type.lower()}")
        dtype_dict[dtype_dict_type] = parse_dtype_dict_args(
            dtype_dict_args, DEFAULT_DTYPE_DICT[dtype_dict_type]
        )

    missing_types = get_missing_types_in_dtype_dict(
        dtype_dict, data_dict, args.categorical_cols
    )
    if missing_types["Type"] or missing_types["Encoding_type"]:
        raise ValueError(
            "Some types found in data_dict are not present in dtype_dict,"
            f" types not found are: {missing_types}"
        )

    settings = parse_settings_args(args.settings)

    convert_to_parquet(
        tsv_file_in=ukb_file_name,
        out_dir=args.out_dir,
        data_dict=data_dict,
        dtype_dict=dtype_dict,
        settings=settings,
        column_names=column_names,
        nrows=args.nrows,
        npartitions=args.repartition,
        offset=args.offset,
        force=args.force,
        cat_cols=args.categorical_cols,
        max_categories=args.max_categories,
        encoding=args.encoding,
    )


def main_cli():
    """
    Entry point for the command-line interface (CLI) of the UK Biobank TSV
    to Parquet conversion tool.

    Usage:
        ukb-to-parquet <ukb_file> <out_dir> <data_dict> [OPTIONS]
    """
    # Function to run when program termination signal is recieved
    # (for example process is killed because of slurm time contraint),
    # it will write a line to logfile
    signal.signal(signal.SIGTERM, utils.terminate_signal_handler)

    # Parse arguments
    args = parse_args()

    # Set log file
    set_logfile(args)
    joined_args = ", ".join([f"\n\t{arg}: {getattr(args, arg)}" for arg in vars(args)])
    logging.info("Parsed following args: {args}".format(args=joined_args))

    try:
        main(args)
    except Exception:
        logfile_path = logging.getLoggerClass().root.handlers[0].baseFilename
        msg = (
            f"An exception while converting {args.ukb_file.name}"
            f" to {args.out_dir} parquet file, see {logfile_path}"
        )
        utils.exit_script(
            ("An exception occured during the execution of the" f"{__file__} script."),
            log_function=logging.exception,
            status=1,
            exit_message=msg,
        )

    logging.info("Script done running, exiting...")
    sys.exit(0)


if __name__ == "__main__":
    main_cli()
