"""
UK Biobank DataType Mapping Script

This script provides functions to create dictionaries that map UK Biobank columns
to desired data types. Currently, it supports mapping columns to pandas dtypes and
pyarrow schema types. These dictionaries are intended to facilitate data processing
and storage operations.

Functions meant for external use:
- get_pa_schema: Creates a dictionary mapping columns to pyarrow schema types.
- get_date_format_dict: Creates a dictionary mapping date columns to their respective
  date formats.
- get_d_type_dict: Creates a dictionary mapping columns to pandas dtypes.

Author:
    Naomi Hindriks
"""

import logging
import numpy as np
import pandas as pd
import math
import pyarrow as pa


EXCEEDING_MAX_CAT_KEYWORD = "EXCEEDING MAX LEVELS OF CATEGORY"


def _find_date_types(dtype_conversion_dict):
    """
    Find the keys in dtype_conversion_dict that correspond to date types.

    Args:
        dtype_conversion_dict (dict): Dictionary mapping data types to desired
            data types.

    Returns:
        set: Set of keys corresponding to date types in dtype_conversion_dict.
    """
    date_type_keys = set()
    # Find which keys have a date value inside the dtype["Type"] dict:
    for key in dtype_conversion_dict:
        if isinstance(dtype_conversion_dict[key], list):
            if dtype_conversion_dict[key][0] == "Date":
                date_type_keys.add(key)
                logging.info(f"Found date type '{key}' in dtype_conversion_dict")
    return date_type_keys


def _get_pyarrow_int_func_for_dict(num_of_categories):
    """
    Return the pyarrow unsigned integer constructor needed to create a pyarrow
    dictionary for the given number of categories.

    Args:
        num_of_categories (int): The number of categories that need to fit in
            the dictionary.

    Returns:
        Callable: PyArrow unsigned integer constructor (e.g., pa.uint8 or uint16).
    """
    bit_length = 2 ** (math.ceil(math.log(num_of_categories, 2)) - 1).bit_length()
    if bit_length <= 8:
        return pa.uint8
    else:
        int_func = getattr(pa, "uint{}".format(bit_length))
        return int_func


def _determine_categorical_type_pyarrow(
    main_table_entry, data_dict, conversion_table_encoding, num_members
):
    """
    Translate UK Biobank categorical type to PyArrow dictionary type (pa.dictionary).

    Args:
        main_table_entry (pandas.Series): A row from the main table of the UK Biobank
            data dictionary.
        data_dict (UKB_DataDict): An instance of the UKB_DataDict class containing
            information about the data dictionary.
        conversion_table_encoding (dict): A dictionary mapping encoding types to
            Pyarrow types.
        num_members (int): Number of levels in categorical.

    Returns:
        PyArrowType: PyArrow dictionary type.

    Raises:
        ValueError: If setting categories for the given dataframe fails.
    """
    # my_encoding_table = data_dict.get_encoding_table(encoding_id)

    # if main_table_entry["Is_hierarchical"].item():
    #     my_encoding_table = my_encoding_table[my_encoding_table["selectable"] == "Y"]

    encoding_type = main_table_entry["Encoding_type"].item()

    try:
        int_func = _get_pyarrow_int_func_for_dict(num_members)
        logging.info(f"set int_func to {int_func}")
        my_type = pa.dictionary(
            index_type=int_func(), value_type=conversion_table_encoding[encoding_type]
        )
    except Exception as e:
        raise ValueError(
            "Something went wrong for setting categories for given dataframe"
            " (UDI: {}, encoding id: {}, encoding type: {})".format(
                main_table_entry["UDI"].item(),
                main_table_entry["Encoding_id"].item(),
                main_table_entry["Encoding_type"].item(),
            )
        ) from e

    return my_type


def _determine_categorical_type_dtype(
    main_table_entry, data_dict, conversion_table_encoding, num_members
):
    """
    Function that translates UK Biobank categorical type to Pandas CategoricalDtype.

    Args:
        main_table_entry (pandas.Series): A row from the main table of the UK Biobank
            data dictionary.
        data_dict (UKB_DataDict): An instance of the UKB_DataDict class containing
            information about the data dictionary.
        conversion_table_encoding (dict): A dictionary mapping encoding types to
            Pandas dtype.
        num_members (int): Number of levels in categorical.

    Returns:
        Pandas CategoricalDtype: Instance of Pandas categorical dtype.

    Raises:
        ValueError: If setting categories for the given dataframe fails.
    """
    encoding_id = main_table_entry["Encoding_id"].item()
    my_encoding_table = data_dict.get_encoding_table(encoding_id)

    if main_table_entry["Is_hierarchical"].item():
        my_encoding_table = my_encoding_table[my_encoding_table["selectable"] == "Y"]

    categories = my_encoding_table["Code"].tolist()
    encoding_type = main_table_entry["Encoding_type"].item()
    categories = pd.Index(categories, dtype=conversion_table_encoding[encoding_type])

    try:
        my_type = pd.CategoricalDtype(categories=categories, ordered=False)
    except Exception as e:
        raise ValueError(
            "Something went wrong for setting categories for given dataframe"
            " (UDI: {}, encoding id: {}, encoding type: {})".format(
                main_table_entry["UDI"].item(),
                main_table_entry["Encoding_id"].item(),
                main_table_entry["Encoding_type"].item(),
            )
        ) from e

    return my_type


def _determine_categorical_type(
    main_table_entry,
    data_dict,
    conversion_table_encoding,
    conversion_function,
    categorical_max_size=256,
):
    """
    Determine the appropriate type for a categorical column in the UK Biobank
    dataset given the conversion_table_encoding and conversion_function.

    Args:
        main_table_entry (pandas.Series): A row from the main table of the UK
            Biobank data dictionary.
        data_dict (UKB_DataDict): An instance of the UKB_DataDict class containing
            information about the data dictionary.
        conversion_table_encoding (dict): A dictionary mapping encoding types to
            Pandas dtype.
        conversion_function (function): A function that does the actual translation.
        categorical_max_size (int, optional): The maximum number of levels
            that can be present in a categorical column. Defaults to 256.

    Returns:
        Result of the conversion_function.

    Raises:
        ValueError: If the necessary information for determining the dtype
            is not found in the provided data.

    """
    num_members = main_table_entry["Encoding_num_members"].item()

    if np.isnan(num_members) or main_table_entry["Encoding_type"].isna().item():
        raise ValueError(
            'Trying to parse {} as categorical, but no "Encoding_num_members"'
            " item found in given dataframe (encoding id: {}, encoding type: {},"
            " encoding num members: {})".format(
                main_table_entry["UDI"].item(),
                main_table_entry["Encoding_id"].item(),
                main_table_entry["Encoding_type"].item(),
                main_table_entry["Encoding_num_members"].item(),
            )
        )

    if num_members <= categorical_max_size:
        my_type = conversion_function(
            main_table_entry, data_dict, conversion_table_encoding, num_members
        )

    else:
        my_type = EXCEEDING_MAX_CAT_KEYWORD

    return my_type


def _determine_column_type(
    column_name,
    data_dict,
    conversion_table,
    categorical_conversion_method,
    cat_cols,
    categorical_max_size,
):
    """
    Determine the appropriate type for a column in the UK Biobank
    dataset, considering optional categorical columns. Using the conversion_table
    for the mapping

    Args:
        column_name (str): The name of the column in the UK Biobank dataset.
        data_dict (UKB_DataDict): An instance of the UKB_DataDict class containing
            information about the data dictionary.
        conversion_table (dict): A dictionary mapping UK Biobank types to
            desired type.
        categorical_conversion_method: A function that will be called to perform
            categorical conversion.
        cat_cols (list): A list of column types (e.g.
            'Categorical (single)'), columns with this type will be treated
            as categorical.
        categorical_max_size (int): The maximum number of levels
            that can be present in a categorical column. If the categorical type
            exeecds this number it will be translated to it's Encoding_type

    Returns:
        The type found in the conversion_table, or in case of categorical the return
            of the categorical_conversion_method,

    Raises:
        ValueError: If the necessary information for determining the dtype
            is not found in the provided data or if an invalid configuration
            is detected.
    """
    conversion_table_regular = conversion_table["Type"]
    conversion_table_encoding = conversion_table["Encoding_type"]

    main_table_entry = data_dict.main_table[data_dict.main_table["UDI"] == column_name]

    # TODO: add test to check if there is a single row for the given column
    # name in the main table, throw error if not found or multiple rows
    ukb_type = main_table_entry["Type"].item()

    if ukb_type in cat_cols:
        my_type = _determine_categorical_type(
            main_table_entry,
            data_dict,
            conversion_table_encoding,
            categorical_conversion_method,
            categorical_max_size,
        )
        if my_type == EXCEEDING_MAX_CAT_KEYWORD:
            encoding_type = main_table_entry["Encoding_type"].item()
            try:
                my_type = conversion_table_encoding[encoding_type]
            except KeyError:
                raise ValueError(
                    f"The type {encoding_type} could not be translated"
                    " to a dtype, because it was not present in given"
                    f" conversion_table_encoding ({conversion_table_encoding})"
                )
    else:
        try:
            my_type = conversion_table_regular[ukb_type]
        except KeyError:
            raise ValueError(
                f"The type {ukb_type} could not be translated"
                " to a dtype, because it was not present in given"
                f" conversion_table_regular ({conversion_table_regular})"
            )

    return my_type


def get_d_type_dict(use_columns, data_dict, dtype_dict, cat_cols, max_num_categories):
    """
    Create a dictionary that maps UK Biobank columns to a pandas dtype based on
    the given dtype_dict.


    Args:
        use_columns (list): List of column names to be used.
        data_dict (UKB_DataDict): Instance of UKB_DataDict.
        dtype_dict (dict): Dictionary mapping UK Biobank data types to with
            data types to use in read_table function.
        cat_cols (list): A list of column types (e.g.
            ['Categorical (single)']), columns with this type will be treated
            as categorical.
        max_num_categories (int): Maximum number of categories for categorical
            columns. If the number of unique categories in a column exceeds
            this limit, the dtype will be set according to the values of the
            category levels, rather than treating it as a categorical column.

    Returns:
        dict: A dictionary where keys are column names from
            `use_columns` and values are the corresponding pandas dtypes
    """
    # TODO: add more logging?
    logging.info(
        "Determining dtypes for columns to use in the dask dataframe"
        "read_table function"
    )

    date_type_keys = _find_date_types(dtype_dict["Type"])
    # date_type_encoding_keys = _find_date_types(dtype_dict["Encoding_type"])

    # Call the _determine_column_type function for every column that is
    # not considered a "Date" type
    types_dict = {
        col_name: _determine_column_type(
            col_name,
            data_dict,
            dtype_dict,
            _determine_categorical_type_dtype,
            cat_cols,
            max_num_categories,
        )
        for col_name in use_columns
        if not (
            data_dict.main_table.loc[
                data_dict.main_table["UDI"] == col_name, "Type"
            ].item()
            in date_type_keys
        )
    }
    logging.info(f"Initial types dict has length {len(types_dict)}")

    for col_name in types_dict:
        if isinstance(types_dict[col_name], list):
            if types_dict[col_name][0] == "Date":
                del types_dict[col_name]

    return types_dict


def get_date_format_dict(use_columns, data_dict, dtype_dict, max_categories):
    """
    Create a dictionary mapping date columns to their respective date formats.

    Args:
        use_columns (list): Iterable containing column names to consider.
        data_dict (UKB_DataDict): An instance of the UKB_DataDict class
            containing information about the data dictionary.
        dtype_dict (dict): Dictionary mapping data types to desired date formats.
        max_categories (int): Maximum levels allowed in a categorical column
            for dtype conversion.

    Returns:
        dict: Dictionary mapping date columns to their respective date formats.
    """
    date_type_keys = _find_date_types(dtype_dict["Type"])
    date_type_encoding_keys = _find_date_types(dtype_dict["Encoding_type"])

    date_format_dict = {}
    date_columns_found = 0
    encoding_date_columns_found = 0

    for col_name in use_columns:
        col_type = data_dict.main_table.loc[
            data_dict.main_table["UDI"] == col_name, "Type"
        ].item()
        if col_type in date_type_keys:
            date_format_dict[col_name] = dtype_dict["Type"][col_type][1]
            date_columns_found += 1
            logging.debug(
                f"Column '{col_name}' identified as date type with"
                f" format: {dtype_dict['Type'][col_type][1]}"
            )

    encoding_date_columns = data_dict.main_table.loc[
        data_dict.main_table["Encoding_type"].isin(date_type_encoding_keys)
        & data_dict.main_table["UDI"].isin(use_columns),
        "UDI",
    ]

    for col_name in encoding_date_columns:
        n_categories = data_dict.main_table.loc[
            data_dict.main_table["UDI"] == col_name, "Encoding_num_members"
        ].item()

        if n_categories > max_categories:
            col_encoding_type = data_dict.main_table.loc[
                data_dict.main_table["UDI"] == col_name, "Encoding_type"
            ].item()
            if col_encoding_type in date_type_encoding_keys:
                date_format_dict[col_name] = dtype_dict["Encoding_type"][
                    col_encoding_type
                ][1]
                encoding_date_columns_found += 1
                encoding_format = dtype_dict["Encoding_type"][col_encoding_type][1]
                logging.debug(
                    f"Column '{col_name}' identified as date type"
                    f" with format: {encoding_format}"
                )

    logging.info(
        f"Identified {date_columns_found} date columns and"
        f" {encoding_date_columns_found} encoding date columns."
    )

    return date_format_dict


def get_pa_schema(use_columns, data_dict, pa_type_dict, cat_cols, max_num_categories):
    """
    Create a dictionary that maps uk biobank columns to a pyarrow schema type

    Args:
        use_columns (list): List of column names to be used.
        data_dict (UKB_DataDict): Instance of UKB_DataDict.
        pa_type_dict (dict): Dictionary mapping UK Biobank data types to with
            data types to use in read_table function.
        cat_cols (list): A list of column types (e.g.
            ['Categorical (single)']), columns with this type will be treated
            as categorical.
        max_num_categories (int): Maximum number of categories for categorical
            columns. If the number of unique categories in a column exceeds
            this limit, the dtype will be set according to the values of the
            category levels, rather than treating it as a categorical column.

    Returns:
        types (dict): A dictionary where keys are column names from
            `use_columns` and values are the corresponding pyarrow datatype.
    """
    logging.info("Determining pyarrow types for schema to write to parquet file")

    pyarrow_schema = {
        col_name: _determine_column_type(
            col_name,
            data_dict,
            pa_type_dict,
            _determine_categorical_type_pyarrow,
            cat_cols,
            max_num_categories,
        )
        for col_name in use_columns
    }

    return pyarrow_schema
