import pytest
import pandas as pd
from pathlib import Path
import numpy as np
import pyarrow as pa
import deepdiff
import math

from ukbutils.UKB_data_dict import UKB_DataDict
import ukbutils.get_translation_dict as get_translation_dict


@pytest.fixture
def data_dict():
    # Initialize the UKB_DataDict object with the test HTML file
    test_html_path = str((Path(__file__).parent / "test_data" / "data_dict_test.html"))
    data_dict = UKB_DataDict(test_html_path)
    yield data_dict


@pytest.fixture
def columns_list():
    columns_file = str((Path(__file__).parent / "test_data" / "test_columns.csv"))
    columns = pd.read_csv(columns_file, index_col=0, header=0, names=["Columns"])
    return columns.Columns.tolist()


my_dtype_mapping = {
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


my_pa_schema_mapping = {
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


@pytest.mark.parametrize("use_col, expected", [
    (["eid"], {"eid": my_dtype_mapping["Type"]["Sequence"]}),
    (["19-0.0"], {"19-0.0": pd.CategoricalDtype(categories=pd.Index([1, 2, 6, 7, 3], dtype=my_dtype_mapping["Encoding_type"]["Integer"]), ordered=False)}),
    (["34-0.0"], {"34-0.0": my_dtype_mapping["Type"]["Integer"]}),
    (["36-0.0"], {"36-0.0": my_dtype_mapping["Type"]["Text"]}),
    (["48-0.0"], {"48-0.0": my_dtype_mapping["Type"]["Continuous"]}),
    (["53-0.0"], {}),
    (["eid", "19-0.0", "34-0.0", "36-0.0", "48-0.0", "53-0.0"], 
        {"eid": my_dtype_mapping["Type"]["Sequence"],
         "19-0.0": pd.CategoricalDtype(categories=pd.Index([1, 2, 6, 7, 3], dtype=my_dtype_mapping["Encoding_type"]["Integer"]), ordered=False),
         "34-0.0": my_dtype_mapping["Type"]["Integer"],
         "36-0.0": my_dtype_mapping["Type"]["Text"],
         "48-0.0": my_dtype_mapping["Type"]["Continuous"]})
])
def test_get_d_type_dict(data_dict, use_col, expected):

    cat_cols = ["Categorical (single)", "Categorical (multiple)"] 
    max_num_categories = 256

    # Call the function
    result = get_translation_dict.get_d_type_dict(
        use_columns=use_col,
        data_dict=data_dict,
        dtype_dict=my_dtype_mapping,
        cat_cols=cat_cols,
        max_num_categories=max_num_categories
    )
    deep_diff = deepdiff.diff.DeepDiff(result, expected)

    # Assert the result is as expected
    assert isinstance(result, dict)
    assert not deep_diff


def test_get_date_format_dict(data_dict, columns_list):
    
    max_categories = 256
    
    result = get_translation_dict.get_date_format_dict(
        use_columns=columns_list,
        data_dict=data_dict,
        dtype_dict=my_dtype_mapping,
        max_categories=max_categories
    )

    expected = {
        "53-0.0": "%Y-%m-%d", 
        "53-1.0": "%Y-%m-%d", 
        "53-2.0": "%Y-%m-%d", 
        "53-3.0": "%Y-%m-%d"}
    
    deep_diff = deepdiff.diff.DeepDiff(result, expected)

    assert isinstance(result, dict)
    assert not deep_diff


@pytest.mark.parametrize("use_col, expected", [
    (["eid"], {"eid": my_pa_schema_mapping["Type"]["Sequence"]}),
    (["19-0.0"], {"19-0.0": pa.dictionary(index_type=pa.uint8(), value_type=my_pa_schema_mapping["Encoding_type"]["Integer"])}),
    (["34-0.0"], {"34-0.0": my_pa_schema_mapping["Type"]["Integer"]}),
    (["36-0.0"], {"36-0.0": my_pa_schema_mapping["Type"]["Text"]}),
    (["48-0.0"], {"48-0.0": my_pa_schema_mapping["Type"]["Continuous"]}),
    (["53-0.0"], {"53-0.0": my_pa_schema_mapping["Type"]["Date"]}),
    (["eid", "19-0.0", "34-0.0", "36-0.0", "48-0.0", "53-0.0"], 
        {"eid": my_pa_schema_mapping["Type"]["Sequence"],
         "19-0.0": pa.dictionary(index_type=pa.uint8(), value_type=my_pa_schema_mapping["Encoding_type"]["Integer"]),
         "34-0.0": my_pa_schema_mapping["Type"]["Integer"],
         "36-0.0": my_pa_schema_mapping["Type"]["Text"],
         "48-0.0": my_pa_schema_mapping["Type"]["Continuous"],
         "53-0.0": my_pa_schema_mapping["Type"]["Date"]})
])
def test_get_pa_schema_type_dict(data_dict, use_col, expected):

    cat_cols = ["Categorical (single)", "Categorical (multiple)"] 
    max_num_categories = 256

    # Call the function
    result = get_translation_dict.get_pa_schema(
        use_columns=use_col,
        data_dict=data_dict,
        pa_type_dict=my_pa_schema_mapping,
        cat_cols=cat_cols,
        max_num_categories=max_num_categories
    )

    deep_diff = deepdiff.diff.DeepDiff(result, expected)

    # Assert the result is as expected
    assert isinstance(result, dict)
    assert not deep_diff


@pytest.mark.parametrize("max_size, use_col, expected", [
    (2, 
     ["19-0.0", "54-0.0"], 
     {
         "19-0.0": my_pa_schema_mapping["Encoding_type"]["Integer"], 
         "54-0.0": my_pa_schema_mapping["Encoding_type"]["Integer"]
     }),
    (20, 
     ["19-0.0", "54-0.0"], 
     {
         "19-0.0": pa.dictionary(index_type=pa.uint8(), value_type=my_pa_schema_mapping["Encoding_type"]["Integer"]),
         "54-0.0": my_pa_schema_mapping["Encoding_type"]["Integer"]}
    ),
])
def test_get_pa_schema_type_dict_categorical_larger_than_max(data_dict, max_size, use_col, expected):

    cat_cols = ["Categorical (single)", "Categorical (multiple)"]

    # Call the function
    result = get_translation_dict.get_pa_schema(
        use_columns=use_col,
        data_dict=data_dict,
        pa_type_dict=my_pa_schema_mapping,
        cat_cols=cat_cols,
        max_num_categories=max_size
    )

    deep_diff = deepdiff.diff.DeepDiff(result, expected)

    # Assert the result is as expected
    assert isinstance(result, dict)
    assert not deep_diff


def test_get_pa_schema_type_dict_categorical_larger_than_8_bit(data_dict):
    # Add fictional large categorical to data dict
    col_index = data_dict.main_table.Column.max() + 1
    data_dict.main_table.loc[col_index] = [
        col_index, "Large categorical", 300, "Categorical (single)", 
        "Fictional categorical column, used to test the code", "100260", 
        300, "Integer", False, True, True
    ] 
    
    cat_cols = ["Categorical (single)", "Categorical (multiple)"]

    # Call the function
    result = get_translation_dict.get_pa_schema(
        use_columns=["Large categorical"],
        data_dict=data_dict,
        pa_type_dict=my_pa_schema_mapping,
        cat_cols=cat_cols,
        max_num_categories=300
    )
    
    # Expeccted result
    expected = {
        "Large categorical": pa.dictionary(
            index_type=pa.uint16(), value_type=my_pa_schema_mapping["Encoding_type"]["Integer"]
        )
    }

    deep_diff = deepdiff.diff.DeepDiff(result, expected)

    # Assert the result is as expected
    assert True
    assert isinstance(result, dict)
    assert not deep_diff

