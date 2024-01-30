import pytest
import pandas as pd
import os
from pathlib import Path

from ukbutils.UKB_data_dict import UKB_DataDict


@pytest.fixture
def test_html_path():
    yield str((Path(__file__).parent / "test_data" / "data_dict_test.html"))


@pytest.fixture
def data_dict(test_html_path):
    # Initialize the UKB_DataDict object with the test HTML file
    data_dict = UKB_DataDict(test_html_path)
    yield data_dict


@pytest.fixture
def expected_encoding_tables():
    expected_encoding_tables = {}
    used_udis = ["19-0.0", "21"]
    
    for udi in used_udis:
        file_path = Path(__file__).parent / "test_data" / f"expected_encoding_table_{udi}.csv"
        expected_encoding_tables[udi] = pd.read_csv(file_path)
    
    yield expected_encoding_tables
    


def test_path_to_html(data_dict, test_html_path):
    assert data_dict.path_to_html == test_html_path


def test_invalid_html_path():
    with pytest.raises(ValueError):
        UKB_DataDict("/path/to/invalid/file.txt")


def test_html_file_content(data_dict):
    # Test that the HTML file content is parsed correctly
    html_content = data_dict.html_file_content
    assert "UK Biobank : Application 55495" in str(html_content.title)


def test_invalid_encoding_table_limit(test_html_path):
    with pytest.raises(ValueError):
        UKB_DataDict(test_html_path, encoding_table_limit=-1)


def test_main_table_exists(data_dict):
    # Test that the main table is correctly parsed and is a DataFrame
    main_table = data_dict.main_table
    assert isinstance(main_table, pd.DataFrame)
    assert not main_table.empty


def test_main_table_required_columns(data_dict):
    main_table = data_dict.main_table

    # Check if specific columns are present
    expected_columns = ["Column", "UDI", "Type", "Description", 
                        "Count", "Encoding_id", "Encoding_num_members", "Encoding_type", 
                        "Is_hierarchical", "Is_categorical", "Has_encoding"]
    if not set(main_table.columns) == set(expected_columns):
        pytest.fail(f"Expected columns: {', '.join(expected_columns)}, Actual columns: {', '.join(main_table.columns)}")


def test_main_table_dtypes(data_dict):
    main_table = data_dict.main_table

    assert main_table["Column"].dtype == pd.Int64Dtype()
    assert main_table["UDI"].dtype == "object"
    assert main_table["Type"].dtype == "object"
    assert main_table["Description"].dtype == "object"
    assert main_table["Count"].dtype == pd.Int64Dtype()
    assert main_table["Encoding_id"].dtype == "object"
    assert main_table["Encoding_num_members"].dtype == pd.Int64Dtype()
    assert main_table["Encoding_type"].dtype == "object"
    assert main_table["Is_hierarchical"].dtype == bool
    assert main_table["Is_categorical"].dtype == bool
    assert main_table["Has_encoding"].dtype == bool
    

def test_info(data_dict):
    # Test that info is correctly parsed and is a dictionary
    info = data_dict.info
    assert isinstance(info, dict)
    assert "Date Extracted:" in info
    assert "Data columns:" in info


@pytest.mark.parametrize("encoding_id", [
    ("100260"),
    ("100261"),
    ("8"),
    ("10")
])
def test_valid_encoding_id_download(data_dict, encoding_id):
    data_dict._download_encoding_table(encoding_id)
    assert os.path.exists(os.path.expanduser(data_dict._encoding_file_template.format(encoding_id)))

    #Clean up
    os.remove(os.path.expanduser(data_dict._encoding_file_template.format(encoding_id)))


@pytest.mark.parametrize("encoding_id", [
    ("100001"), # Exists, but not in testing data dict
    ("Fake encoding id") # Non existsting
])
def test_invalid_encoding_id_download(data_dict, encoding_id):
    with pytest.raises(ValueError):
        data_dict._download_encoding_table(encoding_id)


@pytest.mark.parametrize("field_id, is_udi", [
    ("19-0.0", True),
    ("21", False),
])
def test_get_encoding_by_data_field(data_dict, expected_encoding_tables, field_id, is_udi):
    result_df= data_dict.get_encoding_by_data_field(field_id, is_udi=is_udi)
    assert expected_encoding_tables[field_id].equals(result_df)
    

@pytest.mark.parametrize("field_id, is_udi", [
    ("Not a UDI", True),
    ("Not a UDI", False),
    ("21", True),
    ("19-0.0", False)
])
def test_get_encoding_by_data_field_UDI_not_found(data_dict, field_id, is_udi):
    with pytest.raises(ValueError):
        data_dict.get_encoding_by_data_field(field_id, is_udi=is_udi)



#TODO FINISH TESTS BELOW
# def test_get_encoding_table_from_file(data_dict):
#     encoding_id = 123  # Replace with a valid encoding ID for testing
#     table = data_dict._get_encoding_table_from_file(encoding_id)
#     assert isinstance(table, pd.DataFrame)

# def test_get_encoding_table_from_html(data_dict):
#     encoding_id = 123  # Replace with a valid encoding ID for testing
#     table = data_dict._get_encoding_table_from_html(encoding_id)
#     assert isinstance(table, pd.DataFrame)


# def test_encoding_id_is_valid(data_dict):
#     valid_encoding_id = 123  # Replace with a valid encoding ID for testing
#     invalid_encoding_id = 456  # Replace with an invalid encoding ID for testing

#     assert data_dict._encoding_id_is_valid(valid_encoding_id)
#     assert not data_dict._encoding_id_is_valid(invalid_encoding_id)

if __name__ == "__main__":
    pytest.main()