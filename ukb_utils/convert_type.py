import numpy as np
import pandas as pd

# def get_required_bit_length(num_of_categories):
#     """
#     This function calculates the required bit length for a given number of categories.
#     It returns the dictionary type with the appropriate integer and string types based on the calculated bit length.
#     """
#     bit_length = 2**(math.ceil(math.log(num_of_categories, 2)) - 1).bit_length()
#     if bit_length <= 8:
#         return pa.dictionary(pa.int8(), pa.string())
#     else:
#         int_func = getattr(pa, 'int{}'.format(bit_length))
#         return pa.dictionary(int_func(), pa.string())

def ukb_get_dtype_categorical(main_table_entry, data_dict, conversion_table, categorical_max_size=256):
    num_members = main_table_entry["Encoding_num_members"].item()

    if np.isnan(num_members):
        raise ValueError("No \"Encoding_num_members\" item found in given dataframe (UDI: {}, encoding id: {}, encoding type: {})"
                         .format(main_table_entry["UDI"], main_table_entry["Encoding_id"], main_table_entry["Encoding_type"]))
    if main_table_entry["Encoding_type"].isna().item():
        raise ValueError("No \"Encoding_type\" item found in given dataframe (UDI: {}, encoding id: {}, encoding type: {})"
                         .format(main_table_entry["UDI"], main_table_entry["Encoding_id"], main_table_entry["Encoding_type"]))

    if num_members <= categorical_max_size:
        # if main_table_entry["Encoding_type"].item() != "Integer":
        my_encoding_table = data_dict.get_encoding_table(main_table_entry["Encoding_id"].item())
        if main_table_entry["Is_hierarchical"].item():
            my_encoding_table = my_encoding_table[my_encoding_table["selectable"] == "Y"]
        categories = my_encoding_table["Code"].tolist()
        encoding_type = main_table_entry["Encoding_type"].item()
        categories = pd.Index(categories, dtype = conversion_table[encoding_type])
        try:
            my_dtype = pd.CategoricalDtype(categories=categories, ordered=False)
        except Exception as e:
            raise ValueError("Something went wrong for setting categories for given dataframe (UDI: {}, encoding id: {}, encoding type: {})"
                             .format(main_table_entry["UDI"], main_table_entry["Encoding_id"], main_table_entry["Encoding_type"])) from e
    else:
        my_dtype = conversion_table[main_table_entry["Encoding_type"].item()]
    return(my_dtype)
            

def get_type_for_dask_dict(column_name, data_dict, conversion_table, categorical_max_size=256):
    if column_name == "eid":
        return("int")
        
    else:
        try:
            column_base, colname_ext = column_name.split("-")
            field_id = int(column_base)
        except ValueError:
            raise ValueError("Invalid column name format. Expected 'FieldID-Extension', where FieldID is an integer format, but got {}".format(column_name))

        main_table_entry = data_dict.main_table[data_dict.main_table["UDI"] == column_name]
        ukb_type = main_table_entry["Type"].item()

        if ukb_type.startswith("Categorical"):
            my_d_type = ukb_get_dtype_categorical(main_table_entry, data_dict, conversion_table, categorical_max_size)
        
        else:
            try:
                my_d_type = conversion_table[ukb_type]
            except KeyError:
                my_d_type = conversion_table["Compound"]

        # Check how big proportion of filled cells, if proportion small, make dtype sparse
        # max_count = data_dict.main_table.loc[data_dict.main_table["UDI"] == "eid", "Count"].item()
        # filled_cell_prop = main_table_entry["Count"].item() / max_count
        # if filled_cell_prop < 0.36:
        #     my_d_type = "Sparse[{}]".format(my_d_type)
        
        return my_d_type


def create_dask_type_dict(fields_list, data_dict, conversion_table, categorical_max_size=256):
    type_dict = {
        field: get_type_for_dask_dict(field, data_dict, categorical_max_size, conversion_table, categorical_max_size) 
        for field in fields_list 
        if not data_dict.main_table[
                data_dict.main_table["UDI"] == col_name
            ]["Type"].item() in ["Date", "Time"]
    }
    return(type_dict)

