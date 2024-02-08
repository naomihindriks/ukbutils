import os
import pandas as pd
import subprocess as sp
from bs4 import BeautifulSoup
from io import StringIO


class UKB_DataDict:
    """
    A class for working with UK Biobank Data Dictionary HTML files generated
    by the 'ukbconv' utility.

    The 'ukbconv' utility creates an HTML document that lists information about
    the structure of the dataset, including data coding information. This class
    provides methods for parsing and extracting relevant information from the
    generated HTML file.

    TODO:
        - When new UKB_DataDict is initialized check the encoding_file_template
            path to see if there are encoding files, if so add to current encodings
            in cache. (If too many exceeding encoding_table_limit, what to do?)
        - Some way to save state of UKB_DataDict object?
    """

    ENCODING_DOWNLOAD_URL_TEMPLATE = (
        "https://biobank.ndph.ox.ac.uk/showcase/codown.cgi?id={}"  # noqa: E501
    )
    DEFAULT_ENCODING_FILE_TEMPLATE = (
        "~/ukb-analyses/data/ukb/encoding_dicts/encoding_table_{}.txt"  # noqa: E501
    )

    def __init__(
        self,
        path,
        encoding_file_template=DEFAULT_ENCODING_FILE_TEMPLATE,
        encoding_table_limit=100,
    ):
        """
        Initialize the UKB_DataDict object.

        Args:
            path (str): The path to the UK Biobank Data Dictionary HTML file.
            encoding_file_template (str, optional): The template for encoding
                table file paths.
            encoding_table_limit (int, optional): The limit for the number
                of encoding tables to be stored in the cache.
        """
        self._path_to_html = None
        self.path_to_html = path

        self._html_file_content = None
        self._main_table = None
        self._info = None
        self._data_encoding_tables = dict()

        self._saved_encodings = []
        self.encoding_table_limit = encoding_table_limit

        self.encoding_file_template = encoding_file_template

    @property
    def path_to_html(self):
        """
        str: The path to the UK Biobank Data Dictionary HTML file.
        """
        return self._path_to_html

    @path_to_html.setter
    def path_to_html(self, path):
        """
        Setter for `path_to_html` property.

        Args:
            path (str): The path to the UK Biobank Data Dictionary HTML file.

        Raises:
            ValueError: If the path is not a valid HTML file or lacks read access.
        """
        if path != self._path_to_html:
            if os.access(path, os.R_OK) and path.endswith(".html"):
                self._path_to_html = path
                self._html_file_content = None
                self._main_table = None
                self._info = None
                self._data_encoding_tables = dict()
                self._saved_encodings = []
            else:
                raise ValueError(
                    "Path must be an valid HTML file with read"
                    f" access. {path} could not be read or was"
                    " not an HTML file."
                )

    @property
    def html_file_content(self):
        """
        BeautifulSoup: Parsed HTML content of the Data Dictionary file.
        """
        if self._html_file_content is None:
            with open(self.path_to_html) as fp:
                self._html_file_content = BeautifulSoup(fp, "html.parser")
        return self._html_file_content

    @property
    def main_table(self):
        """
        pd.DataFrame: The main table from the HTML data dictionary that lists
        information about the structure of the dataset.
        """
        if self._main_table is None:
            self._main_table = self._parse_main_table()
        return self._main_table

    @property
    def info(self):
        """
        dict: Metadata scraped from the top of the HTML data dictionary file.
        It contains information about the extraction date and the number of
        data columns in the dataset.
        """
        if self._info is None:
            # Get info table from file and convert to pandas
            info_table_html = self.html_file_content.find("table")
            info_table_pd = pd.read_html(StringIO(str(info_table_html)))[0]
            self._info = {
                info_table_pd[0][0]: info_table_pd[1][0],
                info_table_pd[0][1]: info_table_pd[1][1],
            }
        return self._info

    @property
    def encoding_table_limit(self):
        """
        int: The limit for the number of encoding tables to be stored in the
        cache.
        """
        return self._encoding_table_limit

    @encoding_table_limit.setter
    def encoding_table_limit(self, limit):
        """
        Setter for `encoding_table_limit` property.

        Args:
            limit (int): The new limit on the number of encoding tables to be
            stored in the cache.

        Raises:
            ValueError: If the limit is not a positive integer.
        """
        if not isinstance(limit, int) or limit <= 0:
            raise ValueError("Encoding table limit must be a positive integer.")

        self._encoding_table_limit = limit
        self._remove_oldest_if_needed_encoding_cache()

    @property
    def encoding_file_template(self):
        return self._encoding_file_template

    @encoding_file_template.setter
    def encoding_file_template(self, new_file_template):
        self._encoding_file_template = os.path.expanduser(new_file_template)

    def _parse_main_table(self):
        """
        Parse the main data dictionary table.

        Returns:
            pd.DataFrame: The parsed main data dictionary table.
        """
        main_table = self._get_main_from_html()
        self._process_main_table(main_table)
        return main_table

    def _get_main_from_html(self):
        """
        Extract the main data dictionary table from the HTML.

        Returns:
            pd.DataFrame: The extracted main data dictionary table.
        """
        html_content = self.html_file_content.findAll("table", limit=2)[1]
        return pd.read_html(StringIO(str(html_content)))[0]

    def _process_main_table(self, main_table):
        """
        Process the main data dictionary table, adding derived columns, and
        setting appropriate dtypes.

        Args:
            main_table (pd.DataFrame): The main data dictionary table to process.
        """
        main_table["Column"] = main_table["Count"].astype("Int64")
        main_table["Count"] = main_table["Count"].astype("Int64")
        main_table["Encoding_id"] = main_table["Description"].str.extract(
            r"Uses data-coding (\d+)"
        )
        main_table["Encoding_num_members"] = (
            main_table["Description"]
            .str.extract(r"Uses data-coding \d+ comprises (\d+)")
            .astype("Int64")
        )
        main_table["Encoding_type"] = main_table["Description"].str.extract(
            r"Uses data-coding \d+ comprises \d+ (\w+)-valued"
        )
        main_table["Is_hierarchical"] = main_table["Description"].str.contains(
            "hierarchical"
        )
        main_table["Is_categorical"] = main_table["Type"].str.startswith("Categorical")
        main_table["Has_encoding"] = main_table["Encoding_id"].notna()

    def _download_encoding_table(self, encoding_id):
        """
        Download an encoding table from the UK Biobank website using the curl
        command inside a subprocess.

        Args:
            encoding_id (int): The ID of the encoding table to download.

        Raises:
            ValueError: If the provided encoding ID is not valid (does not
                exist in the main table).
        """
        if not self._encoding_id_is_valid(encoding_id):
            raise ValueError("Invalid encoding ID: {}".format(encoding_id))

        url = self.ENCODING_DOWNLOAD_URL_TEMPLATE.format(encoding_id)
        encoding_dest_filename = self.encoding_file_template.format(encoding_id)

        command_list = [
            "curl",
            url,
            "-silent",
            "--create-dirs",
            "-o",
            encoding_dest_filename,
        ]
        sp.run(command_list, stdout=open(os.devnull, "wb"))

    def _get_encoding_table_from_file(self, encoding_id):
        """
        Get an encoding table from a local file, if not present try to download
        it with the _download_encoding_table method.

        Args:
            encoding_id (int): The ID of the encoding table.

        Returns:
            pd.DataFrame: The encoding table.

        Raises:
            ValueError: If the file is inaccessible or if the UK Biobank website
                returns an error.
        """
        encoding_filename = self.encoding_file_template.format(encoding_id)

        if not os.path.isfile(encoding_filename):
            print(f"Trying to download ({encoding_filename})")
            self._download_encoding_table(encoding_id)
        elif not os.access(encoding_filename, os.R_OK):
            raise ValueError(
                f"File with encoding table {encoding_filename}"
                " does not have read access."
            )

        with open(encoding_filename, "r") as f:
            file_content = f.read()

        if "Sorry, internal error prevents download of coding" in file_content:
            os.remove(encoding_filename)
            raise ValueError(
                "UK Biobank website returned an internal server"
                " error while trying to download encoding table"
                f" for encoding id {encoding_id}"
            )

        encoding_table = pd.read_table(encoding_filename, skiprows=7)
        encoding_table = encoding_table.rename(columns={"coding": "Code"})
        return encoding_table

    def _get_encoding_table_from_html(self, encoding_id):
        """
        Extract an encoding table from the HTML content.

        Args:
            encoding_id (int): The ID of the encoding table.

        Returns:
            pd.DataFrame: The encoding table.

        Raises:
            ValueError: If no encoding table wth given encoding_id is found in the HTML.
        """
        encoding_table_html = self.html_file_content.find(
            "table", summary=f"Coding {encoding_id}"
        )

        if not encoding_table_html:
            raise ValueError(
                "No encoding table found for encoding with encoding"
                f" ID: {encoding_id}"
            )

        return pd.read_html(StringIO(str(encoding_table_html)))[0]

    def _encoding_id_is_valid(self, encoding_id):
        """
        Check if an encoding ID is valid (exists in the main table).

        Args:
            encoding_id (int): The ID of the encoding table to check.

        Returns:
            bool: True if the encoding ID is valid, False otherwise.
        """
        return encoding_id in self.main_table["Encoding_id"].unique()

    def _encoding_is_hierarchical(self, encoding_id):
        """
        Check if an encoding is hierarchical, with the main_table
        Is_hierarchical column.

        Args:
            encoding_id (int): The ID of the encoding table to check.

        Returns:
            bool: True if the encoding is hierarchical, False otherwise.
        """
        encoding_is_hierarchical = self.main_table[
            self.main_table["Encoding_id"] == encoding_id
        ].iloc[0]["Is_hierarchical"]
        return encoding_is_hierarchical

    def _retrieve_encoding_table(self, encoding_id):
        """
        Retrieve an encoding table, either from a file (if hierarchical) or
        from HTML (if not hierarchical).

        Args:
            encoding_id (int): The ID of the encoding table to retrieve.

        Returns:
            pd.DataFrame: The retrieved encoding table.

        Raises:
            ValueError: If the encoding ID is not found or an error occurs
                during retrieval.
        """
        if not self._encoding_id_is_valid(encoding_id):
            raise ValueError("Encoding ID ({encoding_id}) not found in main table.")

        if self._encoding_is_hierarchical(encoding_id):
            return self._get_encoding_table_from_file(encoding_id)
        else:
            return self._get_encoding_table_from_html(encoding_id)

    def _calc_encoding_file_size(self):
        """
        Calculate the total size of saved encoding files in Mebibytes (MiB).

        Returns:
            float: The total size of saved encoding files in Mebibytes.
        """
        total_size = sum(
            os.path.getsize(self.encoding_filename.format(enc_id)) / (1024 * 1024)
            for enc_id in self._saved_encodings
            if os.path.exists(self.encoding_filename.format(enc_id))
        )
        return total_size

    def _remove_oldest_if_needed_encoding_cache(self):
        """
        Remove the oldest encoding tables from the cache if it exceeds the
        specified limit (property).
        """
        while len(self._saved_encodings) > self._encoding_table_limit:
            del self._data_encoding_tables[self._saved_encodings[0]]
            del self._saved_encodings[0]

    def _update_saved_encoding_order(self, encoding_id):
        """
        Update the order of saved encodings in the cache.
        Take the given encoding_id from the list, remove it and append it again.

        Args:
            encoding_id (int): The ID of the encoding table.
        """
        self._saved_encodings.remove(encoding_id)
        self._saved_encodings.append(encoding_id)

    def _add_encoding_table_to_cache(self, encoding_id):
        """
        Add an encoding table to the cache.

        Args:
            encoding_id (int): The ID of the encoding table to cache.
        """
        self._data_encoding_tables[encoding_id] = self._retrieve_encoding_table(
            encoding_id
        )  # noqa: E501
        self._saved_encodings.append(encoding_id)
        self._remove_oldest_if_needed_encoding_cache()

    def get_encoding_table(self, encoding_id):
        """
        Get an encoding table by ID, either from cache or by retrieving it.

        Args:
            encoding_id (int): The ID of the encoding table to retrieve.

        Returns:
            pd.DataFrame: The retrieved or cached encoding table.

        Raises:
            ValueError: If the encoding table cannot be read or an error occurs
                during retrieval.
        """
        if encoding_id not in self._data_encoding_tables.keys():
            try:
                self._add_encoding_table_to_cache(encoding_id)
            except Exception as e:
                raise ValueError(
                    "No encoding table could be read for encoding" f" id {encoding_id}."
                ) from e
        else:
            self._update_saved_encoding_order(encoding_id)
        return self._data_encoding_tables[encoding_id]

    def get_encoding_by_data_field(self, field_id, is_udi=True):
        """
        Get an encoding table associated with a specific data field ID, either
        by UDI (unique data identifier) or by the first part of the UDI.

        Args:
            field_id (str): The data field ID (either a complete UDI or its
                first part).
            is_udi (bool, optional): If True, `field_id` is treated as a complete UDI;
                if False, it's treated as the first part of the UDI.

        Returns:
            pd.DataFrame: The retrieved or cached encoding table associated with
            the specified data field ID.

        Raises:
            ValueError: If the encoding table cannot be read or an error occurs
                during retrieval, or if the specified data field ID is not found
                in the main table.
        """
        if is_udi:
            if field_id not in self.main_table["UDI"].unique():
                raise ValueError(
                    f"Given field_id ({field_id}) not found in the"
                    "UDI column of the main table."
                )

            # Get encoding id belonging to field id
            enc_id = self.main_table.loc[
                self.main_table["UDI"] == field_id, "Encoding_id"
            ].item()

        else:
            split_udi_id = self.main_table["UDI"].str.split("-", expand=True)[0]
            if field_id not in split_udi_id.unique():
                raise ValueError(
                    f"Given field_id ({field_id}) not found as part of a"
                    " UDI in the UDI column of the main table."
                )

            enc_id_series = self.main_table.loc[split_udi_id == field_id, "Encoding_id"]
            if enc_id_series.size == 1:
                enc_id = enc_id_series.item()
            else:
                enc_id = enc_id_series.iloc[0]

        return self.get_encoding_table(enc_id)

    def get_human_readable_name(self, field_id, is_udi=True):
        """
        Retrieve the human-readable name associated with a given field ID.

        Args:
            field_id (str): The data field ID, either a complete UDI or its
                first part.
            is_udi (bool, optional): If True, `field_id` is treated as a complete
                UDI; if False, it's treated as the first part of the UDI.

        Returns:
            str: The human-readable name associated with the specified data field ID.

        Raises:
            ValueError: If the specified field ID is not found in the main table
                UDI column or if it's ambiguous (multiple names found).
        """
        if is_udi:
            if field_id not in self.main_table["UDI"].unique():
                raise ValueError(
                    f"Field ID ({field_id}) not found in the UDI"
                    " column of the main table. Please check the"
                    " provided field ID."
                )

            # Get description belonging to field id
            description = self.main_table.loc[
                self.main_table["UDI"] == field_id, "Description"
            ].item()

        else:
            split_udi_id = self.main_table["UDI"].str.split("-", expand=True)[0]
            if field_id not in split_udi_id.unique():
                raise ValueError(
                    f"Field ID ({field_id}) not found as part of"
                    " a UDI in the UDI column of the main table."
                    " Please check the provided field ID."
                )

            description_series = self.main_table.loc[
                split_udi_id == field_id, "Description"
            ]
            if description_series.size == 1:
                description = description_series.item()
            else:
                # Check if all the descriptions are indentical
                if len(description_series.unique()) > 1:
                    raise ValueError(
                        f"Ambiguous field_id: {field_id}. Multiple"
                        " non-identical human-readable names found."
                    )
                else:
                    description = description_series.iloc[0]

        stripped_description = description.split(" Uses data-coding", 1)[0]
        return stripped_description
