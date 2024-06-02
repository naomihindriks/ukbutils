import os
import shutil
import tempfile
import argparse
from importlib import resources as impresources

import pytest

import ukbutils
# from ukbutils.get_template import *
import ukbutils.get_template as get_template

@pytest.fixture(scope="module")
def temp_dir():
    temp_dir = tempfile.mkdtemp()

    # Create the existing file
    existing_file_path = os.path.join(temp_dir, "existing_file")
    with open(existing_file_path, 'w') as f:
        f.write("This is an existing file.")
        
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture(scope="function")
def dest_file_name(temp_dir):
    dest_file_name = os.path.join(temp_dir, "new_test_template.yaml")
    yield dest_file_name
    if os.path.exists(dest_file_name):
        os.remove(dest_file_name)


@pytest.fixture(scope="module")
def template_file():
    template_yaml = (impresources.files(ukbutils.templates) / "TEMPLATE_config.yaml")
    return template_yaml


def test_parse_args(monkeypatch):
    monkeypatch.setattr("sys.argv", ["script_name", "dest_path"])
    args = get_template._parse_args()
    assert args.dest == "dest_path"


@pytest.mark.parametrize("path", ['~/example/relative_path', '~/relative path with spaces'])
def test_get_abs_path_relative_and_special(path):
    abs_path = get_template._get_abs_path(path)
    assert os.path.isabs(abs_path)


@pytest.mark.parametrize("path", ["/absolute/path", "/absolute/path with spaces"])
def test_get_abs_path_absolute(path):
    # Test with an absolute path
    abs_result = get_template._get_abs_path(path)
    assert abs_result == path


def test_path_is_safe_existing_path(temp_dir):
    dest = os.path.join(temp_dir, "existing_file")
    assert get_template._path_is_safe(dest) == False


def test_path_is_safe_new_path(temp_dir):
    dest = os.path.join(temp_dir, "non_existing_file")
    assert get_template._path_is_safe(dest) == True


def test_copy_template(temp_dir, dest_file_name):
    dest_file = os.path.join(temp_dir, dest_file_name)
    get_template._copy_template(dest_file)
    assert os.path.exists(dest_file)


def test_copy_template_content(temp_dir, template_file, dest_file_name):
    get_template._copy_template(dest_file_name)

    with open(template_file, mode="r") as f:
        template_content = f.read()

    with open (dest_file_name, mode="r") as f:
        dest_content = f.read()

    assert template_content == dest_content


def test_generate_template_file_exists(temp_dir):
    dest_file = os.path.join(temp_dir, "existing_file")
    # monkeypatch.setattr("sys.argv", ["script_name", dest_file])
    
    with pytest.raises(ValueError):
        get_template.generate_template(dest_file)
        # main_cli()


def test_generate_template_copy_template(dest_file_name):
    # monkeypatch.setattr("sys.argv", ["get_template.py", dest_file_name])
    # main_cli(dest_file_name)
    get_template.generate_template(dest_file_name)
    assert os.path.exists(dest_file_name)


def test_generate_template_content(template_file, dest_file_name):
    # monkeypatch.setattr("sys.argv", ["script_name", dest_file_name])
    # main_cli()
    get_template.generate_template(dest_file_name)

    with open(template_file, mode="r") as f:
        template_content = f.read()

    with open (dest_file_name, mode="r") as f:
        dest_content = f.read()

    assert template_content == dest_content


def test_main_cli(monkeypatch):
    generate_template_input = None

    def generate_template_patch(dest):
        nonlocal generate_template_input
        generate_template_input = dest

    # Monkeypatch cli input
    monkeypatch.setattr("sys.argv", ["script_name", "dest_path"])
    # Monkeypatch generate_template function to check what is passed on
    monkeypatch.setattr(get_template, "generate_template", generate_template_patch)
    
    get_template.main_cli()
    assert generate_template_input == "dest_path"
