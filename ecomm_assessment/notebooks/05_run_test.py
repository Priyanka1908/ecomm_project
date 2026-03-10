# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

import sys
import os
import pytest

sys.dont_write_bytecode = True

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
project_root = "/Workspace" + os.path.dirname(os.path.dirname(notebook_path))
tests_path = project_root + "/tests"

sys.path.insert(0, project_root)

pytest.main([tests_path, "-v", "-p", "no:cacheprovider", "--import-mode=importlib"])