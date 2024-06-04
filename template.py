import os
from pathlib import Path

package_name = "db_connect"

list_of_files = [
   ".github/workflows/ci.yaml",
   "src/__init__.py",
   f"src/{package_name}/mongodb_connect/mongo_crud.py", 
   f"src/{package_name}/mongodb_connect/__init__.py", 
   f"src/{package_name}/postgreysqlconnector/postgreysql_crud.py", 
   f"src/{package_name}/postgreysqlconnector/__init__.py", 
   "tests/__init__.py",
   "tests/unit/__init__.py",
   "tests/unit/test_int.py",
   "tests/integration/__init__.py",
   "tests/integration/test_unit.py",
   "init_setup.sh",
   "requirements.txt", 
   "setup.py",
   "setup.cfg",
   "pyproject.toml",
   "tox.ini",
   "experiments/experiments.ipynb", 
]

for filepath in list_of_files:
    filepath = Path(filepath)
    filedir, filename = os.path.split(filepath)
    if filedir != "":
        os.makedirs(filedir, exist_ok=True)

    if (not os.path.exists(filepath)) or (os.path.getsize(filepath) == 0):
        with open(filepath, "w") as f:
            pass # create an empty file

#its updated