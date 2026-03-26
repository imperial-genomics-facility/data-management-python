import os
import unittest
from unittest.mock import patch, MagicMock
from igf_airflow.utils.dag54_metadata_rehydrate_utils import (
    _get_project_igf_id_for_project_id,
    _remove_target_project_from_list,
    get_known_projects_func,
    _find_and_move_metadata_for_current_project,
    get_current_metadata_files_func
)
from igf_data.utils.fileutils import (
  get_temp_dir,
  remove_dir
)
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.igfTables import (
    Base,
    Project
)
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_airflow.utils.dag20_portal_metadata_utils import (
  _get_all_known_projects
)

MODULE_PATH = "igf_airflow.utils.dag54_metadata_rehydrate_utils"

class Test_dag54_metadata_rehydrate_utilsA(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.dbconfig = 'data/dbconfig.json'
    dbparam = read_dbconf_json(self.dbconfig)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class = base.get_session_class()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    remove_dir(self.temp_dir)

  def test_get_project_igf_id_for_project_id(self):
    project = Project(
      project_igf_id="AAABBB"
    )
    base = BaseAdaptor(
      **{"session_class": self.session_class}
    )
    base.start_session()
    base.session.add(project)
    base.session.flush()
    base.session.commit()
    project_name = _get_project_igf_id_for_project_id(
      project_id=project.project_id,
      database_config_file=self.dbconfig
    )
    assert project_name == "AAABBB"
    project_name = _get_project_igf_id_for_project_id(
      project_id=2,
      database_config_file=self.dbconfig
    )
    assert project_name is None


  def test_remove_target_project_from_list(self):
    project1 = Project(
      project_igf_id="AAABBB"
    )
    project2 = Project(
      project_igf_id="AAABBBC"
    )
    base = BaseAdaptor(
      **{"session_class": self.session_class}
    )
    base.start_session()
    base.session.add(project1)
    base.session.add(project2)
    base.session.flush()
    base.session.commit()
    project_list_file = _get_all_known_projects(
      db_conf_file=self.dbconfig
    )
    mod_project_list_file = _remove_target_project_from_list(
      project_list_file=project_list_file,
      project_name=project2.project_igf_id
    )
    with open(mod_project_list_file, "r") as fp:
      csv_data = fp.read()
    assert "AAABBB" in csv_data
    assert "AAABBBC" not in csv_data.split("\n")

  @patch(f"{MODULE_PATH}.get_current_context")
  @patch(f"{MODULE_PATH}.DATABASE_CONFIG_FILE", 'data/dbconfig.json')
  def test_get_known_projects_func(self, mock_context):
    mock_dagrun = MagicMock()
    mock_dagrun.dag_run.conf.project_id = 2
    mock_dagrun.get.return_value = mock_dagrun.dag_run
    mock_dagrun.dag_run.conf.get.return_value = 2
    mock_context.return_value = mock_dagrun
    project1 = Project(
      project_igf_id="AAABBB"
    )
    project2 = Project(
      project_igf_id="AAABBBC"
    )
    base = BaseAdaptor(
      **{"session_class": self.session_class}
    )
    base.start_session()
    base.session.add(project1)
    base.session.add(project2)
    base.session.flush()
    base.session.commit()
    base.close_session()
    # with patch(f"{MODULE_PATH}.DATABASE_CONFIG_FILE", 'data/dbconfig.json'):
    #   output_dict = get_known_projects_func.function()
    output_dict = get_known_projects_func.function()
    project_list = output_dict.get('known_projects')
    with open(project_list, "r") as fp:
      csv_data = fp.read()
    assert "AAABBB" in csv_data
    assert "AAABBBC" not in csv_data.split("\n")

  def test_find_and_move_metadata_for_current_project(self):
    metadata_dir = os.path.join(self.temp_dir, "metadat1")
    new_metadata_dir = os.path.join(self.temp_dir, "metadat2")
    os.makedirs(metadata_dir, exist_ok=True)
    os.makedirs(new_metadata_dir, exist_ok=True)
    with open(os.path.join(metadata_dir, "AAABBB_something.csv"), "w") as fp:
      fp.write("A")
    with open(os.path.join(metadata_dir, "AAABBBC_something.csv"), "w") as fp:
      fp.write("A")
    _find_and_move_metadata_for_current_project(
      metadata_dir=metadata_dir,
      project_name="AAABBBC",
      new_metadata_dir=new_metadata_dir
    )
    assert "AAABBBC_something.csv" in os.listdir(new_metadata_dir)
    assert "AAABBB_something.csv" not in os.listdir(new_metadata_dir)

if __name__=='__main__':
  unittest.main()