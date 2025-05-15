import os, unittest, json
from unittest.mock import patch
from igf_data.igfdb.igfTables import (
  Base,
  Project,
  User,
  Sample,
  ProjectUser,
  Project_attribute)
from igf_data.utils.fileutils import (
  get_temp_dir,
  remove_dir)
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.useradaptor import UserAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.process.seqrun_processing.unified_metadata_registration import (
  UnifiedMetadataRegistration,
  FetchNewMetadataCommand,
  CheckRawMetadataColumnsCommand,
  ValidateMetadataCommand,
  CheckAndRegisterMetadataCommand,
  SyncMetadataCommand,
  ChainCommand,
  MetadataContext,
  _get_db_session_class)

class TestUnifiedMetadataRegistrationA(unittest.TestCase):
  def setUp(self):
    self.portal_config_file = "test_config.json"
    self.fetch_metadata_url_suffix = "test_fetch_suffix"
    self.sync_metadata_url_suffix = "test_sync_suffix"
    self.metadata_validation_schema = "data/validation_schema/minimal_metadata_validation.json"
    self.db_config_file = 'data/dbconfig.json'
    self.table_columns = {
      "project": ["project_igf_id", "deliverable"],
      "project_user": ["project_igf_id", "email_id"],
      "user": ["name", "email_id", "username"],
      "sample": ["sample_igf_id", "project_igf_id"]}
    with open(self.db_config_file, 'r') as json_data:
      dbparam = json.load(json_data)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    Base.metadata.create_all(self.engine)
    self.session_class = base.session_class
    base.start_session()
    ua = UserAdaptor(**{'session':base.session})
    user_data = [
      {'name':'user1','email_id':'user1@ic.ac.uk','username':'user1'},
       {'name':'C','email_id':'c@c.com','username':'c'}]
    ua.store_user_data(data=user_data)
    project_data = [{
      'project_igf_id':'IGFP0001_test_22-8-2017_rna'}]
    pa = ProjectAdaptor(**{'session':base.session})
    pa.store_project_and_attribute_data(data=project_data)
    project_user_data = [{
      'project_igf_id':'IGFP0001_test_22-8-2017_rna',
      'email_id':'user1@ic.ac.uk',
      'data_authority': True}]
    pa.assign_user_to_project(data=project_user_data)
    sample_data = [
      {'sample_igf_id':'IGF00001','project_igf_id':'IGFP0001_test_22-8-2017_rna',},
      {'sample_igf_id':'IGF00002','project_igf_id':'IGFP0001_test_22-8-2017_rna',},
      {'sample_igf_id':'IGF00003','project_igf_id':'IGFP0001_test_22-8-2017_rna',},
      {'sample_igf_id':'IGF00004','project_igf_id':'IGFP0001_test_22-8-2017_rna',},
      {'sample_igf_id':'IGF00005','project_igf_id':'IGFP0001_test_22-8-2017_rna',}]
    sa = SampleAdaptor(**{'session':base.session})
    sa.store_sample_and_attribute_data(data=sample_data)
    base.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)


  def test_MetadataContext(self):
    metadata_context = MetadataContext(
      portal_config_file=self.portal_config_file,
      fetch_metadata_url_suffix=self.fetch_metadata_url_suffix,
      sync_metadata_url_suffix=self.sync_metadata_url_suffix,
      metadata_validation_schema=self.metadata_validation_schema,
      db_config_file=self.db_config_file,
      )
    self.assertIsInstance(metadata_context, MetadataContext)

  @patch("igf_data.process.seqrun_processing.unified_metadata_registration.get_data_from_portal", return_value={})
  def test_FetchNewMetadataCommand1(self, *args):
    fetch_command = FetchNewMetadataCommand()
    self.assertIsInstance(fetch_command, FetchNewMetadataCommand)
    metadata_context = MetadataContext(
      portal_config_file=self.portal_config_file,
      fetch_metadata_url_suffix=self.fetch_metadata_url_suffix,
      sync_metadata_url_suffix=self.sync_metadata_url_suffix,
      metadata_validation_schema=self.metadata_validation_schema,
      db_config_file=self.db_config_file,
      )
    fetch_command.execute(metadata_context=metadata_context)
    self.assertFalse(
      metadata_context.metadata_fetched)

  @patch("igf_data.process.seqrun_processing.unified_metadata_registration.get_data_from_portal", return_value={"1": [{"project_igf_id": "A"}]})
  def test_FetchNewMetadataCommand2(self, *args):
    fetch_command = FetchNewMetadataCommand()
    self.assertIsInstance(fetch_command, FetchNewMetadataCommand)
    metadata_context = MetadataContext(
      portal_config_file=self.portal_config_file,
      fetch_metadata_url_suffix=self.fetch_metadata_url_suffix,
      sync_metadata_url_suffix=self.sync_metadata_url_suffix,
      metadata_validation_schema=self.metadata_validation_schema,
      db_config_file=self.db_config_file,
      )
    fetch_command.execute(metadata_context=metadata_context)
    self.assertTrue(
      metadata_context.metadata_fetched)
    self.assertEqual(
      metadata_context.raw_metadata_dict,
      {"1": [{"project_igf_id": "A"}]})


  def test_CheckRawMetadataColumnsCommand_check_columns(self):
    table_columns_dict = {
      "project": ["project_igf_id"],
      "project_user": ["project_igf_id", "email_id"],
      "user": ["name", "email_id", "username"],
      "sample": ["sample_igf_id", "project_igf_id"]}
    metadata_columns = ["project_igf_id", "name", "email_id", "username", "sample_igf_id"]
    sample_required = True
    errors = CheckRawMetadataColumnsCommand._check_columns(
      metadata_columns=metadata_columns,
      table_columns_dict=table_columns_dict,
      sample_required=sample_required)
    self.assertEqual(len(errors), 0)
    metadata_columns = ["project_igf_id", "name", "email_id", "username"]
    sample_required = False
    errors = CheckRawMetadataColumnsCommand._check_columns(
      metadata_columns=metadata_columns,
      table_columns_dict=table_columns_dict,
      sample_required=sample_required)
    self.assertEqual(len(errors), 0)
    metadata_columns = ["project_igf_id", "name", "email_id"]
    sample_required = False
    errors = CheckRawMetadataColumnsCommand._check_columns(
      metadata_columns=metadata_columns,
      table_columns_dict=table_columns_dict,
      sample_required=sample_required)
    self.assertEqual(len(errors), 1)
    self.assertEqual(errors[0], f"Missing required columns in user table: {table_columns_dict.get('user')}")
    metadata_columns = ["project_igf_id", "name", "email_id", "username"]
    sample_required = True
    errors = CheckRawMetadataColumnsCommand._check_columns(
      metadata_columns=metadata_columns,
      table_columns_dict=table_columns_dict,
      sample_required=sample_required)
    self.assertEqual(len(errors), 1)
    self.assertEqual(errors[0], f"Missing required columns in sample table: {table_columns_dict.get('sample')}")

  def test_CheckRawMetadataColumnsCommand_execute(self):
    metadata_context = MetadataContext(
      portal_config_file=self.portal_config_file,
      fetch_metadata_url_suffix=self.fetch_metadata_url_suffix,
      sync_metadata_url_suffix=self.sync_metadata_url_suffix,
      metadata_validation_schema=self.metadata_validation_schema,
      db_config_file=self.db_config_file,
      metadata_fetched=True,
      raw_metadata_dict={}
      )
    CheckRawMetadataColumnsCommand().execute(metadata_context=metadata_context)
    self.assertEqual(metadata_context.checked_required_column_dict, {})
    self.assertEqual(len(metadata_context.error_list), 0)
    metadata_context.raw_metadata_dict = {
      1: [{"project_igf_id": "A", "deliverable": "COSMX", "name": "B", "email_id": "C", "username": "D", "sample_igf_id": "E"}],
      2: [{"project_igf_id": "A", "deliverable": "COSMX", "name": "B", "email_id": "C", "sample_igf_id": "E"}]
    }
    CheckRawMetadataColumnsCommand().execute(metadata_context=metadata_context)
    self.assertEqual(len(metadata_context.checked_required_column_dict), 2)
    self.assertTrue(1 in metadata_context.checked_required_column_dict)
    self.assertTrue(2 in metadata_context.checked_required_column_dict)
    self.assertEqual(metadata_context.checked_required_column_dict.get(1), True)
    self.assertEqual(metadata_context.checked_required_column_dict.get(2), False)
    self.assertEqual(len(metadata_context.error_list), 1)
    metadata_context = MetadataContext(
      portal_config_file=self.portal_config_file,
      fetch_metadata_url_suffix=self.fetch_metadata_url_suffix,
      sync_metadata_url_suffix=self.sync_metadata_url_suffix,
      metadata_validation_schema=self.metadata_validation_schema,
      db_config_file=self.db_config_file,
      metadata_fetched=True,
      raw_metadata_dict={1: [{"project_igf_id": "A", "deliverable": "COSMX", "name": "B", "email_id": "C", "username": "D"}]},
      samples_required=False)
    CheckRawMetadataColumnsCommand().execute(metadata_context=metadata_context)
    self.assertEqual(len(metadata_context.checked_required_column_dict), 1)
    self.assertTrue(1 in metadata_context.checked_required_column_dict)
    self.assertEqual(metadata_context.checked_required_column_dict.get(1), True)


  def test_ValidateMetadataCommand_validate_metadata(self):
    validate = ValidateMetadataCommand()
    errors = \
      validate.\
        _validate_metadata(
          metadata_entry=[{
            "project_igf_id": "IGF001",
            "deliverable": "COSMX",
            "name": "Ba Da",
            "email_id": "c@d.com",
            "username": "aaa",
            "sample_igf_id": "IGF001"}],
          metadata_validation_schema=self.metadata_validation_schema,
          metadata_id=1)
    self.assertEqual(len(errors), 0)
    errors = \
      validate.\
        _validate_metadata(
          metadata_entry=[{
            "project_igf_id": "IGF001",
            "deliverable": "COSMX",
            "name": "Ba Da",
            "email_id": "c@d.com",
            "username": "aaa"}],
          metadata_validation_schema=self.metadata_validation_schema,
          metadata_id=1)
    self.assertEqual(len(errors), 0)
    errors = \
      validate.\
        _validate_metadata(
          metadata_entry=[{
            "project_igf_id": "IGF001",
            "deliverable": "COSMX",
            "name": "Ba Da",
            "email_id": "c-d.com",
            "username": "aaa"}],
          metadata_validation_schema=self.metadata_validation_schema,
          metadata_id=1)
    self.assertEqual(len(errors), 1)

  def test_ValidateMetadataCommand_execute(self):
    validate = ValidateMetadataCommand()
    metadata_context = MetadataContext(
      portal_config_file=self.portal_config_file,
      fetch_metadata_url_suffix=self.fetch_metadata_url_suffix,
      sync_metadata_url_suffix=self.sync_metadata_url_suffix,
      metadata_validation_schema=self.metadata_validation_schema,
      db_config_file=self.db_config_file,
      error_list=[],
      raw_metadata_dict={
        1: [{
          "project_igf_id": "IGF001",
          "deliverable": "COSMX",
          "name": "Ba Da",
          "email_id": "c-d.com",
          "username": "aaa"}],
        2: [{
          "project_igf_id": "IGF001",
          "deliverable": "COSMX",
          "name": "Ba Da",
          "username": "aaa"}]},
      checked_required_column_dict={1: True, 2: False})
    validate.execute(metadata_context=metadata_context)
    self.assertEqual(len(metadata_context.error_list), 1)
    validated_metadata_dict = metadata_context.validated_metadata_dict
    self.assertEqual(len(validated_metadata_dict), 1)
    self.assertTrue(1 in validated_metadata_dict)
    self.assertFalse(validated_metadata_dict.get(1))


  def test_CheckAndRegisterMetadataCommand_split_metadata(self):
    metadata_context = MetadataContext(
      portal_config_file=self.portal_config_file,
      fetch_metadata_url_suffix=self.fetch_metadata_url_suffix,
      sync_metadata_url_suffix=self.sync_metadata_url_suffix,
      metadata_validation_schema=self.metadata_validation_schema,
      db_config_file=self.db_config_file,
      error_list=[],
      raw_metadata_dict={
        1: [{
          "project_igf_id": "IGF001",
          "deliverable": "COSMX",
          "name": "Ba Da",
          "email_id": "c@d.com",
          "sample_igf_id": "IGF0012",
          "username": "aaa"}],
        2: [{
          "project_igf_id": "IGF001",
          "deliverable": "COSMX",
          "name": "Ba Da",
          "username": "aaa"}]},
      checked_required_column_dict={1: True, 2: False},
      validated_metadata_dict={1: True})
    check_and_register = CheckAndRegisterMetadataCommand()
    (project_metadata_list,
     user_metadata_list,
     project_user_metadata,
     sample_metadata_list) = \
      check_and_register._split_metadata(
        metadata_entry=metadata_context.raw_metadata_dict.get(1),
        table_columns=self.table_columns,
        samples_required=True)
    self.assertEqual(len(project_metadata_list), 1)
    self.assertEqual(len(project_user_metadata), 1)
    self.assertEqual(len(user_metadata_list), 1)
    self.assertEqual(len(sample_metadata_list), 1)
    self.assertEqual(
      project_metadata_list,
        [{"project_igf_id": "IGF001", "deliverable": "COSMX"}])
    (_, _, _, sample_metadata_list) = \
      check_and_register._split_metadata(
        metadata_entry=metadata_context.raw_metadata_dict.get(1),
        table_columns=self.table_columns,
        samples_required=False)
    self.assertEqual(len(sample_metadata_list), 0)


  def test_CheckAndRegisterMetadataCommand_check_and_filter_existing_metadata(self):
    session_class = \
      _get_db_session_class(
        db_config_file=self.db_config_file)
    check_and_register = \
      CheckAndRegisterMetadataCommand()
    (filtered_project_metadata,
     filtered_user_metadata,
     filtered_project_user_metadata,
     filtered_sample_metadata,
     _) = \
      check_and_register.\
        _check_and_filter_existing_metadata(
          project_metadata_list=[
            {"project_igf_id": "IGFP0001_test_22-8-2017_rna"},
            {"project_igf_id": "IGFP0002_test_22-8-2017_rna"}],
          user_metadata_list=[
            {"name": "DB DB", "email_id": "d@b.com", "username": "db"},
            {"name": "user1", "email_id": "user1@ic.ac.uk", "username": "user1"}],
          project_user_metadata_list=[
            {"project_igf_id": "IGFP0001_test_22-8-2017_rna", "email_id": "user1@ic.ac.uk"},
            {"project_igf_id": "IGFP0002_test_22-8-2017_rna", "email_id": "d@b.com"}],
          sample_metadata_list=[{'sample_igf_id': 'IGF00001', "project_igf_id": "IGFP0001_test_22-8-2017_rna"}],
          session_class=session_class)
    self.assertEqual(len(filtered_project_metadata), 1)
    self.assertEqual(len(filtered_user_metadata), 1)
    self.assertEqual(len(filtered_project_user_metadata), 1)
    self.assertEqual(len(filtered_sample_metadata), 0)
    self.assertEqual(
      filtered_project_metadata,
        [{"project_igf_id": "IGFP0002_test_22-8-2017_rna"}])
    self.assertEqual(
      filtered_user_metadata,
        [{"name": "DB DB", "email_id": "d@b.com", "username": "db"}])
    self.assertEqual(
      filtered_project_user_metadata,
        [{"project_igf_id": "IGFP0002_test_22-8-2017_rna", "email_id": "d@b.com"}])

  def test_CheckAndRegisterMetadataCommand_register_update_projectuser_metadata(self):
    check_and_register = \
      CheckAndRegisterMetadataCommand()
    updated_project_user_dataset = \
      check_and_register.\
        _update_projectuser_metadata(
          project_user_metadata_list=[
            {"project_igf_id": "IGFP0002_test_22-8-2017_rna", "email_id": "d@b.com"},
            {"project_igf_id": "IGFP0002_test_22-8-2017_rna", "email_id": "a@a.com"},
            {"project_igf_id": "IGFP0003_test_22-8-2017_rna", "email_id": "a@a.com"}],
          secondary_user_email='c@c.com')
    self.assertEqual(len(updated_project_user_dataset), 5)
    self.assertTrue('data_authority' in updated_project_user_dataset[0])
    self.assertTrue([e['data_authority'] for e in updated_project_user_dataset \
                      if e["project_igf_id"]=="IGFP0002_test_22-8-2017_rna" and e["email_id"]=="d@b.com"][0])
    self.assertFalse([e['data_authority'] for e in updated_project_user_dataset \
                      if e["project_igf_id"]=="IGFP0002_test_22-8-2017_rna" and e["email_id"]=="a@a.com"][0])
    self.assertTrue("c@c.com" in [e["email_id"] for e in updated_project_user_dataset \
                      if e["project_igf_id"]=="IGFP0002_test_22-8-2017_rna"])
    self.assertFalse([e['data_authority'] for e in updated_project_user_dataset \
                      if e["project_igf_id"]=="IGFP0002_test_22-8-2017_rna" and e["email_id"]=="c@c.com"][0])
    self.assertTrue([e['data_authority'] for e in updated_project_user_dataset \
                      if e["project_igf_id"]=="IGFP0003_test_22-8-2017_rna" and e["email_id"]=="a@a.com"][0])



  def test_CheckAndRegisterMetadataCommand_register_new_metadata(self):
    session_class = \
      _get_db_session_class(
        db_config_file=self.db_config_file)
    check_and_register = \
      CheckAndRegisterMetadataCommand()
    updated_project_user_metadata = \
      check_and_register.\
        _update_projectuser_metadata(
          project_user_metadata_list=[
            {"project_igf_id": "IGFP0002_test_22-8-2017_rna", "email_id": "d@b.com"}],
          secondary_user_email='c@c.com')
    status, _ = \
      check_and_register.\
        _register_new_metadata(
          project_metadata_list=[
            {"project_igf_id": "IGFP0002_test_22-8-2017_rna"}],
          user_metadata_list=[
            {"name": "DB DB", "email_id": "d@b.com", "username": "db"}],
          project_user_metadata_list=updated_project_user_metadata,
          sample_metadata_list=[
            {'sample_igf_id': 'IGF00006', "project_igf_id": "IGFP0002_test_22-8-2017_rna"}],
          session_class=session_class)
    self.assertEqual(status, True)
    updated_project_user_metadata = \
      check_and_register.\
        _update_projectuser_metadata(
          project_user_metadata_list=[
            {"project_igf_id": "IGFP0003_test_22-8-2017_rna", "email_id": "e@e.com"}],
          secondary_user_email='c@c.com')
    status, _ = \
      check_and_register.\
        _register_new_metadata(
          project_metadata_list=[
            {"project_igf_id": "IGFP0003_test_22-8-2017_rna"}],
          user_metadata_list=[
            {"name": "EE", "email_id": "e@e.com", "username": "ee"}],
          project_user_metadata_list=updated_project_user_metadata,
          sample_metadata_list=[],
          session_class=session_class)
    self.assertEqual(status, True)
    base = \
      BaseAdaptor(**{'session_class':self.session_class})
    base.start_session()
    query = \
      base.session.\
      query(Project.project_igf_id).\
      filter(Project.project_igf_id=="IGFP0002_test_22-8-2017_rna")
    records = \
      base.fetch_records(query=query, output_mode="one_or_none")
    self.assertIsNotNone(records)
    query = \
      base.session.\
      query(Project.project_igf_id).\
      filter(Project.project_igf_id=="IGFP0003_test_22-8-2017_rna")
    records = \
      base.fetch_records(query=query, output_mode="one_or_none")
    self.assertIsNotNone(records)
    query = \
      base.session.\
      query(User.email_id).\
      filter(User.email_id=="d@b.com")
    records = \
      base.fetch_records(query=query, output_mode="one_or_none")
    self.assertIsNotNone(records)
    query = \
      base.session.\
      query(ProjectUser).\
      join(Project, Project.project_id==ProjectUser.project_id).\
      join(User, User.user_id==ProjectUser.user_id).\
      filter(Project.project_igf_id=="IGFP0002_test_22-8-2017_rna").\
      filter(ProjectUser.data_authority=='T').\
      filter(User.email_id=="d@b.com")
    records = \
      base.fetch_records(query=query, output_mode="one_or_none")
    self.assertIsNotNone(records)
    query = \
      base.session.\
      query(ProjectUser).\
      join(Project, Project.project_id==ProjectUser.project_id).\
      join(User, User.user_id==ProjectUser.user_id).\
      filter(Project.project_igf_id=="IGFP0002_test_22-8-2017_rna").\
      filter(User.email_id=="c@c.com")
    records = \
      base.fetch_records(query=query, output_mode="one_or_none")
    self.assertIsNotNone(records)
    self.assertIsNone(records.data_authority)
    query = \
      base.session.\
      query(Sample.sample_igf_id).\
      join(Project, Project.project_id==Sample.project_id).\
      filter(Project.project_igf_id=="IGFP0002_test_22-8-2017_rna").\
      filter(Sample.sample_igf_id=='IGF00006')
    records = \
      base.fetch_records(query=query, output_mode="one_or_none")
    self.assertIsNotNone(records)

  def test_CheckAndRegisterMetadataCommand_execute(self):
    session_class = \
      _get_db_session_class(
        db_config_file=self.db_config_file)
    check_and_register = \
      CheckAndRegisterMetadataCommand()
    metadata_context = MetadataContext(
      portal_config_file=self.portal_config_file,
      fetch_metadata_url_suffix=self.fetch_metadata_url_suffix,
      sync_metadata_url_suffix=self.sync_metadata_url_suffix,
      metadata_validation_schema=self.metadata_validation_schema,
      db_config_file=self.db_config_file,
      default_project_user_email='c@c.com',
      samples_required=False,
      error_list=[],
      raw_metadata_dict={
        1: [{
          "project_igf_id": "IGFP0004_test_22-8-2017_rna",
          "deliverable": "COSMX",
          "name": "Ga Ga",
          "email_id": "g@g.com",
          "username": "ggg"}],
        2: [{
          "project_igf_id": "IGF001",
          "deliverable": "COSMX",
          "name": "Ba Da",
          "username": "aaa"}]},
      validated_metadata_dict={1: True, 2: False})
    check_and_register.\
      execute(metadata_context=metadata_context)
    self.assertEqual(len(metadata_context.registered_metadata_dict), 2)
    self.assertTrue(metadata_context.registered_metadata_dict.get(1))
    self.assertFalse(metadata_context.registered_metadata_dict.get(2))
    base = \
      BaseAdaptor(**{'session_class':self.session_class})
    base.start_session()
    query = \
      base.session.\
      query(Project.project_igf_id).\
      filter(Project.project_igf_id=="IGFP0004_test_22-8-2017_rna")
    records = \
      base.fetch_records(query=query, output_mode="one_or_none")
    self.assertIsNotNone(records)
    query = \
      base.session.\
      query(Project).\
      filter(Project.project_igf_id=="IGFP0004_test_22-8-2017_rna")
    records = \
      base.fetch_records(query=query, output_mode="one_or_none")
    self.assertEqual(records.deliverable, 'COSMX')
    self.assertIsNotNone(records)
    query = \
      base.session.\
      query(User.email_id).\
      filter(User.email_id=="g@g.com")
    records = \
      base.fetch_records(query=query, output_mode="one_or_none")
    self.assertIsNotNone(records)
    query = \
      base.session.\
      query(ProjectUser).\
      join(Project, Project.project_id==ProjectUser.project_id).\
      join(User, User.user_id==ProjectUser.user_id).\
      filter(Project.project_igf_id=="IGFP0004_test_22-8-2017_rna").\
      filter(ProjectUser.data_authority=='T').\
      filter(User.email_id=="g@g.com")
    records = \
      base.fetch_records(query=query, output_mode="one_or_none")
    self.assertIsNotNone(records)
    query = \
      base.session.\
      query(ProjectUser).\
      join(Project, Project.project_id==ProjectUser.project_id).\
      join(User, User.user_id==ProjectUser.user_id).\
      filter(Project.project_igf_id=="IGFP0004_test_22-8-2017_rna").\
      filter(User.email_id=="c@c.com")
    records = \
      base.fetch_records(query=query, output_mode="one_or_none")
    self.assertIsNotNone(records)
    self.assertIsNone(records.data_authority)


  @patch("igf_data.process.seqrun_processing.unified_metadata_registration.get_data_from_portal", return_value=True)
  def test_SyncMetadataCommand_execute(self, *args):
    metadata_context = MetadataContext(
      portal_config_file=self.portal_config_file,
      fetch_metadata_url_suffix=self.fetch_metadata_url_suffix,
      sync_metadata_url_suffix=self.sync_metadata_url_suffix,
      metadata_validation_schema=self.metadata_validation_schema,
      db_config_file=self.db_config_file,
      default_project_user_email='c@c.com',
      samples_required=False,
      error_list=[],
      registered_metadata_dict={1: True, 2: False})
    sync_metadata = SyncMetadataCommand()
    sync_metadata.execute(metadata_context=metadata_context)
    self.assertEqual(len(metadata_context.synced_metadata_dict), 2)
    self.assertTrue(1 in metadata_context.synced_metadata_dict)
    self.assertTrue(2 in metadata_context.synced_metadata_dict)
    self.assertTrue(metadata_context.synced_metadata_dict.get(1))
    self.assertFalse(metadata_context.synced_metadata_dict.get(2))

class TestUnifiedMetadataRegistrationB(unittest.TestCase):
  def setUp(self):
    self.portal_config_file = "test_config.json"
    self.fetch_metadata_url_suffix = "test_fetch_suffix"
    self.sync_metadata_url_suffix = "test_sync_suffix"
    self.metadata_validation_schema = "data/validation_schema/minimal_metadata_validation.json"
    self.db_config_file = 'data/dbconfig.json'
    self.table_columns = {
      "project": ["project_igf_id", "deliverable"],
      "project_user": ["project_igf_id", "email_id"],
      "user": ["name", "email_id", "username"],
      "sample": ["sample_igf_id", "project_igf_id"]}
    with open(self.db_config_file, 'r') as json_data:
      dbparam = json.load(json_data)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    Base.metadata.create_all(self.engine)
    self.session_class = base.session_class
    base.start_session()
    ua = UserAdaptor(**{'session':base.session})
    user_data = [
      {'name':'C','email_id':'c@c.com','username':'c'}]
    ua.store_user_data(data=user_data)
    base.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  @patch("igf_data.process.seqrun_processing.unified_metadata_registration.get_data_from_portal", return_value={
      1: [{"project_igf_id": "IGFA001", "deliverable": "COSMX", "name": "User B", "email_id": "a@b.com", "username": "aaaa"}],
      2: [{"project_igf_id": "IGFA002", "deliverable": "COSMX", "name": "User C", "email_id": "a@c.com"}],
      3: [{"project_igf_id": "IGFA003", "deliverable": "COSMX", "name": "User D", "email_id": "a-c.com", "username": "ddd"}]})
  def test_UnifiedMetadataRegistration_execute(self, *args):
    metadata_registration = UnifiedMetadataRegistration(
      portal_config_file=self.portal_config_file,
      fetch_metadata_url_suffix=self.fetch_metadata_url_suffix,
      sync_metadata_url_suffix=self.sync_metadata_url_suffix,
      metadata_validation_schema=self.metadata_validation_schema,
      db_config_file=self.db_config_file,
      default_project_user_email='c@c.com',
      samples_required=False,)
    metadata_registration.execute()
    


if __name__ == '__main__':
  unittest.main()