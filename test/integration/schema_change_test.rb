require "test_helper"

class SchemaChangeIntegrationTests < GhostferryTestCase
  def test_create_table_during_cutover_phase
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
    end

    ghostferry = new_altering_ghostferry

    row_copy_called = false
    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      source_db.query("CREATE TABLE #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data int(11), primary key(id))")
      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data) VALUES (1, 2)")
      row_copy_called = true
    end

    ghostferry.run

    assert row_copy_called
    assert_test_table_is_identical

    state_table_prefix = "#{DEFAULT_STATE_DB}._ghostferry_#{DEFAULT_SERVER_ID}_"
    res = target_db.query("SELECT table_name, copy_complete FROM #{state_table_prefix}_row_copy_state")
    assert_equal 1, res.count
    res.each do |row|
      assert_equal "#{DEFAULT_DB}.#{DEFAULT_TABLE}", row["table_name"]
      assert_equal 1, row["copy_complete"]
    end
  end

  def test_alter_table_during_cutover_phase
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data1 int(11), primary key(id))")
    end

    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data1) VALUES (1, 2)")

    ghostferry = new_altering_ghostferry

    row_copy_called = false
    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      res = target_db.query("SELECT * FROM #{DEFAULT_FULL_TABLE_NAME}")
      assert_equal 1, res.count
      res.each do |row|
        assert_equal 1, row["id"]
        assert_equal 2, row["data1"]
      end

      source_db.query("ALTER TABLE #{DEFAULT_FULL_TABLE_NAME} ADD COLUMN data2 int(11) default null")
      # trigger an insert that would fail on the target, if the table is not
      # migrated successfully before the insert
      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data1, data2) VALUES (4, 5, 6)")
      source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data2=3 where id=1")

      row_copy_called = true
    end

    ghostferry.run

    assert row_copy_called
    assert_test_table_is_identical

    res = target_db.query("SELECT * FROM #{DEFAULT_FULL_TABLE_NAME}")
    assert_equal 2, res.count
    res.each do |row|
      if row["id"] == 1
        assert_equal 2, row["data1"]
        assert_equal 3, row["data2"]
      else
        assert_equal 4, row["id"]
        assert_equal 5, row["data1"]
        assert_equal 6, row["data2"]
      end
    end
  end

  def test_rename_table_during_cutover_phase
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data int(11), primary key(id))")
    end

    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data) VALUES (1, 2)")
    table_name_renamed = "#{DEFAULT_TABLE}_renamed"
    full_quoted_table_name = full_table_name(DEFAULT_DB, table_name_renamed)

    ghostferry = new_altering_ghostferry

    row_copy_called = false
    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      source_db.query("RENAME TABLE #{DEFAULT_FULL_TABLE_NAME} TO #{full_quoted_table_name}")
      source_db.query("INSERT INTO #{full_quoted_table_name} (id, data) VALUES (3, 4)")
      source_db.query("DELETE FROM #{full_quoted_table_name} WHERE id=1")

      row_copy_called = true
    end

    ghostferry.run

    assert row_copy_called

    res = target_db.query("SELECT * FROM #{full_quoted_table_name}")
    assert_equal 1, res.count
    res.each do |row|
      assert_equal 3, row["id"]
      assert_equal 4, row["data"]
    end

    state_table_prefix = "#{DEFAULT_STATE_DB}._ghostferry_#{DEFAULT_SERVER_ID}_"
    res = target_db.query("SELECT table_name, copy_complete FROM #{state_table_prefix}_row_copy_state")
    assert_equal 2, res.count
    res.each do |row|
      assert_includes ["#{DEFAULT_DB}.#{DEFAULT_TABLE}", "#{DEFAULT_DB}.#{table_name_renamed}"], row["table_name"]
      assert_equal 1, row["copy_complete"]
    end
  end

  def test_truncate_table_during_cutover_phase
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data int(11), primary key(id))")
    end

    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data) VALUES (1, 2)")

    ghostferry = new_altering_ghostferry

    row_copy_called = false
    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      res = target_db.query("SELECT * FROM #{DEFAULT_FULL_TABLE_NAME}")
      assert_equal 1, res.count

      source_db.query("TRUNCATE TABLE #{DEFAULT_FULL_TABLE_NAME}")
      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data) VALUES (3, 4)")
      # this should work but be a NOP
      source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data=999 where id=1")
      # this would only work if we really truncated correctly (as the primary
      # key is reused)
      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data) VALUES (1, 5)")

      row_copy_called = true
    end

    ghostferry.run

    assert row_copy_called
    assert_test_table_is_identical

    res = target_db.query("SELECT * FROM #{DEFAULT_FULL_TABLE_NAME}")
    assert_equal 2, res.count
    res.each do |row|
      if row["id"] == 1
        assert_equal 5, row["data"]
      else
        assert_equal 3, row["id"]
        assert_equal 4, row["data"]
      end
    end
  end

  def test_drop_table_during_cutover_phase
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data int(11), primary key(id))")
    end

    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data) VALUES (1, 2)")

    ghostferry = new_altering_ghostferry

    row_copy_called = false
    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      res = target_db.query("SELECT * FROM #{DEFAULT_FULL_TABLE_NAME}")
      assert_equal 1, res.count

      source_db.query("DROP TABLE #{DEFAULT_FULL_TABLE_NAME}")

      row_copy_called = true
    end

    ghostferry.run

    assert row_copy_called

    res = target_db.query("SHOW TABLES IN #{DEFAULT_DB}")
    assert_equal 0, res.count
  end

  def test_alter_table_during_copy_phase
    # NOTE: Verifying that the binlog writing was delayed until copying has
    # completed is incredibly difficult to test without being prone to races.
    # This test simply triggers the paths involved with the delaying, but the
    # logic for verifying that the correct path was chosen is slim.
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data1 int(11), primary key(id))")
    end

    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data1) VALUES (1, 2)")
    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data1) VALUES (3, 4)")

    ghostferry = new_altering_ghostferry

    ghostferry.on_status(Ghostferry::Status::BINLOG_STREAMING_STARTED) do
      source_db.query("ALTER TABLE #{DEFAULT_FULL_TABLE_NAME} ADD COLUMN data2 int(11) DEFAULT 0")
      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data1, data2) VALUES (5, 6, 7)")
    end

    ghostferry.run

    assert first_row_copied

    res = target_db.query("SHOW TABLES IN #{DEFAULT_DB}")
    assert_equal 1, res.count

    res = target_db.query("SHOW CREATE TABLE #{DEFAULT_FULL_TABLE_NAME}")
    puts res
  end

  private

  def new_altering_ghostferry()
    config = {
        replicate_schema_changes: true,
        resume_state_from_db: DEFAULT_STATE_DB,
        verifier_type: "NoVerification",
    }
    return new_ghostferry(MINIMAL_GHOSTFERRY, config: config)
  end
end
