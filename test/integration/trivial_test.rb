require "test_helper"

class TrivialIntegrationTests < GhostferryTestCase
  def test_copy_data_without_any_writes_to_source
    seed_simple_database_with_single_table

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)
    ghostferry.run

    assert_test_table_is_identical
  end

  def test_copy_data_with_writes_to_source
    seed_simple_database_with_single_table

    datawriter = new_source_datawriter
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    start_datawriter_with_ghostferry(datawriter, ghostferry)
    stop_datawriter_during_cutover(datawriter, ghostferry)

    ghostferry.run
    assert_test_table_is_identical
  end

  def test_logged_query_omits_columns
    seed_simple_database_with_single_table

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)
    ghostferry.run

    assert ghostferry.logrus_lines["cursor"].length > 0

    ghostferry.logrus_lines["cursor"].each do |line|
      if line["msg"].start_with?("found ")
        assert line["sql"].start_with?("SELECT [omitted] FROM")
      end
    end
  end

  def test_copy_table_with_string_primary_key
    number_of_rows = 123
    dbtable = full_table_name(DEFAULT_DB, DEFAULT_TABLE)
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{dbtable} (id varchar(20) not null, data TEXT, primary key(id))")
    end

    transaction(source_db) do
      insert_statement = source_db.prepare("INSERT INTO #{dbtable} (id, data) VALUES (?, ?)")

      number_of_rows.times do |i|
        insert_statement.execute("row_#{i}", rand_data)
      end
    end

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)
    ghostferry.run
    assert_test_table_is_identical
  end

  def test_copy_table_with_composite_primary_key
    number_of_rows = 123
    dbtable = full_table_name(DEFAULT_DB, DEFAULT_TABLE)
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{dbtable} (id varchar(20) not null, id2 int, data TEXT, primary key(id, id2))")
    end

    transaction(source_db) do
      insert_statement = source_db.prepare("INSERT INTO #{dbtable} (id, id2, data) VALUES (?, ?, ?)")

      number_of_rows.times do |i|
        insert_statement.execute("row_#{i}", i, rand_data)
      end
    end

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)
    ghostferry.run
    assert_test_table_is_identical
  end
end
