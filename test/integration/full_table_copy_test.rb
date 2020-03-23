require "test_helper"

class FullTableCopyTest < GhostferryTestCase
  def test_reject_table_with_string_primary_key
    define_test_table_with_data("data varchar(32), primary key(data)")

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)
    _, stderr = ghostferry.run_expecting_crash
    assert_includes stderr, "panic: Pagination Key `data` for `gftest`.`test_table_1` is non-numeric"
  end

  def test_reject_table_without_primary_key
    define_test_table_with_data("data varchar(32)")

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)
    _, stderr = ghostferry.run_expecting_crash
    assert_includes stderr, "panic: `gftest`.`test_table_1` has no Primary Key to default to for Pagination purposes"
  end

  def test_copy_table_with_string_primary_key
    define_test_table_with_data("data varchar(32), primary key(data)")

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: {
        cascading_pagination_column_config: {
            FullTableCopies: {
                DEFAULT_DB => [
                    DEFAULT_TABLE,
                ],
            },
        }.to_json,
    })
    ghostferry.run

    assert_test_table_is_identical
  end

  def test_copy_table_without_primary_key
    define_test_table_with_data("data varchar(32)")

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: {
        cascading_pagination_column_config: {
            FullTableCopies: {
                DEFAULT_DB => [
                    DEFAULT_TABLE,
                ],
            },
        }.to_json,
    })
    ghostferry.run

    assert_test_table_is_identical
  end

  private

  def define_test_table_with_data(column_definition)
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (#{column_definition})")
    end

    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (data) VALUES ('data1'), ('data2')")
  end
end
