require "test_helper"

require "json"

class InterruptResumeTest < GhostferryTestCase
  def setup
    seed_simple_database_with_single_table
  end

  def test_interrupt_resume_without_writes_to_source_to_check_target_state_when_interrupted
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    # Writes one batch
    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      ghostferry.send_signal("TERM")
    end

    dumped_state = ghostferry.run_expecting_interrupt
    assert_basic_fields_exist_in_dumped_state(dumped_state)

    result = target_db.query("SELECT COUNT(*) AS cnt FROM #{DEFAULT_FULL_TABLE_NAME}")
    count = result.first["cnt"]
    # the default batch size for the data-iterator is 200 rows. We send the
    # term signal as soon as we have copied one batch, but the processing of
    # the signal then races with further iterations.
    # Make sure we shut down in one of the first few iterations - on a fast
    # system, we typically end up with 200, but we may also see one or two
    # more copy iterations complete
    assert_includes [200, 400, 600], count

    result = target_db.query("SELECT MAX(id) AS max_id FROM #{DEFAULT_FULL_TABLE_NAME}")
    last_successful_id = result.first["max_id"]
    assert last_successful_id > 0
    # NOTE: Ideally the resume data in the target database would match the data
    # in the resume state exactly, but there is a race condition between
    # committing the data transaction and updating the resume data.
    # See comments in batch_writer.go on committing the transaction for details.
    assert_operator last_successful_id, :>=, dumped_state["LastSuccessfulPaginationKeys"]["#{DEFAULT_DB}.#{DEFAULT_TABLE}"]["Values"][0]
  end

  def test_interrupt_and_resume_without_last_known_schema_cache
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    # Writes one batch
    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      ghostferry.send_signal("TERM")
    end

    dumped_state = ghostferry.run_expecting_interrupt
    assert_basic_fields_exist_in_dumped_state(dumped_state)
    dumped_state["LastKnownTableSchemaCache"] = nil

    # Resume Ghostferry with dumped state
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    ghostferry.run(dumped_state)

    assert_test_table_is_identical
  end

  def test_interrupt_resume_with_writes_to_source
    # Start a ghostferry run expecting it to be interrupted.
    datawriter = new_source_datawriter
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    start_datawriter_with_ghostferry(datawriter, ghostferry)

    batches_written = 0
    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      batches_written += 1
      if batches_written >= 2
        ghostferry.send_signal("TERM")
      end
    end

    dumped_state = ghostferry.run_expecting_interrupt
    assert_basic_fields_exist_in_dumped_state(dumped_state)

    # Resume Ghostferry with dumped state
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    # The datawriter is still writing to the database since earlier, so we need
    # to stop it during cutover.
    stop_datawriter_during_cutover(datawriter, ghostferry)

    ghostferry.run(dumped_state)

    assert_test_table_is_identical
  end

  def test_interrupt_resume_when_table_has_completed
    # Start a run of Ghostferry expecting to be interrupted
    datawriter = new_source_datawriter
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    start_datawriter_with_ghostferry(datawriter, ghostferry)
    stop_datawriter_during_cutover(datawriter, ghostferry)

    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      ghostferry.send_signal("TERM")
    end

    dumped_state = ghostferry.run_expecting_interrupt
    assert_basic_fields_exist_in_dumped_state(dumped_state)

    # Resume ghostferry from interrupted state
    datawriter = new_source_datawriter
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    start_datawriter_with_ghostferry(datawriter, ghostferry)
    stop_datawriter_during_cutover(datawriter, ghostferry)

    ghostferry.run(dumped_state)

    assert_test_table_is_identical
  end

  def test_interrupt_resume_will_not_emit_binlog_position_for_inline_verifier_if_no_verification_is_used
    datawriter = new_source_datawriter
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    start_datawriter_with_ghostferry(datawriter, ghostferry)

    batches_written = 0
    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      batches_written += 1
      if batches_written >= 2
        ghostferry.term_and_wait_for_exit
      end
    end

    dumped_state = ghostferry.run_expecting_interrupt
    assert_basic_fields_exist_in_dumped_state(dumped_state)
    assert_equal "", dumped_state["LastStoredBinlogPositionForInlineVerifier"]["EventPosition"]["Name"]
    assert_equal 0, dumped_state["LastStoredBinlogPositionForInlineVerifier"]["EventPosition"]["Pos"]
    assert_equal "", dumped_state["LastStoredBinlogPositionForInlineVerifier"]["ResumePosition"]["Name"]
    assert_equal 0, dumped_state["LastStoredBinlogPositionForInlineVerifier"]["ResumePosition"]["Pos"]
  end

  def test_interrupt_resume_inline_verifier_with_datawriter
    datawriter = new_source_datawriter
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })

    start_datawriter_with_ghostferry(datawriter, ghostferry)

    batches_written = 0
    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      batches_written += 1
      if batches_written >= 2
        ghostferry.term_and_wait_for_exit
      end
    end

    dumped_state = ghostferry.run_expecting_interrupt
    assert_basic_fields_exist_in_dumped_state(dumped_state)
    refute_nil dumped_state["BinlogVerifyStore"]
    refute_nil dumped_state["BinlogVerifyStore"]["gftest"]
    refute_nil dumped_state["BinlogVerifyStore"]["gftest"]["test_table_1"]

    # Resume Ghostferry with dumped state
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })

    verification_ran = false
    incorrect_tables = []
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*tables|
      verification_ran = true
      incorrect_tables = tables
    end

    # The datawriter is still writing to the database since earlier, so we need
    # to stop it during cutover.
    stop_datawriter_during_cutover(datawriter, ghostferry)

    ghostferry.run(dumped_state)

    assert verification_ran
    assert_equal 0, incorrect_tables.length
    assert_test_table_is_identical
  end

  def test_interrupt_inline_verifier_will_emit_binlog_verify_store
    result = source_db.query("SELECT MAX(id) FROM #{DEFAULT_FULL_TABLE_NAME}")
    chosen_id = result.first["MAX(id)"] + 1

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })

    i = 0
    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      sleep if i >= 1 # block the DataIterator so it doesn't race with the term_and_wait_for_exit below.
      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data) VALUES (#{chosen_id}, 'data')")
      i += 1
    end

    ghostferry.on_status(Ghostferry::Status::AFTER_BINLOG_APPLY) do
      ghostferry.term_and_wait_for_exit
    end

    dumped_state = ghostferry.run_expecting_interrupt
    assert_basic_fields_exist_in_dumped_state(dumped_state)
    refute_nil dumped_state["LastStoredBinlogPositionForInlineVerifier"].nil?
    refute_nil dumped_state["BinlogVerifyStore"]
    refute_nil dumped_state["BinlogVerifyStore"]["gftest"]
    refute_nil dumped_state["BinlogVerifyStore"]["gftest"]["test_table_1"]

    # FLAKY: AFTER_BINLOG_APPLY is emitted after the BinlogStreamer
    #        finishes processing all of the event listeners. The
    #        term_and_wait_for_exit above blocks the BinlogStreamer from
    #        processing additional rows but it does not block
    #        InlineVerifier.PeriodicallyVerifyBinlogEvents. This means the
    #        binlog event created in the code above may be verified before the
    #        TERM signal is processed. Thus, the state dump may not contain
    #        this row.
    #
    #        Fixing this is somewhat non-trivial and likely require a more
    #        extensive signal emitting system within Ghostferry.
    assert_equal 1, dumped_state["BinlogVerifyStore"]["gftest"]["test_table_1"]["#{chosen_id}"]

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })
    ghostferry.run(dumped_state)
    assert_test_table_is_identical
  end

  def test_interrupt_resume_inline_verifier_will_verify_entries_in_reverify_store
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })

    # This row would have been copied as we terminate ghostferry after 1 batch
    # is copied.
    result = source_db.query("SELECT MIN(id) FROM #{DEFAULT_FULL_TABLE_NAME}")
    chosen_id = result.first["MIN(id)"] + 1

    i = 0
    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      if i == 1
        # We need to delete it the second batch because trying to delete the
        # minimum id row while in the first batch will result in a lock as the
        # DataIterator holds a FOR UPDATE lock for the minimum id row.
        source_db.query("DELETE FROM #{DEFAULT_FULL_TABLE_NAME} WHERE id = #{chosen_id}")
      end
      sleep if i > 1 # block the DataIterator so it doesn't race with the term_and_wait_for_exit below.
      i += 1
    end

    ghostferry.on_status(Ghostferry::Status::AFTER_BINLOG_APPLY) do
      ghostferry.term_and_wait_for_exit
    end

    dumped_state = ghostferry.run_expecting_interrupt
    assert_basic_fields_exist_in_dumped_state(dumped_state)
    refute_nil dumped_state["BinlogVerifyStore"]
    refute_nil dumped_state["BinlogVerifyStore"]["gftest"]
    refute_nil dumped_state["BinlogVerifyStore"]["gftest"]["test_table_1"]

    # FLAKY: See explanation in test_interrupt_inline_verifier_will_emit_binlog_verify_store
    assert_equal 1, dumped_state["BinlogVerifyStore"]["gftest"]["test_table_1"]["#{chosen_id}"]

    # Corrupt the row before resuming
    target_db.query("REPLACE INTO #{DEFAULT_FULL_TABLE_NAME} (id, data) VALUES (#{chosen_id}, 'corrupted')")

    verification_ran = false
    incorrect_tables = nil
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*tables|
      verification_ran = true
      incorrect_tables = tables
    end

    ghostferry.run(dumped_state)
    assert verification_ran
    assert_equal 1, incorrect_tables.length
    assert_equal "gftest.test_table_1", incorrect_tables.first

    error_line = ghostferry.error_lines.last
    assert_equal "cutover verification failed for: gftest.test_table_1 [paginationKeys: #{chosen_id} ] ", error_line["msg"]
  end

  def test_interrupt_resume_inline_verifier_will_verify_additional_rows_changed_on_source_during_interrupt
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })

    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      ghostferry.term_and_wait_for_exit
    end

    dumped_state = ghostferry.run_expecting_interrupt
    assert_basic_fields_exist_in_dumped_state(dumped_state)
    refute_nil dumped_state["BinlogVerifyStore"]

    result = source_db.query("SELECT MIN(id) FROM #{DEFAULT_FULL_TABLE_NAME}")
    chosen_id = result.first["MIN(id)"]

    # This test is intended to observe the actions of a resumed InlineVerifier
    # on a binlog entry that was inserted into the source database while
    # Ghostferry is interrupted. Normally, Ghostferry would correctly apply the
    # binlog entry to the target and the InlineVerifier will verify the row
    # without errors. This means the test will not be able to observe that the
    # InlineVerifier actually tried to verify that row. To observe the action
    # of the InlineVerifier, we artificially corrupt the data on the target,
    # causing the BinlogStreamer to not able to SET data = 'data2' on the
    # target. The InlineVerifier should pick up this case if it is implemented
    # correctly.
    source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = 'data2' WHERE id = #{chosen_id}")
    target_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = 'corrupted' WHERE id = #{chosen_id}")

    verification_ran = false
    incorrect_tables = nil
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*tables|
      verification_ran = true
      incorrect_tables = tables
    end

    ghostferry.run(dumped_state)
    assert verification_ran
    assert_equal 1, incorrect_tables.length
    assert_equal "gftest.test_table_1", incorrect_tables.first

    error_line = ghostferry.error_lines.last
    assert_equal "cutover verification failed for: gftest.test_table_1 [paginationKeys: #{chosen_id} ] ", error_line["msg"]
  end

  def test_interrupt_resume_between_consecutive_rows_events
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })

    # create a series of rows-events that do not have interleaved table-map
    # events. This is the case when multiple rows are affected in a single
    # DML event.
    # Since we are racing between applying rows and sending the shutdown event,
    # we emit a whole bunch of them
    num_batches = 5
    num_preload_batches = 3
    num_values_per_batch = 250
    row_id = 0
    ghostferry.on_status(Ghostferry::Status::BINLOG_STREAMING_STARTED) do
      for _batch_id in 0..num_preload_batches do
        insert_sql = "INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (data) VALUES "
        for value_in_batch in 0..num_values_per_batch do
          row_id += 1
          insert_sql += ", " if value_in_batch > 0
          insert_sql += "('data#{row_id}')"
        end
        source_db.query(insert_sql)
      end
    end

    ghostferry.on_status(Ghostferry::Status::AFTER_BINLOG_APPLY) do
      for _batch_id in num_preload_batches..num_batches do
        insert_sql = "INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (data) VALUES "
        for value_in_batch in 0..num_values_per_batch do
          row_id += 1
          insert_sql += ", " if value_in_batch > 0
          insert_sql += "('data#{row_id}')"
        end
        source_db.query(insert_sql)
      end

      # while we are handling events from the loop above, try to inject a
      # shutdown signal, hoping to interrupt between applying an INSERT
      # and receiving the next table-map event
      if row_id > 3
        ghostferry.term_and_wait_for_exit
      end
    end

    dumped_state = ghostferry.run_expecting_interrupt

    # We can verify if the race occurred (and we successfully worked around it)
    # by looking at the dumped state (the LastWrittenBinlogPosition field should
    # have different EventPosition and ResumePosition values).
    #
    # If this starts to make the test unreliable, we may want to remove this or
    # further tweak the batch values.
    resume_state = dumped_state["LastWrittenBinlogPosition"]

    # XXX: It's just too racy. We keep failing builds on travis-ci
    #refute_equal resume_state["EventPosition"], resume_state["ResumePosition"]

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)
    # if we did not resume at a proper state, this invocation of ghostferry
    # will crash, complaining that a rows-event is referring to an unknown
    # table
    ghostferry.run(dumped_state)

    assert_test_table_is_identical
  end

  def test_interrupt_resume_state_on_db
    state_table_prefix = "#{DEFAULT_STATE_DB}._ghostferry_#{DEFAULT_SERVER_ID}_"

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { resume_state_from_db: DEFAULT_STATE_DB })

    ghostferry.on_status(Ghostferry::Status::BINLOG_STREAMING_STOPPED) do
      ghostferry.term_and_wait_for_exit
    end

    ghostferry.run_expecting_interrupt

    res = target_db.query("SELECT event_filename, event_pos, resume_filename, resume_pos FROM #{state_table_prefix}_last_binlog_writer_state")
    assert_equal 1, res.count
    last_binlog_state_row = nil
    res.each do |row|
      refute_empty row["event_filename"]
      refute_equal row["event_pos"], 0
      refute_empty row["resume_filename"]
      refute_equal row["resume_pos"], 0
      last_binlog_state_row = row
    end

    res = target_db.query("SELECT event_filename, event_pos, resume_filename, resume_pos FROM #{state_table_prefix}_last_inline_verifier_state")
    assert_equal 1, res.count
    res.each do |row|
      refute_empty row["event_filename"]
      refute_equal row["event_pos"], 0
      refute_empty row["resume_filename"]
      refute_equal row["resume_pos"], 0
    end

    res = target_db.query("SELECT table_name, copy_complete FROM #{state_table_prefix}_row_copy_state")
    assert_equal 1, res.count
    res.each do |row|
      assert_equal "#{DEFAULT_DB}.#{DEFAULT_TABLE}", row["table_name"]
      assert_equal 1, row["copy_complete"]
    end

    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (data) VALUES ('value')")

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { resume_state_from_db: DEFAULT_STATE_DB })
    ghostferry.run

    assert_test_table_is_identical

    res = target_db.query("SELECT event_filename, event_pos, resume_filename, resume_pos FROM #{state_table_prefix}_last_binlog_writer_state")
    assert_equal 1, res.count
    res.each do |row|
      assert row['event_filename'] > last_binlog_state_row['event_filename'] || row['event_pos'] > last_binlog_state_row['event_pos']
      assert row['resume_filename'] > last_binlog_state_row['resume_filename'] || row['resume_pos'] > last_binlog_state_row['resume_pos']
    end
  end
end
