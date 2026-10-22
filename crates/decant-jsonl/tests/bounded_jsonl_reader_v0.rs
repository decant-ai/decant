use std::io::{self, BufRead, BufReader, Cursor, Read};

use decant_jsonl::{
    BoundedJsonlReaderV0, JSONL_READER_VERSION_V0, JsonlConfigErrorV0, JsonlLimitFieldV0,
    JsonlLimitsV0, JsonlReadErrorV0,
};

fn limits(record_bytes: usize, records: usize, input_bytes: usize) -> JsonlLimitsV0 {
    JsonlLimitsV0::new(JSONL_READER_VERSION_V0, record_bytes, records, input_bytes).unwrap()
}

#[test]
fn split_buffers_blank_records_and_eof_are_exact() {
    let input = b"one\n\ntwo\n";
    let inner = BufReader::with_capacity(2, Cursor::new(input));
    let mut reader = BoundedJsonlReaderV0::new(inner, limits(8, 4, input.len()));

    let first = reader.next_record().unwrap().unwrap();
    let second = reader.next_record().unwrap().unwrap();
    let third = reader.next_record().unwrap().unwrap();
    assert_eq!(first.line_number(), 1);
    assert_eq!(first.as_bytes(), b"one\n");
    assert_eq!(second.line_number(), 2);
    assert_eq!(second.as_bytes(), b"\n");
    assert_eq!(third.line_number(), 3);
    assert_eq!(third.as_bytes(), b"two\n");
    assert!(reader.next_record().unwrap().is_none());
    assert!(reader.next_record().unwrap().is_none());
    assert_eq!(reader.usage().completed_records(), 3);
    assert_eq!(reader.usage().consumed_bytes(), input.len() as u64);
}

#[test]
fn exact_and_one_past_record_limits_are_atomic() {
    let mut exact = BoundedJsonlReaderV0::new(Cursor::new(b"ab\n"), limits(3, 1, 3));
    assert_eq!(exact.next_record().unwrap().unwrap().as_bytes(), b"ab\n");
    assert!(exact.next_record().unwrap().is_none());

    let mut over = BoundedJsonlReaderV0::new(Cursor::new(b"abc\n"), limits(3, 2, 8));
    assert_eq!(
        over.next_record().unwrap_err(),
        JsonlReadErrorV0::RecordBytesExceeded {
            line_number: 1,
            maximum: 3,
            attempted: 4,
        }
    );
    assert_eq!(over.usage().completed_records(), 0);
    assert_eq!(over.usage().consumed_bytes(), 0);
    assert_eq!(over.next_record(), Err(JsonlReadErrorV0::ReaderFailed));
}

#[test]
fn count_and_total_byte_limits_distinguish_exact_eof_from_more_input() {
    let mut exact_count = BoundedJsonlReaderV0::new(Cursor::new(b"a\n"), limits(2, 1, 4));
    exact_count.next_record().unwrap().unwrap();
    assert!(exact_count.next_record().unwrap().is_none());

    let mut over_count = BoundedJsonlReaderV0::new(Cursor::new(b"a\nb\n"), limits(2, 1, 4));
    over_count.next_record().unwrap().unwrap();
    assert_eq!(
        over_count.next_record().unwrap_err(),
        JsonlReadErrorV0::RecordCountExceeded { maximum: 1 }
    );

    let mut exact_bytes = BoundedJsonlReaderV0::new(Cursor::new(b"a\nb\n"), limits(2, 2, 4));
    exact_bytes.next_record().unwrap().unwrap();
    exact_bytes.next_record().unwrap().unwrap();
    assert!(exact_bytes.next_record().unwrap().is_none());

    let mut over_bytes = BoundedJsonlReaderV0::new(Cursor::new(b"a\nbb\n"), limits(3, 3, 4));
    over_bytes.next_record().unwrap().unwrap();
    assert_eq!(
        over_bytes.next_record().unwrap_err(),
        JsonlReadErrorV0::InputBytesExceeded {
            maximum: 4,
            attempted: 5,
        }
    );
    assert_eq!(over_bytes.usage().consumed_bytes(), 2);
}

#[test]
fn final_fragment_is_never_returned_as_a_record() {
    let mut reader = BoundedJsonlReaderV0::new(
        BufReader::with_capacity(1, Cursor::new(b"secret")),
        limits(8, 2, 8),
    );
    assert_eq!(
        reader.next_record().unwrap_err(),
        JsonlReadErrorV0::MissingTerminalLineFeed {
            line_number: 1,
            fragment_bytes: 6,
        }
    );
    assert_eq!(reader.usage().completed_records(), 0);
    assert_eq!(reader.usage().consumed_bytes(), 6);
}

#[test]
fn configuration_boundaries_are_exact() {
    assert!(JsonlLimitsV0::new(JSONL_READER_VERSION_V0, 1, 1, 1).is_ok());
    assert_eq!(
        JsonlLimitsV0::new(JSONL_READER_VERSION_V0 + 1, 1, 1, 1).unwrap_err(),
        JsonlConfigErrorV0::UnsupportedVersion { actual: 1 }
    );
    assert_eq!(
        JsonlLimitsV0::new(JSONL_READER_VERSION_V0, 0, 1, 1).unwrap_err(),
        JsonlConfigErrorV0::Zero {
            field: JsonlLimitFieldV0::RecordBytes
        }
    );
    assert!(JsonlLimitsV0::new(JSONL_READER_VERSION_V0, 2, 1, 1).is_err());
}

struct FailingBufRead;

impl Read for FailingBufRead {
    fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::other("distinctive-secret"))
    }
}

impl BufRead for FailingBufRead {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        Err(io::Error::other("distinctive-secret"))
    }

    fn consume(&mut self, _amount: usize) {}
}

#[test]
fn io_and_debug_diagnostics_are_value_safe_and_repeatable() {
    let mut reader = BoundedJsonlReaderV0::new(FailingBufRead, limits(8, 2, 8));
    let error = reader.next_record().unwrap_err();
    assert_eq!(error, JsonlReadErrorV0::Io);
    let diagnostics = format!("{error:?} {error} {reader:?}");
    assert!(!diagnostics.contains("distinctive-secret"));

    for _ in 0..2 {
        let mut repeated = BoundedJsonlReaderV0::new(Cursor::new(b"x\ny\n"), limits(2, 2, 4));
        let records = [
            repeated.next_record().unwrap().unwrap().into_bytes(),
            repeated.next_record().unwrap().unwrap().into_bytes(),
        ];
        assert_eq!(records, [b"x\n".to_vec(), b"y\n".to_vec()]);
    }
}
