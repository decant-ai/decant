use decant_ingest::{
    KAFKA_JSON_INGESTION_REPLAY_JOURNAL_VERSION_V0, KafkaJsonIngestionReplayAppendErrorV0,
    KafkaJsonIngestionReplayConfigErrorV0, KafkaJsonIngestionReplayInputV0,
    KafkaJsonIngestionReplayJournalV0, KafkaJsonIngestionReplayLimitFieldV0,
    KafkaJsonIngestionReplayLimitsV0, MAX_KAFKA_JSON_INGESTION_REPLAY_BYTES_V0,
    MAX_KAFKA_JSON_INGESTION_REPLAY_INPUTS_V0,
    kafka_json_ingestion_replay_input_accounted_bytes_v0,
};
use decant_ir::{KafkaPositionV0, SourceIdV0};
use decant_kafka::{
    KafkaClusterIdV0, KafkaFetchedRecordV0, KafkaRecordTimestampV0, KafkaTopicIdV0,
};

fn input(
    source: &str,
    cluster: &str,
    topic_byte: u8,
    offset: i64,
    value: Option<&[u8]>,
) -> KafkaJsonIngestionReplayInputV0 {
    KafkaJsonIngestionReplayInputV0::new(
        SourceIdV0::new(source).unwrap(),
        KafkaClusterIdV0::new(cluster).unwrap(),
        KafkaTopicIdV0::from_bytes([topic_byte; 16]).unwrap(),
        KafkaFetchedRecordV0::new(
            KafkaPositionV0::new(0, offset).unwrap(),
            Some(b"secret-key"),
            value,
            KafkaRecordTimestampV0::NotAvailable,
            &[],
        )
        .unwrap(),
    )
}

#[test]
fn exact_accounting_and_maxima_are_stable() {
    assert_eq!(KAFKA_JSON_INGESTION_REPLAY_JOURNAL_VERSION_V0, 0);
    assert_eq!(MAX_KAFKA_JSON_INGESTION_REPLAY_INPUTS_V0, 100_001);
    assert_eq!(
        MAX_KAFKA_JSON_INGESTION_REPLAY_BYTES_V0,
        128 * 1_024 * 1_024
    );
    let entry = input("source", "cluster", 1, 0, Some(b"not-json"));
    assert_eq!(
        kafka_json_ingestion_replay_input_accounted_bytes_v0(&entry).unwrap(),
        4 + "source".len() + 4 + "cluster".len() + 16 + entry.record().logical_bytes()
    );
    assert_eq!(
        KafkaJsonIngestionReplayLimitsV0::maximum().input_limit(),
        MAX_KAFKA_JSON_INGESTION_REPLAY_INPUTS_V0
    );
    assert_eq!(
        KafkaJsonIngestionReplayLimitsV0::maximum().byte_limit(),
        MAX_KAFKA_JSON_INGESTION_REPLAY_BYTES_V0
    );
}

#[test]
fn configuration_boundaries_are_exact() {
    assert_eq!(
        KafkaJsonIngestionReplayJournalV0::new(1, KafkaJsonIngestionReplayLimitsV0::maximum())
            .unwrap_err(),
        KafkaJsonIngestionReplayConfigErrorV0::UnsupportedVersion { actual: 1 }
    );
    assert_eq!(
        KafkaJsonIngestionReplayLimitsV0::new(0, 1).unwrap_err(),
        KafkaJsonIngestionReplayConfigErrorV0::ZeroLimit {
            field: KafkaJsonIngestionReplayLimitFieldV0::Inputs,
        }
    );
    assert_eq!(
        KafkaJsonIngestionReplayLimitsV0::new(1, 0).unwrap_err(),
        KafkaJsonIngestionReplayConfigErrorV0::ZeroLimit {
            field: KafkaJsonIngestionReplayLimitFieldV0::Bytes,
        }
    );
    assert!(matches!(
        KafkaJsonIngestionReplayLimitsV0::new(MAX_KAFKA_JSON_INGESTION_REPLAY_INPUTS_V0 + 1, 1),
        Err(KafkaJsonIngestionReplayConfigErrorV0::AboveMaximum {
            field: KafkaJsonIngestionReplayLimitFieldV0::Inputs,
            ..
        })
    ));
    assert!(matches!(
        KafkaJsonIngestionReplayLimitsV0::new(1, MAX_KAFKA_JSON_INGESTION_REPLAY_BYTES_V0 + 1),
        Err(KafkaJsonIngestionReplayConfigErrorV0::AboveMaximum {
            field: KafkaJsonIngestionReplayLimitFieldV0::Bytes,
            ..
        })
    ));
}

#[test]
fn count_refusal_returns_input_and_leaves_journal_exact() {
    let first = input("source", "cluster", 1, 0, Some(b"bad-json"));
    let second = input("source", "cluster", 1, 1, None);
    let input_bytes = kafka_json_ingestion_replay_input_accounted_bytes_v0(&first).unwrap();
    let mut journal = KafkaJsonIngestionReplayJournalV0::new(
        0,
        KafkaJsonIngestionReplayLimitsV0::new(1, input_bytes * 2).unwrap(),
    )
    .unwrap();
    journal.try_push(first.clone()).unwrap();
    let before = journal.clone();
    let (error, returned) = journal.try_push(second.clone()).unwrap_err();
    assert_eq!(
        error,
        KafkaJsonIngestionReplayAppendErrorV0::CapacityExceeded {
            field: KafkaJsonIngestionReplayLimitFieldV0::Inputs,
            maximum: 1,
            attempted: 2,
        }
    );
    assert_eq!(returned, second);
    assert_eq!(journal, before);
}

#[test]
fn byte_refusal_is_independent_atomic_and_exact_at_the_boundary() {
    let first = input("source", "cluster", 1, 0, Some(b"bad-json"));
    let second = input("source", "cluster", 1, 1, Some(b"more-bad-json"));
    let first_bytes = kafka_json_ingestion_replay_input_accounted_bytes_v0(&first).unwrap();
    let second_bytes = kafka_json_ingestion_replay_input_accounted_bytes_v0(&second).unwrap();
    let mut journal = KafkaJsonIngestionReplayJournalV0::new(
        0,
        KafkaJsonIngestionReplayLimitsV0::new(2, first_bytes).unwrap(),
    )
    .unwrap();
    journal.try_push(first).unwrap();
    assert_eq!(journal.usage().bytes(), first_bytes);
    let before = journal.clone();
    let (error, returned) = journal.try_push(second.clone()).unwrap_err();
    assert_eq!(
        error,
        KafkaJsonIngestionReplayAppendErrorV0::CapacityExceeded {
            field: KafkaJsonIngestionReplayLimitFieldV0::Bytes,
            maximum: first_bytes,
            attempted: first_bytes + second_bytes,
        }
    );
    assert_eq!(returned, second);
    assert_eq!(journal, before);
}

#[test]
fn insertion_order_retains_malformed_tombstone_and_mismatched_observations() {
    let malformed = input("wrong-source", "wrong-cluster", 2, 7, Some(b"{bad"));
    let tombstone = input("source", "cluster", 1, 8, None);
    let mut journal =
        KafkaJsonIngestionReplayJournalV0::new(0, KafkaJsonIngestionReplayLimitsV0::maximum())
            .unwrap();
    assert!(journal.is_empty());
    journal.try_push(malformed.clone()).unwrap();
    journal.try_push(tombstone.clone()).unwrap();
    assert_eq!(journal.len(), 2);
    assert_eq!(journal.inputs(), &[malformed, tombstone]);
    assert_eq!(
        journal.inputs()[0].observed_source_id().as_str(),
        "wrong-source"
    );
    assert_eq!(
        journal.inputs()[0].observed_cluster_id().as_str(),
        "wrong-cluster"
    );
    assert_eq!(journal.inputs()[0].observed_topic_id().as_bytes(), &[2; 16]);
    assert_eq!(
        journal.inputs()[0].record().value(),
        Some(b"{bad".as_slice())
    );
    assert!(journal.inputs()[1].record().is_tombstone());
}

#[test]
fn debug_output_is_value_safe_and_repeated_journals_are_equal() {
    let secret = input(
        "secret-source",
        "secret-cluster",
        1,
        0,
        Some(b"secret-value"),
    );
    let mut left =
        KafkaJsonIngestionReplayJournalV0::new(0, KafkaJsonIngestionReplayLimitsV0::maximum())
            .unwrap();
    let mut right = left.clone();
    left.try_push(secret.clone()).unwrap();
    right.try_push(secret.clone()).unwrap();
    assert_eq!(left, right);
    for rendered in [format!("{secret:?}"), format!("{left:?}")] {
        assert!(!rendered.contains("secret-source"));
        assert!(!rendered.contains("secret-cluster"));
        assert!(!rendered.contains("secret-key"));
        assert!(!rendered.contains("secret-value"));
    }
}
