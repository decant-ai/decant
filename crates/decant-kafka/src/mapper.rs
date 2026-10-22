//! Validated mapping from one bounded fetched record to one canonical event.

use std::error::Error;
use std::fmt;

use decant_ir::{EventEnvelopeV0, SourceIdV0};

use crate::{
    KafkaCanonicalEventAssemblyErrorV0, KafkaClusterIdV0, KafkaEventValueDecodeErrorV0,
    KafkaFetchedRecordV0, KafkaRecordTimestampKindV0, KafkaSourceContractV0,
    KafkaSourceObservationErrorV0, KafkaTopicIdV0, assemble_validated_kafka_canonical_event_v0,
    decode_decant_kafka_event_value_v0,
};

pub const KAFKA_DECANT_JSON_RECORD_MAPPER_VERSION_V0: u16 = 0;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KafkaTransportMetadataOmissionV0 {
    header_count: usize,
    header_bytes: usize,
    timestamp_kind: KafkaRecordTimestampKindV0,
}

impl KafkaTransportMetadataOmissionV0 {
    pub(crate) fn from_record(record: &KafkaFetchedRecordV0) -> Self {
        Self {
            header_count: record.headers().len(),
            header_bytes: record.header_bytes(),
            timestamp_kind: record.timestamp().kind(),
        }
    }

    pub const fn header_count(self) -> usize {
        self.header_count
    }

    pub const fn header_bytes(self) -> usize {
        self.header_bytes
    }

    pub const fn timestamp_kind(self) -> KafkaRecordTimestampKindV0 {
        self.timestamp_kind
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct KafkaMappedEventV0 {
    event: EventEnvelopeV0,
    omitted_transport_metadata: KafkaTransportMetadataOmissionV0,
}

impl KafkaMappedEventV0 {
    pub(crate) const fn from_parts(
        event: EventEnvelopeV0,
        omitted_transport_metadata: KafkaTransportMetadataOmissionV0,
    ) -> Self {
        Self {
            event,
            omitted_transport_metadata,
        }
    }

    pub fn event(&self) -> &EventEnvelopeV0 {
        &self.event
    }

    pub const fn omitted_transport_metadata(&self) -> KafkaTransportMetadataOmissionV0 {
        self.omitted_transport_metadata
    }

    pub fn into_parts(self) -> (EventEnvelopeV0, KafkaTransportMetadataOmissionV0) {
        (self.event, self.omitted_transport_metadata)
    }
}

impl fmt::Debug for KafkaMappedEventV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaMappedEventV0")
            .field("event", &self.event)
            .field(
                "omitted_transport_metadata",
                &self.omitted_transport_metadata,
            )
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaDecantJsonRecordMapErrorV0 {
    SourceObservation {
        category: KafkaSourceObservationErrorV0,
    },
    ValueDecode {
        category: KafkaEventValueDecodeErrorV0,
    },
    CanonicalAssembly {
        category: KafkaCanonicalEventAssemblyErrorV0,
    },
}

impl fmt::Debug for KafkaDecantJsonRecordMapErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceObservation { category } => formatter
                .debug_struct("SourceObservation")
                .field("category", category)
                .finish(),
            Self::ValueDecode { category } => formatter
                .debug_struct("ValueDecode")
                .field("category", category)
                .finish(),
            Self::CanonicalAssembly { category } => formatter
                .debug_struct("CanonicalAssembly")
                .field("category", category)
                .finish(),
        }
    }
}

impl fmt::Display for KafkaDecantJsonRecordMapErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaDecantJsonRecordMapErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SourceObservation { category } => Some(category),
            Self::ValueDecode { category } => Some(category),
            Self::CanonicalAssembly { category } => Some(category),
        }
    }
}

pub fn map_decant_json_kafka_record_v0(
    source_contract: &KafkaSourceContractV0,
    observed_source_id: &SourceIdV0,
    observed_cluster_id: &KafkaClusterIdV0,
    observed_topic_id: KafkaTopicIdV0,
    record: &KafkaFetchedRecordV0,
) -> Result<KafkaMappedEventV0, KafkaDecantJsonRecordMapErrorV0> {
    source_contract
        .validate_observed(observed_source_id, observed_cluster_id, observed_topic_id)
        .map_err(|category| KafkaDecantJsonRecordMapErrorV0::SourceObservation { category })?;

    let decoded = decode_decant_kafka_event_value_v0(record.value())
        .map_err(|category| KafkaDecantJsonRecordMapErrorV0::ValueDecode { category })?;
    let (schema_id, event_time_ns, payload) = decoded.into_parts();
    let event = assemble_validated_kafka_canonical_event_v0(
        source_contract,
        observed_source_id,
        observed_cluster_id,
        observed_topic_id,
        record.position(),
        schema_id,
        record.key(),
        event_time_ns,
        payload,
    )
    .map_err(|category| KafkaDecantJsonRecordMapErrorV0::CanonicalAssembly { category })?;

    Ok(KafkaMappedEventV0::from_parts(
        event,
        KafkaTransportMetadataOmissionV0::from_record(record),
    ))
}
