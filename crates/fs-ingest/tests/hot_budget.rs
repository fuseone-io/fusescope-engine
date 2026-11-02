//! A hot scan's answer is charged message by message before it is decoded,
//! so a budget stops it while little of the answer has crossed the wire.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use fs_catalog::TenantId;
use fs_ingest::IngestError;
use fs_ingest::hot::{HotScanRequest, scan_charged, scan_with};
use fs_ingest::record::RowSignal;
use futures_util::{Stream, StreamExt, TryStreamExt};
use tonic::{Request, Response, Status, Streaming};

/// Bytes of one message the fake ingester sends.
const MESSAGE: usize = 256 * 1024;
/// Messages it sends: 64 MiB in all.
const MESSAGES: usize = 256;

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// Answers every scan with `messages`, counting what it hands to the wire.
struct Prebuilt {
    messages: Arc<Vec<FlightData>>,
    sent: Arc<AtomicUsize>,
}

#[tonic::async_trait]
impl FlightService for Prebuilt {
    type HandshakeStream = BoxStream<HandshakeResponse>;
    type ListFlightsStream = BoxStream<FlightInfo>;
    type DoGetStream = BoxStream<FlightData>;
    type DoPutStream = BoxStream<PutResult>;
    type DoActionStream = BoxStream<arrow_flight::Result>;
    type ListActionsStream = BoxStream<ActionType>;
    type DoExchangeStream = BoxStream<FlightData>;

    async fn do_get(&self, _: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        let (messages, sent) = (Arc::clone(&self.messages), Arc::clone(&self.sent));
        let stream = futures_util::stream::iter(0..messages.len()).map(move |i| {
            let message = messages[i].clone();
            sent.fetch_add(
                message.data_body.len() + message.data_header.len(),
                Ordering::SeqCst,
            );
            Ok(message)
        });
        Ok(Response::new(Box::pin(stream)))
    }
    async fn handshake(
        &self,
        _: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("do_get only"))
    }
    async fn list_flights(
        &self,
        _: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("do_get only"))
    }
    async fn get_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("do_get only"))
    }
    async fn poll_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("do_get only"))
    }
    async fn get_schema(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("do_get only"))
    }
    async fn do_put(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_get only"))
    }
    async fn do_exchange(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_get only"))
    }
    async fn do_action(
        &self,
        _: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_get only"))
    }
    async fn list_actions(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("do_get only"))
    }
}

/// The messages of `MESSAGES` batches of one text column of about
/// `MESSAGE` bytes each.
async fn messages() -> Vec<FlightData> {
    let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, false)]));
    let batches = (0..MESSAGES).map(|n| {
        let text = format!("{n:08}").repeat(MESSAGE / 8 / 64);
        let column = StringArray::from_iter_values(std::iter::repeat_n(text.as_str(), 64));
        Ok(RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(column)]).unwrap())
    });
    FlightDataEncoderBuilder::new()
        .build(futures_util::stream::iter(batches.collect::<Vec<_>>()))
        .try_collect()
        .await
        .unwrap()
}

async fn serve(messages: Vec<FlightData>) -> (String, Arc<AtomicUsize>) {
    let sent = Arc::new(AtomicUsize::new(0));
    let service = Prebuilt {
        messages: Arc::new(messages),
        sent: Arc::clone(&sent),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(service).max_encoding_message_size(usize::MAX))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    (address, sent)
}

fn request() -> HotScanRequest {
    HotScanRequest {
        tenant: TenantId::new(),
        ts_min: None,
        ts_max: None,
        services: None,
        columns: None,
        trace_ids: None,
        span_ids: None,
        distinct: None,
        signal: RowSignal::Traces,
        table: None,
    }
}

#[tokio::test]
async fn a_hot_answer_past_its_budget_stops_before_it_is_read() {
    let messages = messages().await;
    let total: usize = messages
        .iter()
        .map(|m| m.data_body.len() + m.data_header.len())
        .sum();
    assert!(total >= MESSAGES * MESSAGE, "{total}");
    let (address, sent) = serve(messages).await;
    let timeout = Duration::from_secs(30);

    let unbounded = scan_with(&address, &request(), timeout, None)
        .await
        .unwrap();
    assert_eq!(
        unbounded.batches.len(),
        MESSAGES,
        "the whole answer, unbudgeted"
    );
    assert!(sent.load(Ordering::SeqCst) >= total);

    sent.store(0, Ordering::SeqCst);
    let budget = 4 * 1024 * 1024;
    let charged = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&charged);
    let charge =
        Arc::new(move |bytes: usize| counted.fetch_add(bytes, Ordering::SeqCst) + bytes <= budget);
    let err = scan_charged(&address, &request(), timeout, None, charge)
        .await
        .unwrap_err();
    assert!(matches!(err, IngestError::HotScanBudget { .. }), "{err}");
    let read = sent.load(Ordering::SeqCst);
    assert!(
        read <= 4 * budget,
        "{read} bytes of {total} crossed before the budget of {budget} stopped the scan"
    );
}

#[tokio::test]
async fn a_decoded_batch_is_charged_before_it_is_kept() {
    let messages = messages().await;
    let wire: std::collections::HashSet<usize> = messages
        .iter()
        .map(|m| m.data_body.len() + m.data_header.len())
        .collect();
    let (address, _) = serve(messages).await;
    let timeout = Duration::from_secs(30);

    // Every message as it arrives is admitted; what it decodes into is not.
    let admit_wire = Arc::new(move |bytes: usize| wire.contains(&bytes));
    let err = scan_charged(&address, &request(), timeout, None, admit_wire)
        .await
        .unwrap_err();
    assert!(matches!(err, IngestError::HotScanBudget { .. }), "{err}");

    let charged = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&charged);
    let all = Arc::new(move |bytes: usize| {
        counted.fetch_add(bytes, Ordering::SeqCst);
        true
    });
    let answer = scan_charged(&address, &request(), timeout, None, all)
        .await
        .unwrap();
    let held: usize = answer.batches.iter().map(fs_ingest::hot::held_bytes).sum();
    assert!(
        charged.load(Ordering::SeqCst) >= held,
        "charged {} for batches holding {held}",
        charged.load(Ordering::SeqCst)
    );
}
