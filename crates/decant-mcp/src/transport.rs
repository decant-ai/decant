use std::{
    error::Error,
    fmt, io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use rmcp::{
    RoleServer,
    model::{ClientJsonRpcMessage, JsonRpcMessage, ServerJsonRpcMessage},
    transport::Transport,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{Mutex, Notify},
};

pub const DECANT_MCP_INPUT_JSON_LINE_MAX_BYTES_V0: usize = 64 * 1024;
pub const DECANT_MCP_OUTPUT_JSON_LINE_MAX_BYTES_V0: usize = 128 * 1024;
pub const DECANT_MCP_MAX_IN_FLIGHT_REQUESTS_V0: usize = 1;
const READ_CHUNK_BYTES: usize = 4 * 1024;

pub struct DecantMcpBoundedTransportV0<R, W> {
    read: R,
    pending: Vec<u8>,
    write: Arc<Mutex<Option<W>>>,
    flow: Arc<RequestFlowV0>,
    notification_delivered: bool,
}

struct RequestFlowV0 {
    in_flight: AtomicBool,
    completed: Notify,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecantMcpBoundedTransportErrorV0 {
    Serialize,
    OutputTooLarge,
    Closed,
    Write,
}

impl fmt::Display for DecantMcpBoundedTransportErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for DecantMcpBoundedTransportErrorV0 {}

impl<R, W> DecantMcpBoundedTransportV0<R, W>
where
    R: AsyncRead + Send + Unpin,
    W: AsyncWrite + Send + Unpin + 'static,
{
    pub fn new(read: R, write: W) -> Self {
        Self {
            read,
            pending: Vec::with_capacity(READ_CHUNK_BYTES),
            write: Arc::new(Mutex::new(Some(write))),
            flow: Arc::new(RequestFlowV0 {
                in_flight: AtomicBool::new(false),
                completed: Notify::new(),
            }),
            notification_delivered: false,
        }
    }

    async fn receive_line(&mut self) -> Option<Vec<u8>> {
        loop {
            if let Some(newline) = self.pending.iter().position(|byte| *byte == b'\n') {
                let remainder = self.pending.split_off(newline + 1);
                let mut line = std::mem::replace(&mut self.pending, remainder);
                line.truncate(newline);
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                if line.is_empty() {
                    continue;
                }
                return Some(line);
            }
            if self.pending.len() > DECANT_MCP_INPUT_JSON_LINE_MAX_BYTES_V0 {
                return None;
            }
            let remaining = DECANT_MCP_INPUT_JSON_LINE_MAX_BYTES_V0
                .saturating_add(1)
                .saturating_sub(self.pending.len());
            let mut chunk = [0u8; READ_CHUNK_BYTES];
            let read_limit = remaining.min(chunk.len());
            match self.read.read(&mut chunk[..read_limit]).await {
                Ok(0) | Err(_) => return None,
                Ok(read) => self.pending.extend_from_slice(&chunk[..read]),
            }
        }
    }
}

impl<R, W> Transport<RoleServer> for DecantMcpBoundedTransportV0<R, W>
where
    R: AsyncRead + Send + Unpin,
    W: AsyncWrite + Send + Unpin + 'static,
{
    type Error = DecantMcpBoundedTransportErrorV0;

    fn send(
        &mut self,
        item: ServerJsonRpcMessage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        let write = Arc::clone(&self.write);
        let flow = Arc::clone(&self.flow);
        let completes_request = matches!(
            &item,
            JsonRpcMessage::Response(_) | JsonRpcMessage::Error(_)
        );
        async move {
            let result = async {
                let mut bytes = serde_json::to_vec(&item)
                    .map_err(|_| DecantMcpBoundedTransportErrorV0::Serialize)?;
                if bytes.len() > DECANT_MCP_OUTPUT_JSON_LINE_MAX_BYTES_V0 {
                    return Err(DecantMcpBoundedTransportErrorV0::OutputTooLarge);
                }
                bytes.push(b'\n');
                let mut guard = write.lock().await;
                let writer = guard
                    .as_mut()
                    .ok_or(DecantMcpBoundedTransportErrorV0::Closed)?;
                writer
                    .write_all(&bytes)
                    .await
                    .map_err(|_| DecantMcpBoundedTransportErrorV0::Write)?;
                writer
                    .flush()
                    .await
                    .map_err(|_| DecantMcpBoundedTransportErrorV0::Write)
            }
            .await;
            if completes_request {
                flow.in_flight.store(false, Ordering::Release);
                flow.completed.notify_waiters();
            }
            result
        }
    }

    async fn receive(&mut self) -> Option<ClientJsonRpcMessage> {
        loop {
            wait_for_request_slot(&self.flow).await;
            let line = self.receive_line().await?;
            match serde_json::from_slice(&line) {
                Ok(message @ JsonRpcMessage::Request(_)) => {
                    self.flow.in_flight.store(true, Ordering::Release);
                    return Some(message);
                }
                Ok(message @ JsonRpcMessage::Notification(_)) if !self.notification_delivered => {
                    self.notification_delivered = true;
                    return Some(message);
                }
                Ok(
                    JsonRpcMessage::Notification(_)
                    | JsonRpcMessage::Response(_)
                    | JsonRpcMessage::Error(_),
                ) => {}
                Err(error)
                    if matches!(
                        error.classify(),
                        serde_json::error::Category::Syntax | serde_json::error::Category::Eof
                    ) => {}
                Err(_) => return None,
            }
        }
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        let mut guard = self.write.lock().await;
        if let Some(mut writer) = guard.take() {
            writer
                .shutdown()
                .await
                .map_err(|_| DecantMcpBoundedTransportErrorV0::Write)?;
        }
        Ok(())
    }
}

async fn wait_for_request_slot(flow: &RequestFlowV0) {
    loop {
        let completed = flow.completed.notified();
        if !flow.in_flight.load(Ordering::Acquire) {
            return;
        }
        completed.await;
    }
}

impl From<DecantMcpBoundedTransportErrorV0> for io::Error {
    fn from(error: DecantMcpBoundedTransportErrorV0) -> Self {
        io::Error::other(error)
    }
}

#[cfg(test)]
mod tests {
    use rmcp::{
        model::{CallToolResult, ContentBlock, RequestId, ServerResult},
        transport::Transport,
    };
    use tokio::io::{AsyncWriteExt, duplex, split};

    use super::*;

    #[tokio::test]
    async fn exact_line_bound_is_accepted_and_one_past_closes() {
        let valid_prefix = b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"";
        let padding =
            DECANT_MCP_INPUT_JSON_LINE_MAX_BYTES_V0 - valid_prefix.len() - b",\"x\":\"\"}".len();
        let mut line = Vec::from(valid_prefix.as_slice());
        line.extend_from_slice(b",\"x\":\"");
        line.extend(std::iter::repeat_n(b'a', padding));
        line.extend_from_slice(b"\"}\n");
        assert_eq!(line.len(), DECANT_MCP_INPUT_JSON_LINE_MAX_BYTES_V0 + 1);

        let (client, server) = duplex(DECANT_MCP_INPUT_JSON_LINE_MAX_BYTES_V0 + 2);
        let (server_read, server_write) = split(server);
        let mut transport = DecantMcpBoundedTransportV0::new(server_read, server_write);
        let (mut client_read, mut client_write) = split(client);
        client_write.write_all(&line).await.unwrap();
        assert!(
            Transport::<RoleServer>::receive(&mut transport)
                .await
                .is_some()
        );
        let _ = &mut client_read;

        let (client, server) = duplex(DECANT_MCP_INPUT_JSON_LINE_MAX_BYTES_V0 + 3);
        let (server_read, server_write) = split(server);
        let mut transport = DecantMcpBoundedTransportV0::new(server_read, server_write);
        let (_client_read, mut client_write) = split(client);
        let oversized = vec![b'a'; DECANT_MCP_INPUT_JSON_LINE_MAX_BYTES_V0 + 1];
        client_write.write_all(&oversized).await.unwrap();
        client_write.write_all(b"\n").await.unwrap();
        assert!(
            Transport::<RoleServer>::receive(&mut transport)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn exact_output_bound_is_accepted_and_one_past_is_refused() {
        let (client, server) = duplex(DECANT_MCP_OUTPUT_JSON_LINE_MAX_BYTES_V0 + 2);
        let (server_read, server_write) = split(server);
        let mut transport = DecantMcpBoundedTransportV0::new(server_read, server_write);
        let (_client_read, _client_write) = split(client);
        let message = |length| {
            ServerJsonRpcMessage::response(
                ServerResult::CallToolResult(CallToolResult::success(vec![ContentBlock::text(
                    "x".repeat(length),
                )])),
                RequestId::Number(1),
            )
        };
        let overhead = serde_json::to_vec(&message(0)).unwrap().len();
        let exact = message(DECANT_MCP_OUTPUT_JSON_LINE_MAX_BYTES_V0 - overhead);
        assert_eq!(
            serde_json::to_vec(&exact).unwrap().len(),
            DECANT_MCP_OUTPUT_JSON_LINE_MAX_BYTES_V0
        );
        assert_eq!(
            Transport::<RoleServer>::send(&mut transport, exact).await,
            Ok(())
        );
        let oversized = message(DECANT_MCP_OUTPUT_JSON_LINE_MAX_BYTES_V0 - overhead + 1);
        assert_eq!(
            Transport::<RoleServer>::send(&mut transport, oversized).await,
            Err(DecantMcpBoundedTransportErrorV0::OutputTooLarge)
        );
    }

    #[tokio::test]
    async fn second_request_waits_until_first_response_send_completes() {
        let (client, server) = duplex(8 * 1024);
        let (server_read, server_write) = split(server);
        let mut transport = DecantMcpBoundedTransportV0::new(server_read, server_write);
        let (_client_read, mut client_write) = split(client);
        client_write
            .write_all(
                b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"ping\"}\n",
            )
            .await
            .unwrap();
        assert!(matches!(
            Transport::<RoleServer>::receive(&mut transport).await,
            Some(JsonRpcMessage::Request(_))
        ));

        let response = Transport::<RoleServer>::send(
            &mut transport,
            ServerJsonRpcMessage::response(
                ServerResult::CallToolResult(CallToolResult::success(Vec::new())),
                RequestId::Number(1),
            ),
        );
        let second =
            tokio::spawn(async move { Transport::<RoleServer>::receive(&mut transport).await });
        tokio::task::yield_now().await;
        assert!(!second.is_finished());
        response.await.unwrap();
        assert!(matches!(
            second.await.unwrap(),
            Some(JsonRpcMessage::Request(_))
        ));
    }

    #[test]
    fn transport_errors_are_content_free() {
        assert_eq!(DECANT_MCP_MAX_IN_FLIGHT_REQUESTS_V0, 1);
        for error in [
            DecantMcpBoundedTransportErrorV0::Serialize,
            DecantMcpBoundedTransportErrorV0::OutputTooLarge,
            DecantMcpBoundedTransportErrorV0::Closed,
            DecantMcpBoundedTransportErrorV0::Write,
        ] {
            assert!(!format!("{error:?} {error}").contains("private"));
        }
    }
}
