mod support;
use decant_backend::transport::{BackendErrorV1 as E, HttpSessionV1, LimitsV1};
use std::{error::Error, time::Duration};
use support::{Reply, Server};

fn session(server: &Server, limits: LimitsV1) -> HttpSessionV1 {
    HttpSessionV1::new(
        &format!("{}/grafana", server.url),
        "SYNTHETIC_SECRET",
        Some(&server.ca),
        limits,
    )
    .unwrap()
}

#[tokio::test]
async fn verified_tls_and_scoped_encoded_read() {
    let server = Server::new(vec![Reply::json(200, "{}")]);
    let mut client = session(&server, LimitsV1::default());
    let response = client
        .get(&["api", "datasources"], &[("query", "{service=\"a\"}")])
        .await
        .unwrap();
    assert_eq!(response.body(), b"{}");
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0]
            .starts_with("GET /grafana/api/datasources?query=%7Bservice%3D%22a%22%7D HTTP/1.1")
    );
    assert!(!format!("{client:?} {response:?}").contains("SYNTHETIC_SECRET"));
}

#[tokio::test]
async fn untrusted_and_wrong_host_are_rejected() {
    let server = Server::new(vec![]);
    let mut client =
        HttpSessionV1::new(&server.url, "synthetic", None, LimitsV1::default()).unwrap();
    assert_eq!(client.get(&["api"], &[]).await.unwrap_err(), E::Unavailable);
    assert!(server.requests.lock().unwrap().is_empty());
    let wrong = Server::with_identity(vec![], true);
    let mut client = session(&wrong, LimitsV1::default());
    assert_eq!(client.get(&["api"], &[]).await.unwrap_err(), E::Unavailable);
    assert!(wrong.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn redirects_never_forward_credentials() {
    let destination = Server::new(vec![]);
    let source = Server::new(vec![Reply::raw(
        format!(
            "HTTP/1.1 302 Found\r\nLocation: {}/private\r\nContent-Length: 0\r\n\r\n",
            destination.url
        )
        .into_bytes(),
    )]);
    let mut client = session(&source, LimitsV1::default());
    assert_eq!(
        client.get(&["api"], &[]).await.unwrap_err(),
        E::RedirectRefused
    );
    assert!(destination.requests.lock().unwrap().is_empty());
    assert_eq!(source.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn failures_are_distinct_and_not_retried() {
    let server = Server::new(vec![
        Reply::json(403, "SYNTHETIC_BODY"),
        Reply::json(429, "SYNTHETIC_BODY"),
        Reply::json(503, "SYNTHETIC_BODY"),
        Reply::raw(Vec::new()),
        Reply::json(200, ""),
    ]);
    let mut client = session(&server, LimitsV1::default());
    for expected in [
        E::PermissionDenied,
        E::RateLimited,
        E::Unavailable,
        E::InvalidResponse,
    ] {
        let error = client.get(&["api"], &[]).await.unwrap_err();
        assert_eq!(error, expected);
        assert!(error.source().is_none());
        assert!(!format!("{error} {error:?}").contains("SYNTHETIC"));
    }
    assert!(client.get(&["api"], &[]).await.unwrap().body().is_empty());
    assert_eq!(client.usage().attempts, 5);
    assert_eq!(server.requests.lock().unwrap().len(), 5);
}

#[tokio::test]
async fn attempt_ceiling_and_validation_before_io() {
    let server = Server::new(vec![Reply::json(200, ""); 32]);
    let mut client = session(&server, LimitsV1::default());
    for invalid in ["..", ".", "/api", "%2e", "https://bad", "a\\b", ""] {
        assert_eq!(
            client.get(&[invalid], &[]).await.unwrap_err(),
            E::InvalidRequest
        );
    }
    assert_eq!(client.usage().attempts, 0);
    for _ in 0..32 {
        client.get(&["api"], &[]).await.unwrap();
    }
    assert_eq!(
        client.get(&["api"], &[]).await.unwrap_err(),
        E::BudgetExhausted
    );
    assert_eq!(server.requests.lock().unwrap().len(), 32);
    assert!(
        HttpSessionV1::new(
            &server.url,
            "synthetic",
            Some(&server.ca),
            LimitsV1 {
                attempts: 33,
                ..Default::default()
            }
        )
        .is_err()
    );
}

#[tokio::test]
async fn fixed_and_chunked_body_limits_and_accounting() {
    let server = Server::new(vec![
        Reply::json(200, "1234"),
        Reply::json(200, "12345"),
        Reply::raw(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n4\r\n1234\r\n0\r\n\r\n".to_vec(),
        ),
        Reply::raw(b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n12345".to_vec()),
    ]);
    let mut client = session(
        &server,
        LimitsV1 {
            response_bytes: 4,
            ..Default::default()
        },
    );
    assert_eq!(client.get(&["api"], &[]).await.unwrap().body(), b"1234");
    assert_eq!(
        client.get(&["api"], &[]).await.unwrap_err(),
        E::ResponseTooLarge
    );
    assert_eq!(client.get(&["api"], &[]).await.unwrap().body(), b"1234");
    assert_eq!(
        client.get(&["api"], &[]).await.unwrap_err(),
        E::ResponseTooLarge
    );
    assert_eq!(client.usage().received_bytes, 18);
}

#[tokio::test]
async fn aggregate_limit_and_cancelled_attempts_are_not_refunded() {
    let server = Server::new(vec![Reply::json(200, "1234"); 3]);
    let mut client = session(
        &server,
        LimitsV1 {
            total_bytes: 7,
            ..Default::default()
        },
    );
    client.get(&["api"], &[]).await.unwrap();
    assert_eq!(
        client.get(&["api"], &[]).await.unwrap_err(),
        E::BudgetExhausted
    );
    assert_eq!(
        client.get(&["api"], &[]).await.unwrap_err(),
        E::BudgetExhausted
    );
    assert_eq!(client.usage().received_bytes, 8);
    assert_eq!(server.requests.lock().unwrap().len(), 2);
    let mut slow = Reply::json(200, "123");
    slow.delay_body = Duration::from_millis(120);
    let server = Server::new(vec![slow]);
    let mut client = session(
        &server,
        LimitsV1 {
            attempts: 1,
            ..Default::default()
        },
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(60), client.get(&["api"], &[]))
            .await
            .is_err()
    );
    assert_eq!(client.usage().attempts, 1);
    assert_eq!(
        client.get(&["api"], &[]).await.unwrap_err(),
        E::BudgetExhausted
    );
}

#[tokio::test]
async fn deadlines_cover_headers_and_body_and_session() {
    for body_delay in [false, true] {
        let mut reply = Reply::json(200, "1234");
        if body_delay {
            reply.delay_body = Duration::from_millis(160);
        } else {
            reply.delay_headers = Duration::from_millis(160);
        }
        let server = Server::new(vec![reply]);
        let mut client = session(
            &server,
            LimitsV1 {
                elapsed: Duration::from_millis(100),
                request_timeout: Duration::from_millis(60),
                ..Default::default()
            },
        );
        let start = std::time::Instant::now();
        assert_eq!(client.get(&["api"], &[]).await.unwrap_err(), E::Timeout);
        assert!(start.elapsed() < Duration::from_secs(1));
        tokio::time::sleep(Duration::from_millis(110)).await;
        assert_eq!(
            client.get(&["api"], &[]).await.unwrap_err(),
            E::BudgetExhausted
        );
    }
}

#[tokio::test]
async fn malformed_headers_encoding_and_truncated_body_fail_safely() {
    let too_many = format!(
        "HTTP/1.1 200 OK\r\n{}\r\n",
        "x-test: synthetic\r\n".repeat(65)
    );
    let huge_header = format!(
        "HTTP/1.1 200 OK\r\nx-test: {}\r\nContent-Length: 0\r\n\r\n",
        "x".repeat(17 * 1024)
    );
    let server = Server::new(vec![
        Reply::raw(too_many.into_bytes()),
        Reply::raw(huge_header.into_bytes()),
        Reply::raw(
            b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: 0\r\n\r\n".to_vec(),
        ),
        Reply::raw(b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n1234".to_vec()),
    ]);
    let mut client = session(&server, LimitsV1::default());
    for _ in 0..4 {
        assert_eq!(
            client.get(&["api"], &[]).await.unwrap_err(),
            E::InvalidResponse
        );
    }
    assert_eq!(client.usage().received_bytes, 4);
}

#[test]
fn invalid_configuration_has_no_value_in_diagnostics() {
    for url in [
        "http://synthetic.invalid",
        "https://user:SYNTHETIC@host",
        "https://host/a/../b",
        "https://host/%2e",
        "https://host/?SYNTHETIC",
        "https://host/#SYNTHETIC",
    ] {
        let e = HttpSessionV1::new(url, "synthetic", None, LimitsV1::default()).unwrap_err();
        assert_eq!(e, E::InvalidConfiguration);
        assert!(!format!("{e} {e:?}").contains("SYNTHETIC"));
    }
}

#[tokio::test]
async fn full_response_ceiling_and_request_metadata_boundaries() {
    use decant_backend::transport::MAX_RESPONSE_BYTES;
    let server = Server::new(vec![
        Reply::json(200, &"x".repeat(MAX_RESPONSE_BYTES)),
        Reply::json(200, &"x".repeat(MAX_RESPONSE_BYTES + 1)),
    ]);
    let mut client = session(&server, LimitsV1::default());
    assert_eq!(
        client
            .get(&["api"], &[("query", &"x".repeat(4097))])
            .await
            .unwrap_err(),
        E::InvalidRequest
    );
    assert_eq!(
        client.get(&[&"x".repeat(129)], &[]).await.unwrap_err(),
        E::InvalidRequest
    );
    assert_eq!(
        client
            .get(&["api"], &[("query", "bad\nvalue")])
            .await
            .unwrap_err(),
        E::InvalidRequest
    );
    assert_eq!(client.usage().attempts, 0);
    assert_eq!(
        client.get(&["api"], &[]).await.unwrap().body().len(),
        MAX_RESPONSE_BYTES
    );
    assert_eq!(
        client.get(&["api"], &[]).await.unwrap_err(),
        E::ResponseTooLarge
    );
    assert_eq!(client.usage().received_bytes, 2 * MAX_RESPONSE_BYTES + 1);
}
