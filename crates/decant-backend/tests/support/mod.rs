#![allow(dead_code)]
use std::{
    fs,
    io::{Read, Write},
    net::TcpListener,
    path::PathBuf,
    process::{Command, Stdio},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

static NEXT: AtomicUsize = AtomicUsize::new(0);
pub struct TempDir(pub PathBuf);
impl TempDir {
    pub fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "decant-https-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn material(wrong_host: bool) -> (Vec<u8>, Vec<u8>) {
    static GOOD: OnceLock<(Vec<u8>, Vec<u8>)> = OnceLock::new();
    static WRONG: OnceLock<(Vec<u8>, Vec<u8>)> = OnceLock::new();
    (if wrong_host { &WRONG } else { &GOOD })
        .get_or_init(|| {
            let dir = TempDir::new();
            let san = if wrong_host {
                "subjectAltName=DNS:wrong.invalid"
            } else {
                "subjectAltName=DNS:localhost,IP:127.0.0.1"
            };
            for args in [
                vec![
                    "req",
                    "-x509",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-keyout",
                    "key.pem",
                    "-out",
                    "cert.pem",
                    "-days",
                    "1",
                    "-subj",
                    "/CN=synthetic-local-test",
                    "-addext",
                    san,
                    "-addext",
                    "extendedKeyUsage=serverAuth",
                    "-addext",
                    "keyUsage=digitalSignature,keyEncipherment,keyCertSign",
                ],
                vec![
                    "pkcs12",
                    "-export",
                    "-inkey",
                    "key.pem",
                    "-in",
                    "cert.pem",
                    "-out",
                    "identity.p12",
                    "-passout",
                    "pass:synthetic",
                    "-keypbe",
                    "PBE-SHA1-3DES",
                    "-certpbe",
                    "PBE-SHA1-3DES",
                    "-macalg",
                    "sha1",
                ],
            ] {
                assert!(
                    Command::new("openssl")
                        .args(args)
                        .current_dir(&dir.0)
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status()
                        .expect("OpenSSL test prerequisite")
                        .success()
                );
            }
            (
                fs::read(dir.0.join("cert.pem")).unwrap(),
                fs::read(dir.0.join("identity.p12")).unwrap(),
            )
        })
        .clone()
}

#[derive(Clone)]
pub struct Reply {
    pub bytes: Vec<u8>,
    pub query_time: bool,
    pub delay_headers: Duration,
    pub delay_body: Duration,
}
impl Reply {
    pub fn query_json(body: &str) -> Self {
        let mut reply = Self::json(200, body);
        reply.query_time = true;
        reply
    }
    pub fn json(status: u16, body: &str) -> Self {
        Self::raw(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{body}", body.len()).into_bytes())
    }
    pub fn raw(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            query_time: false,
            delay_headers: Duration::ZERO,
            delay_body: Duration::ZERO,
        }
    }
}
pub struct Server {
    pub url: String,
    pub ca: Vec<u8>,
    pub requests: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}
impl Server {
    pub fn new(replies: Vec<Reply>) -> Self {
        Self::with_identity(replies, false)
    }
    pub fn with_identity(replies: Vec<Reply>, wrong_host: bool) -> Self {
        let (ca, pkcs12) = material(wrong_host);
        let identity = native_tls::Identity::from_pkcs12(&pkcs12, "synthetic").unwrap();
        let acceptor = native_tls::TlsAcceptor::new(identity).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("https://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let handle = thread::spawn(move || {
            let end = Instant::now() + Duration::from_secs(20);
            let mut index = 0;
            while !stopped.load(Ordering::Relaxed) && Instant::now() < end {
                let Ok((socket, _)) = listener.accept() else {
                    thread::sleep(Duration::from_millis(2));
                    continue;
                };
                socket.set_nonblocking(false).unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let Ok(mut tls) = acceptor.accept(socket) else {
                    continue;
                };
                let mut request = Vec::new();
                let mut byte = [0u8; 1];
                while request.len() < 32 * 1024 && !request.ends_with(b"\r\n\r\n") {
                    if tls.read_exact(&mut byte).is_err() {
                        break;
                    }
                    request.push(byte[0]);
                }
                let content_length = String::from_utf8_lossy(&request)
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .and_then(|s| s.trim().parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                if content_length > 96 * 1024 {
                    continue;
                }
                let header_bytes = request.len();
                request.resize(header_bytes + content_length, 0);
                if tls.read_exact(&mut request[header_bytes..]).is_err() {
                    continue;
                }
                recorded
                    .lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&request).into_owned());
                let Some(reply) = replies.get(index) else {
                    continue;
                };
                let adjusted;
                let reply = if reply.query_time {
                    let request = String::from_utf8_lossy(&request);
                    let time = request
                        .split("time=")
                        .nth(1)
                        .unwrap()
                        .split(['&', ' '])
                        .next()
                        .unwrap();
                    let raw = String::from_utf8_lossy(&reply.bytes);
                    let body = raw
                        .split("\r\n\r\n")
                        .nth(1)
                        .unwrap()
                        .replace("\"__TIME__\"", time);
                    adjusted = Reply::json(200, &body);
                    &adjusted
                } else {
                    reply
                };
                index += 1;
                thread::sleep(reply.delay_headers);
                if let Some(split) = reply
                    .bytes
                    .windows(4)
                    .position(|s| s == b"\r\n\r\n")
                    .map(|i| i + 4)
                {
                    let _ = tls.write_all(&reply.bytes[..split]);
                    let _ = tls.flush();
                    thread::sleep(reply.delay_body);
                    let _ = tls.write_all(&reply.bytes[split..]);
                } else {
                    let _ = tls.write_all(&reply.bytes);
                }
                let _ = tls.flush();
            }
        });
        Self {
            url,
            ca,
            requests,
            stop,
            thread: Some(handle),
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.take().unwrap().join().unwrap();
    }
}
