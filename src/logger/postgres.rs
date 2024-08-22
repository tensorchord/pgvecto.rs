use log::Record;
use std::borrow::Cow;

const PIPE_CHUNK_SIZE: usize = size_of::<pgrx::pg_sys::PipeProtoChunk>();
const PIPE_HEADER_SIZE: usize = 9;

pub fn send_message_to_server_log(record: &Record) {
    let log_destination = unsafe { pgrx::pg_sys::Log_destination };
    if log_destination as u32 & pgrx::pg_sys::LOG_DESTINATION_STDERR != 0 {
        write_stderr(record);
    }
    if log_destination as u32 & pgrx::pg_sys::LOG_DESTINATION_CSVLOG != 0 {
        write_csvlog(record);
    }
    #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
    if log_destination as u32 & pgrx::pg_sys::LOG_DESTINATION_JSONLOG != 0 {
        write_jsonlog(record);
    }
}

#[derive(Debug, Clone, Copy)]
pub enum LogDestination {
    Stderr,
    Csvlog,
    #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
    Jsonlog,
}

fn write_stderr(record: &Record) {
    let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
    let pid = cached_pid();
    let error_severity = error_severity(record.level());
    let message = record.args();
    let mut msg = format!("{timestamp} [{pid}] {error_severity}: {message}");
    msg.push('\n');
    write_pipe_chunks(pid, msg.as_bytes(), LogDestination::Stderr)
}

fn write_csvlog(record: &Record) {
    let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
    let pid = cached_pid();
    let error_severity = error_severity(record.level());
    let message = record.args();
    // 2024-07-10 17:08:40.318 CST,,,97854,,668e4f98.17e3e,6,,2024-07-10 17:08:40 CST,,0,LOG,00000,"database system is ready to accept connections",,,,,,,,,"","postmaster",,0
    let mut msg = [
        &timestamp.to_string(),            // log_time
        "",                                // user_name
        "",                                // database_name
        &pid.to_string(),                  // process_id
        "",                                // connection_from
        "",                                // session_id
        "",                                // session_line_num
        "",                                // command_tag
        "",                                // session_start_time
        "",                                // virtual_transaction_id
        "",                                // transaction_id
        error_severity,                    // error_severity
        "",                                // sql_state_code
        &escape_csv(&message.to_string()), // message
        "",                                // detail
        "",                                // hint
        "",                                // internal_query
        "",                                // internal_query_pos
        "",                                // context
        "",                                // query
        "",                                // query_pos
        "",                                // location
        "",                                // application_name
        &format!("{:?}", "vectors"),       // backend_type
        "",                                // leader_pid
        "",                                // query_id
    ]
    .join(",");
    msg.push('\n');
    write_pipe_chunks(pid, msg.as_bytes(), LogDestination::Csvlog);
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
fn write_jsonlog(record: &Record) {
    use std::fmt::Write;
    let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
    let pid = cached_pid();
    let error_severity = error_severity(record.level());
    let message = record.args();
    let mut msg = "{".to_string();
    write!(msg, "{:?}:{:?},", "timestamp", timestamp.to_string()).unwrap();
    write!(msg, "{:?}:{},", "pid", pid).unwrap();
    write!(msg, "{:?}:{:?},", "error_severity", error_severity).unwrap();
    write!(msg, "{:?}:{:?},", "message", message.to_string()).unwrap();
    write!(msg, "{:?}:{:?},", "backend_type", "vectors").unwrap();
    msg.pop();
    msg.push('}');
    msg.push('\n');
    write_pipe_chunks(pid, msg.as_bytes(), LogDestination::Jsonlog);
}

fn write_pipe_chunks(pid: u32, mut msg: &[u8], dest: LogDestination) {
    use std::io::Write;
    // postgresql assumes that logs from same pid are continuous
    let mut stderr = std::io::stderr().lock();
    while !msg.is_empty() {
        let len = std::cmp::min(msg.len(), PIPE_CHUNK_SIZE - PIPE_HEADER_SIZE);
        #[cfg(feature = "pg14")]
        let is_last = match (dest, len == msg.len()) {
            (LogDestination::Stderr, true) => b't',
            (LogDestination::Stderr, false) => b'f',
            (LogDestination::Csvlog, true) => b'T',
            (LogDestination::Csvlog, false) => b'F',
        };
        #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
        let flags = match dest {
            LogDestination::Stderr => pgrx::pg_sys::PIPE_PROTO_DEST_STDERR,
            LogDestination::Csvlog => pgrx::pg_sys::PIPE_PROTO_DEST_CSVLOG,
            LogDestination::Jsonlog => pgrx::pg_sys::PIPE_PROTO_DEST_JSONLOG,
        } | if len == msg.len() {
            pgrx::pg_sys::PIPE_PROTO_IS_LAST
        } else {
            0
        };
        let mut chunk = Vec::with_capacity(PIPE_CHUNK_SIZE);
        chunk.extend([0_u8, 0_u8]);
        chunk.extend((len as u16).to_le_bytes());
        chunk.extend((pid as i32).to_le_bytes());
        #[cfg(feature = "pg14")]
        chunk.extend((is_last as u8).to_le_bytes());
        #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
        chunk.extend((flags as u8).to_le_bytes());
        chunk.extend_from_slice(&msg[..len]);
        msg = &msg[len..];
        let _ = stderr.write(&chunk);
    }
}

fn escape_csv(s: &str) -> Cow<'_, str> {
    if s.bytes().any(|c| matches!(c, b'"' | b',' | b'\n' | b'\r')) {
        let mut escaped = String::with_capacity(2 + s.len() * 2);
        escaped.push('"');
        for c in s.chars() {
            if c == '"' {
                escaped.push('"');
                escaped.push('"');
            } else {
                escaped.push(c);
            }
        }
        escaped.push('"');
        Cow::from(escaped)
    } else {
        Cow::from(s)
    }
}

// it cannot be cached by std because of `fork` syscall
fn cached_pid() -> u32 {
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::Relaxed;
    static PID: AtomicU64 = AtomicU64::new(u64::MAX);
    let cached = PID.load(Relaxed);
    if cached < u32::MAX as u64 {
        cached as u32
    } else {
        let val = std::process::id();
        PID.store(val as u64, Relaxed);
        val
    }
}

fn error_severity(log: log::Level) -> &'static str {
    match log {
        log::Level::Error => "ERROR",
        log::Level::Warn => "WARNING",
        log::Level::Info => "INFO",
        log::Level::Debug => "DEBUG",
        log::Level::Trace => "DEBUG",
    }
}
