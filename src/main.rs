use elasticsearch::{Elasticsearch, IndexParts, http::transport::Transport};
use mail_parser::{Message, MimeHeaders};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::error::Error;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use rayon::prelude::*;
use walkdir::WalkDir;
use indicatif::{ProgressBar, ProgressStyle};
use futures::stream::{self, StreamExt};
use tokio::time::{sleep, Duration};
use md5;
use reqwest::StatusCode;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct AttachmentInfo {
    filename: String,
    content_type: String,
    content_id: Option<String>,
    content_disposition: Option<String>,
    content_transfer_encoding: Option<String>,
    size: usize,
    saved_path: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct EmailDocument {
    message_id: Option<String>,
    from_debug: String,
    to_debug: String,
    cc_debug: String,
    bcc_debug: String,
    subject: Option<String>,
    date: Option<String>,
    body_text: Option<String>,
    body_html: Option<String>,
    attachments: Vec<AttachmentInfo>,
    headers: HashMap<String, String>,
    source_file: String,
    source_folder: String,
    indexed_date: String,
}

struct EmlProcessor {
    client: Elasticsearch,
    index_name: String,
    attachment_dir: String,
}

fn sanitize_filename(name: &str) -> String {
    name.chars()
        .filter(|c| !r#"<>:"/\|?*"#.contains(*c))
        .map(|c| if c.is_ascii() { c } else { '_' })
        .collect()
}

fn parse_eml_file_static(file_path: &Path, attachment_dir: &str) -> Result<EmailDocument, Box<dyn Error>> {
    let mut file = File::open(file_path)?;
    let mut contents = Vec::new();
    file.read_to_end(&mut contents)?;

    let message = Message::parse(&contents).ok_or("Failed to parse email")?;

    let mut headers = HashMap::new();
    for header in message.headers() {
        headers.insert(format!("{:?}", header.name), format!("{:?}", header.value));
    }

    let from_debug = format!("{:?}", message.from());
    let to_debug = format!("{:?}", message.to());
    let cc_debug = format!("{:?}", message.cc());
    let bcc_debug = format!("{:?}", message.bcc());
    let body_text = message.body_text(0).map(|s| s.to_string());
    let body_html = message.body_html(0).map(|s| s.to_string());

    let source_folder = file_path.parent().and_then(|p| p.to_str()).unwrap_or("unknown").to_string();
    let folder_hash = format!("{:x}", md5::compute(source_folder.as_bytes()));
    let folder_path = Path::new(attachment_dir).join(&folder_hash);
    fs::create_dir_all(&folder_path)?;

    let mut attachments = Vec::new();
    for attachment in message.attachments() {
        let filename_raw = attachment.attachment_name().unwrap_or("unknown");
        let filename = sanitize_filename(filename_raw);
        let content_type = format!("{:?}", attachment.content_type());
        let content_id = attachment.content_id().map(|s| s.to_string());
        let content_disposition = attachment.content_disposition().map(|d| format!("{:?}", d));
        let content_transfer_encoding = attachment.content_transfer_encoding().map(|e| format!("{:?}", e));

        let (bytes, size) = match &attachment.body {
            mail_parser::PartType::Text(cow_str) |
            mail_parser::PartType::Html(cow_str) => {
                let bytes = cow_str.as_bytes().to_vec();
                (bytes.clone(), bytes.len())
            }
            mail_parser::PartType::Binary(bytes) |
            mail_parser::PartType::InlineBinary(bytes) => {
                let bytes = bytes.to_vec();
                (bytes.clone(), bytes.len())
            }
            _ => {
                continue;
            }
        };

        let save_path = folder_path.join(&filename);
        fs::write(&save_path, &bytes)?;

        attachments.push(AttachmentInfo {
            filename,
            content_type,
            content_id,
            content_disposition,
            content_transfer_encoding,
            size,
            saved_path: save_path.to_string_lossy().to_string(),
        });
    }

    Ok(EmailDocument {
        message_id: message.message_id().map(|s| s.to_string()),
        from_debug,
        to_debug,
        cc_debug,
        bcc_debug,
        subject: message.subject().map(|s| s.to_string()),
        date: message.date().map(|d| d.to_rfc3339()),
        body_text,
        body_html,
        attachments,
        headers,
        source_file: file_path.file_name().and_then(|f| f.to_str()).unwrap_or("unknown").to_string(),
        source_folder,
        indexed_date: chrono::Utc::now().to_rfc3339(),
    })
}

impl EmlProcessor {
    fn new(es_url: &str, index_name: &str, attachment_dir: &str) -> Result<Self, Box<dyn Error>> {
        let transport = Transport::single_node(es_url)?;
        let client = Elasticsearch::new(transport);
        Ok(Self {
            client,
            index_name: index_name.to_string(),
            attachment_dir: attachment_dir.to_string(),
        })
    }

    async fn setup_index(&self) -> Result<(), Box<dyn Error>> {
        let exists = self.client
            .indices()
            .exists(elasticsearch::indices::IndicesExistsParts::Index(&[&self.index_name]))
            .send()
            .await?;

        if exists.status_code() == 404 {
            let res = self.client
                .indices()
                .create(elasticsearch::indices::IndicesCreateParts::Index(&self.index_name))
                .body(json!({
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0
                    },
                    "mappings": {
                        "properties": {
                            "message_id": { "type": "keyword" },
                            "from_debug": { "type": "text" },
                            "to_debug": { "type": "text" },
                            "cc_debug": { "type": "text" },
                            "bcc_debug": { "type": "text" },
                            "subject": { "type": "text" },
                            "date": { "type": "date" },
                            "body_text": { "type": "text" },
                            "body_html": { "type": "text" },
                            "attachments": {
                                "type": "nested",
                                "properties": {
                                    "filename": { "type": "keyword" },
                                    "content_type": { "type": "keyword" },
                                    "content_id": { "type": "keyword" },
                                    "content_disposition": { "type": "keyword" },
                                    "content_transfer_encoding": { "type": "keyword" },
                                    "size": { "type": "integer" },
                                    "saved_path": { "type": "keyword" }
                                }
                            },
                            "headers": { "type": "object", "enabled": false },
                            "source_file": { "type": "keyword" },
                            "source_folder": { "type": "keyword" },
                            "indexed_date": { "type": "date" }
                        }
                    }
                }))
                .send()
                .await?;

            println!("🆕 Created index `{}`: {}", self.index_name, res.status_code());
        }

        Ok(())
    }

    async fn index_email(&self, email: EmailDocument) -> Result<(), Box<dyn std::error::Error>> {
        for attempt in 1..=5 {
            let response = self.client
                .index(IndexParts::Index(&self.index_name))
                .body(email.clone())
                .send()
                .await?;

            if response.status_code().is_success() {
                return Ok(());
            }

            let status = response.status_code();
            if status == StatusCode::TOO_MANY_REQUESTS {
                eprintln!("🛑 Hit ES backpressure (429) attempt {}. Retrying...", attempt);
            } else if status.as_u16() >= 500 {
                eprintln!("⚠️ Server error {} attempt {}", status, attempt);
            } else {
                let body = response.text().await.unwrap_or_default();
                return Err(format!("❌ Failed to index: {} - {}", status, body).into());
            }

            let delay = Duration::from_millis(200 * 2u64.pow(attempt - 1));
            sleep(delay).await;
        }

        Err("❌ Gave up retrying document".into())
    }

    async fn process_directory(&self, dir: &Path, batch_size: usize) -> Result<(), Box<dyn Error>> {
        use indicatif::MultiProgress;
        use chrono::Utc;
        use std::fs::create_dir_all;
        use std::io::Write;

        let eml_files: Vec<_> = WalkDir::new(dir)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "eml"))
            .map(|e| e.path().to_path_buf())
            .collect();

        let total = eml_files.len();
        println!("📦 Found {} .eml files", total);

        // Prepare log file
        let timestamp = Utc::now().format("%Y%m%d-%H%M%S").to_string();
        let log_dir = Path::new("logs");
        create_dir_all(log_dir)?;
        let log_path = log_dir.join(format!("log-{}.txt", timestamp));
        let log_file = Arc::new(parking_lot::Mutex::new(fs::File::create(&log_path)?));

        let m = MultiProgress::new();
        let global_pb = m.add(ProgressBar::new(total as u64));
        global_pb.set_style(
            ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} total ({eta})")
                .unwrap()
        );
        global_pb.println("Starting indexing...");

        for (i, chunk) in eml_files.chunks(batch_size).enumerate() {
            global_pb.println(format!("📦 Processing batch {}/{}", i + 1, (total + batch_size - 1) / batch_size));

            let batch_pb = m.add(ProgressBar::new(chunk.len() as u64));
            batch_pb.set_style(
                ProgressStyle::with_template("  ↳ [{elapsed_precise}] [{bar:40.magenta/blue}] {pos}/{len} this batch ({eta})")
                    .unwrap()
            );

            let log_file_clone = Arc::clone(&log_file);
            let parsed: Vec<_> = chunk
                .par_iter()
                .filter_map(|p| {
                    match parse_eml_file_static(p, &self.attachment_dir) {
                        Ok(email) => Some(email),
                        Err(e) => {
                            let mut log = log_file_clone.lock();
                            writeln!(log, "⚠️ Skipping file {:?}: {}", p, e).ok();
                            None
                        }
                    }
                })
                .collect();

            let success = Arc::new(AtomicUsize::new(0));
            let failed = Arc::new(AtomicUsize::new(0));
            let log_file_clone = Arc::clone(&log_file);

            stream::iter(parsed)
                .map(|email| {
                    let success = Arc::clone(&success);
                    let failed = Arc::clone(&failed);
                    let batch_pb = batch_pb.clone();
                    let global_pb = global_pb.clone();
                    let log_file = Arc::clone(&log_file_clone);

                    async move {
                        match self.index_email(email).await {
                            Ok(_) => {
                                success.fetch_add(1, Ordering::SeqCst);
                            }
                            Err(e) => {
                                failed.fetch_add(1, Ordering::SeqCst);
                                let mut log = log_file.lock();
                                writeln!(log, "❌ Failed to index email: {}", e).ok();
                            }
                        }

                        batch_pb.inc(1);
                        global_pb.inc(1);
                        sleep(Duration::from_millis(1)).await;
                    }
                })
                .buffer_unordered(1)
                .collect::<Vec<_>>()
                .await;

            batch_pb.finish_and_clear();
            let s = success.load(Ordering::SeqCst);
            let f = failed.load(Ordering::SeqCst);

            // Add to log instead of stdout
            let mut log = log_file.lock();
            writeln!(log, "Indexed: {} | Failed: {}", s, f).ok();
        }

        println!("✅ Proccess Completed!");

        let mut log = log_file.lock();
        writeln!(log, "Completed run at {}", Utc::now().to_rfc3339()).ok();
        writeln!(log, "Log saved to {:?}", log_path).ok();

        Ok(())
    }




}


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("❌ Usage: emlParser <email_directory> [--batch N] [--attachments <dir>]");
        std::process::exit(1);
    }

    let mut email_dir = "";
    let mut batch_size = 50;
    let mut attachment_dir = "attachments";

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--batch" => {
                if let Some(val) = args.get(i + 1) {
                    batch_size = val.parse().unwrap_or(50);
                    i += 1;
                }
            }
            "--attachments" => {
                if let Some(val) = args.get(i + 1) {
                    attachment_dir = val;
                    i += 1;
                }
            }
            val => {
                if !val.starts_with("--") {
                    email_dir = val;
                }
            }
        }
        i += 1;
    }

    let processor = EmlProcessor::new("http://localhost:9200", "emails", attachment_dir)?;
    processor.setup_index().await?;
    processor.process_directory(Path::new(email_dir), batch_size).await?;
    Ok(())
}
