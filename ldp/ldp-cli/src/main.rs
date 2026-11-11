//! `ldp` — the Phase 1 command line (Operational Details §3).
//!
//! ```text
//! ldp run <pipeline.yaml> [--threads N] [--state-file PATH] [--log-level L]
//! ldp validate <pipeline.yaml>
//! ```
//!
//! Exit codes: 0 success · 1 configuration/validation/planning · 2 runtime.
//! (`validate` is an addition to the required surface; it makes DoD #7 testable
//! without a database.)

use ldp_config::Loaded;
use ldp_core::plan::{PartitionKind, SinkKind, SinkSpec, TransformSpec};
use ldp_core::runner::{run, SinkBinding};
use ldp_core::{LdpError, LogLevel, Logger, Sink};
use ldp_csv::CsvSink;
use ldp_parquet::ParquetSink;
use ldp_postgres::PostgresSource;
use std::path::Path;
use std::sync::Arc;

const USAGE: &str = "\
ldp - Ledbetter Data Pump (Phase 1)

USAGE:
    ldp run <pipeline.yaml> [--threads N] [--state-file PATH] [--log-level LEVEL]
    ldp validate <pipeline.yaml>

OPTIONS:
    --threads <N>        override settings.max_threads
    --state-file <PATH>  override settings.state_file
    --log-level <LEVEL>  error | warn | info | debug   (default: info)

EXIT CODES:
    0  success
    1  configuration / validation / planning error
    2  runtime error (source, transform, sink, checkpoint)
";

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let code = dispatch(&args);
    std::process::exit(code);
}

fn dispatch(args: &[String]) -> i32 {
    match args.first().map(|s| s.as_str()) {
        Some("run") => cmd_run(&args[1..]),
        Some("validate") => cmd_validate(&args[1..]),
        Some("-h") | Some("--help") | Some("help") => {
            print!("{USAGE}");
            0
        }
        None => {
            print!("{USAGE}");
            1
        }
        Some(other) => {
            eprintln!("ldp: unknown subcommand '{other}'\n");
            print!("{USAGE}");
            1
        }
    }
}

#[derive(Debug, Default)]
struct RunOpts {
    file: Option<String>,
    threads: Option<usize>,
    state_file: Option<String>,
    log_level: Option<LogLevel>,
}

fn parse_run_opts(args: &[String]) -> Result<RunOpts, String> {
    let mut o = RunOpts::default();
    let mut i = 0usize;
    while i < args.len() {
        match args[i].as_str() {
            "--threads" => {
                let v = args.get(i + 1).ok_or("--threads needs a value")?;
                let n: usize = v
                    .parse()
                    .map_err(|_| format!("--threads expects an integer, got '{v}'"))?;
                if n == 0 {
                    return Err("--threads must be >= 1".into());
                }
                o.threads = Some(n);
                i += 2;
            }
            "--state-file" => {
                o.state_file = Some(
                    args.get(i + 1)
                        .ok_or("--state-file needs a value")?
                        .to_string(),
                );
                i += 2;
            }
            "--log-level" => {
                let v = args.get(i + 1).ok_or("--log-level needs a value")?;
                o.log_level = Some(LogLevel::parse(v).ok_or_else(|| {
                    format!("--log-level must be error|warn|info|debug, got '{v}'")
                })?);
                i += 2;
            }
            flag if flag.starts_with("--") => {
                return Err(format!("unknown option '{flag}'"));
            }
            positional => {
                if o.file.is_some() {
                    return Err(format!("unexpected extra argument '{positional}'"));
                }
                o.file = Some(positional.to_string());
                i += 1;
            }
        }
    }
    if o.file.is_none() {
        return Err("missing <pipeline.yaml>".into());
    }
    Ok(o)
}

fn cmd_validate(args: &[String]) -> i32 {
    let opts = match parse_run_opts(args) {
        Ok(o) => o,
        Err(msg) => {
            eprintln!("ldp: {msg}");
            return 1;
        }
    };
    let file = opts.file.expect("checked by parse_run_opts");
    match ldp_config::load(Path::new(&file)) {
        Ok(loaded) => {
            print_plan(&loaded);
            for w in &loaded.warnings {
                eprintln!("warning: {w}");
            }
            0
        }
        Err(e) => {
            print_error(&e);
            e.exit_code()
        }
    }
}

fn print_plan(l: &Loaded) {
    let p = &l.plan;
    println!("pipeline:  {}", p.name);
    println!("source:    {} via {}", p.source.id, p.source.connection);
    let kind = match p.source.partitioning.kind {
        PartitionKind::Range => "range",
        PartitionKind::None => "none",
    };
    println!(
        "partition: {} x {}",
        p.source.partitioning.chunks.unwrap_or(1),
        kind
    );
    println!("threads:   {}", p.settings.max_threads);
    println!("state:     {}", p.settings.state_file);
    for t in &p.transformations {
        match t {
            TransformSpec::Filter {
                input, condition, ..
            } => println!("  filter {} <- {}: {}", t.id(), input, condition),
            TransformSpec::Derive { input, fields, .. } => {
                let desc = fields
                    .iter()
                    .map(|f| format!("{} = {}", f.name, f.source_text))
                    .collect::<Vec<_>>()
                    .join(", ");
                println!("  derive {} <- {}: {}", t.id(), input, desc);
            }
        }
    }
    for t in &p.targets {
        println!("target:    {} ({}) -> {}", t.id, t.kind.as_str(), t.path);
    }
}

fn print_error(e: &LdpError) {
    match e {
        LdpError::Config(msg) => {
            eprintln!("configuration error:");
            for line in msg.lines() {
                eprintln!("  {line}");
            }
        }
        other => eprintln!("error: {other}"),
    }
}

fn cmd_run(args: &[String]) -> i32 {
    let opts = match parse_run_opts(args) {
        Ok(o) => o,
        Err(msg) => {
            eprintln!("ldp: {msg}");
            return 1;
        }
    };
    let level = opts.log_level.unwrap_or(LogLevel::Info);
    let file = opts.file.expect("checked by parse_run_opts");

    let mut loaded = match ldp_config::load(Path::new(&file)) {
        Ok(l) => l,
        Err(e) => {
            print_error(&e);
            return e.exit_code();
        }
    };

    if let Some(n) = opts.threads {
        loaded.plan.settings.max_threads = n;
    }
    if let Some(p) = opts.state_file {
        loaded.plan.settings.state_file = p;
    }
    for w in &loaded.warnings {
        eprintln!("warning: {w}");
    }

    let logger = Logger::new(loaded.plan.name.clone(), level);
    let plan = loaded.plan;

    let Some(conn) = plan.source_connection() else {
        eprintln!("configuration error: source references no connection");
        return 1;
    };
    let url = conn.url.clone();
    logger.info(
        "job_started",
        &format!(
            "source={} connection={} ({}) threads={} partitions={}",
            plan.source.id,
            conn.name,
            conn.redacted_url(),
            plan.settings.max_threads,
            plan.source.partitioning.chunks.unwrap_or(1)
        ),
    );

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("error: cannot start async runtime: {e}");
            return 2;
        }
    };

    let result = rt.block_on(async move {
        let source =
            PostgresSource::connect(plan.source.clone(), plan.settings.clone(), &url).await?;
        let mut bindings = Vec::with_capacity(plan.targets.len());
        for t in &plan.targets {
            bindings.push(make_sink(t)?);
        }
        run(&plan, Arc::new(source), bindings, &logger).await
    });

    match result {
        Ok(_) => 0,
        Err(e) => {
            print_error(&e);
            e.exit_code()
        }
    }
}

fn make_sink(spec: &SinkSpec) -> Result<SinkBinding, LdpError> {
    let sink: Arc<dyn Sink> = match spec.kind {
        SinkKind::Parquet => Arc::new(ParquetSink::new(&spec.path, spec.compression)),
        SinkKind::Csv => Arc::new(CsvSink::new(&spec.path)),
    };
    Ok(SinkBinding::new(sink, spec.clone()))
}
