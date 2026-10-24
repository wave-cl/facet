//! `facet`: talk to a `facetd` daemon.

mod client;
mod cmd;
mod output;

use clap::{Parser, Subcommand};

use crate::output::Format;

#[derive(Parser, Debug)]
#[command(name = "facet", version, about = "Client for a facetd daemon")]
struct Cli {
    #[command(flatten)]
    conn: client::Conn,
    /// One JSON document per line instead of human lines.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// One entry by its whole key (none for a register).
    Get(cmd::query::GetArgs),
    /// Entries in key order.
    Scan(cmd::query::ScanArgs),
    /// A table's rows through a secondary index.
    Lookup(cmd::query::LookupArgs),
    /// A sorted set's partition in score order.
    Range(cmd::query::RangeArgs),
    /// The highest-scored members of a sorted set's partition.
    Top(cmd::query::TopArgs),
    /// Buckets of one key and width in time order.
    Buckets(cmd::query::BucketsArgs),
    /// Follow a collection's changes until stopped.
    Watch(cmd::query::WatchArgs),
    /// Every projection, or one.
    Projections {
        #[arg(value_name = "NAME")]
        name: Option<String>,
    },
    /// Events a projection skipped.
    DeadLetters(cmd::admin::DeadLettersArgs),
    /// Drop a projection's collections and start it over (admin).
    Rebuild { name: String },
    /// Pause a projection (admin).
    Pause { name: String },
    /// Resume a paused or failed projection (admin).
    Resume { name: String },
    /// Delete an orphaned projection's file (admin).
    Drop { name: String },
    /// One screen of health; exit 1 when degraded (a failed or paused
    /// projection, lag above --max-lag, a source disconnected, a fatal
    /// error), 2 when unreachable.
    Status(cmd::admin::StatusArgs),
    /// Whether the daemon reports SERVING (exit 0), NOT_SERVING (exit 1)
    /// or cannot be reached (exit 2).
    Health {
        #[arg(long, default_value = "facet.v1.Query")]
        service: String,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let format = if cli.json {
        Format::Json
    } else {
        Format::Human
    };
    let code = match run(cli, format).await {
        Ok(()) => 0,
        Err(e) => {
            let code = output::exit_code(&e);
            let text = output::render_error(&e);
            match format {
                Format::Json => eprintln!("{}", serde_json::json!({ "error": text, "code": code })),
                Format::Human => eprintln!("facet: {text}"),
            }
            code
        }
    };
    std::process::exit(code);
}

async fn run(cli: Cli, format: Format) -> anyhow::Result<()> {
    let conn = &cli.conn;
    match cli.command {
        Commands::Get(a) => cmd::query::get(conn, format, a).await,
        Commands::Scan(a) => cmd::query::scan(conn, format, a).await,
        Commands::Lookup(a) => cmd::query::lookup(conn, format, a).await,
        Commands::Range(a) => cmd::query::range(conn, format, a).await,
        Commands::Top(a) => cmd::query::top(conn, format, a).await,
        Commands::Buckets(a) => cmd::query::buckets(conn, format, a).await,
        Commands::Watch(a) => cmd::query::watch(conn, format, a).await,
        Commands::Projections { name } => cmd::admin::projections(conn, format, name).await,
        Commands::DeadLetters(a) => cmd::admin::dead_letters(conn, format, a).await,
        Commands::Rebuild { name } => cmd::admin::rebuild(conn, format, &name).await,
        Commands::Pause { name } => cmd::admin::pause(conn, format, &name).await,
        Commands::Resume { name } => cmd::admin::resume(conn, format, &name).await,
        Commands::Drop { name } => cmd::admin::drop(conn, format, &name).await,
        Commands::Status(a) => cmd::admin::status(conn, format, a).await,
        Commands::Health { service } => cmd::admin::health(conn, format, &service).await,
    }
}
