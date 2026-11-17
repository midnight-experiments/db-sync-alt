use clap::{Parser, Subcommand};
use dsa_chain::profiles::Profile;

#[derive(Parser)]
#[command(version = dsa_chain::VERSION, about = "Cardano data for Midnight nodes")]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print a supported network's node profile.
    Profile {
        #[arg(long)]
        network: String,
    },
}

fn main() -> std::process::ExitCode {
    let result = match Arguments::parse().command {
        Command::Profile { network } => print_profile(&network),
    };
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn print_profile(network: &str) -> Result<(), Box<dyn std::error::Error>> {
    let profile = Profile::load(network)?;
    println!("{}", serde_json::to_string_pretty(&profile)?);
    Ok(())
}
