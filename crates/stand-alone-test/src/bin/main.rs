use clap::{Parser, Subcommand};
use stand_alone_test::{make_hnsw, prepare_dataset, search_hnsw};

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Prepare dataset for testing
    PrepareDataset {
        /// path to the data file in fvecs format
        #[arg(short, long)]
        data_file: String,

        /// directory to save the prepared dataset
        #[arg(short, long)]
        output_dir: String,
    },

    /// Make HNSW index
    MakeHnsw {
        /// directory of the prepared dataset
        #[arg(short, long)]
        data_dir: String,

        /// dimension of the data
        #[arg(long)]
        dims: u32,

        /// m parameter for HNSW index
        #[arg(short, long)]
        m: u32,

        /// ef_construction parameter for HNSW index
        #[arg(short, long)]
        ef_construction: u32,

        /// directory to save the hnsw index
        #[arg(short, long)]
        output_dir: String,
    },

    /// Search HNSW index
    SearchHnsw {
        /// directory of the prepared dataset
        #[arg(short, long)]
        data_dir: String,

        /// dimension of the data
        #[arg(long)]
        dims: u32,

        /// directory of the hnsw index
        #[arg(short, long)]
        hnsw_dir: String,

        /// path to the query file in fvecs format
        #[arg(short, long)]
        query_file: String,

        /// path to the ground truth file in ivecs format
        #[arg(short, long)]
        gt_file: String,

        /// ef_search parameter for HNSW search
        #[arg(long)]
        ef: u32,
    },
}

fn main() {
    let args = Args::parse();
    match &args.command {
        Commands::PrepareDataset {
            data_file,
            output_dir,
        } => {
            prepare_dataset(data_file, output_dir);
        }
        Commands::MakeHnsw {
            data_dir,
            dims,
            m,
            ef_construction,
            output_dir,
        } => {
            make_hnsw(data_dir, *dims, *m, *ef_construction, output_dir);
        }
        Commands::SearchHnsw {
            data_dir,
            dims,
            hnsw_dir,
            query_file,
            gt_file,
            ef,
        } => {
            search_hnsw(data_dir, *dims, hnsw_dir, query_file, gt_file, *ef);
        }
    }
}
