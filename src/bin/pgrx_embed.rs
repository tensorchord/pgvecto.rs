macro_rules! pgrx_embed {
    () => {
        #[cfg(not(pgrx_embed))]
        fn main() {
            panic!("PGRX_EMBED was not set.");
        }
        #[cfg(pgrx_embed)]
        include!(env!("PGRX_EMBED"));
    };
}

pgrx_embed!();
