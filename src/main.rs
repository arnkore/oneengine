use oneengine::core::engine::OneEngine;
use oneengine::utils::config::Config;
use tracing::{info, error};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("Starting OneEngine - Unified Native Engine");

    // Load configuration
    let config = Config::load()?;
    info!("Configuration loaded: {:?}", config);

    // Create and start the engine
    let mut engine = OneEngine::new(config).await?;
    
    match engine.start().await {
        Ok(_) => {
            info!("OneEngine started successfully");
            engine.run().await?;
        }
        Err(e) => {
            error!("Failed to start OneEngine: {}", e);
            return Err(e);
        }
    }

    Ok(())
}
