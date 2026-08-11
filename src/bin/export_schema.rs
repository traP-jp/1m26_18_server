use std::{error::Error, fs};

use om26_18::rest;

fn main() -> Result<(), Box<dyn Error>> {
    let (_, openapi) = rest::setup_openapi_routes();

    fs::write("api/openapi.json", openapi.to_pretty_json()?)?;

    Ok(())
}
