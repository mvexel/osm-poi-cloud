use anyhow::{bail, Context, Result};
use aws_sdk_s3::primitives::ByteStream;
use clap::Parser;
use h3o::{CellIndex, LatLng, Resolution};
use hashbrown::HashMap;
use osmpbf::{Element, ElementReader};
use serde::Serialize;
use std::path::{Path, PathBuf};

/// CLI parameters - all can be set via environment variables.
#[derive(Parser, Debug)]
#[command(author, version, about = "Shard an OSM planet file into H3 cells")]
struct Args {
    /// Path to the .osm.pbf file to scan.
    #[arg(env = "OSM_FILE")]
    osm_file: PathBuf,

    /// Highest H3 resolution to use when binning nodes (0-15).
    #[arg(env = "MAX_RESOLUTION", default_value = "7")]
    max_resolution: u8,

    /// Maximum number of nodes allowed per shard before splitting.
    #[arg(env = "MAX_NODES_PER_SHARD", default_value = "5000000")]
    max_nodes: u64,

    /// S3 bucket to write the manifest to (optional - if not set, writes to stdout).
    #[arg(long, env = "S3_BUCKET")]
    s3_bucket: Option<String>,

    /// Run ID for organizing outputs in S3.
    #[arg(long, env = "RUN_ID")]
    run_id: Option<String>,
}

/// Aggregated counts for every resolution plus the total number of nodes we saw.
struct ScanResult {
    counts: Vec<HashMap<CellIndex, u64>>,
    node_total: u64,
}

/// One shard entry combining the cell index with its aggregated count.
#[derive(Clone, Copy)]
struct Shard {
    cell: CellIndex,
    node_count: u64,
}

/// GeoJSON FeatureCollection wrapper used for serialization.
#[derive(Serialize)]
struct FeatureCollection {
    #[serde(rename = "type")]
    feature_type: &'static str,
    features: Vec<Feature>,
}

/// GeoJSON Feature with the handful of properties we need.
#[derive(Serialize)]
struct Feature {
    #[serde(rename = "type")]
    feature_type: &'static str,
    properties: Properties,
    geometry: Geometry,
}

/// Properties exposed for each shard.
#[derive(Serialize)]
struct Properties {
    h3_index: String,
    resolution: u8,
    node_count: u64,
}

/// Minimal Polygon geometry representation.
#[derive(Serialize)]
struct Geometry {
    #[serde(rename = "type")]
    geometry_type: &'static str,
    coordinates: Vec<Vec<[f64; 2]>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if !args.osm_file.exists() {
        bail!("file does not exist: {}", args.osm_file.display());
    }

    // Convert the requested resolution to the strongly typed enum and validate range.
    let max_resolution = Resolution::try_from(args.max_resolution)
        .context("max_resolution must be between 0 and 15")?;

    eprintln!(
        "Scanning {} at resolution {}...",
        args.osm_file.display(),
        args.max_resolution
    );
    let scan = scan_osm(&args.osm_file, max_resolution)?;
    eprintln!(
        "Scan complete.  {} nodes in {} populated max-resolution cells.",
        scan.node_total,
        scan.counts[resolution_index(max_resolution)].len()
    );

    eprintln!(
        "Building shards (max nodes per shard = {})...",
        args.max_nodes
    );
    let shards = build_shards(&scan.counts, max_resolution, args.max_nodes);
    eprintln!("Generated {} shards.", shards.len());

    // Generate GeoJSON
    let geojson = generate_geojson(&shards)?;

    // Output to S3 or stdout
    if let (Some(bucket), Some(run_id)) = (&args.s3_bucket, &args.run_id) {
        eprintln!("Uploading manifest to S3...");
        upload_to_s3(&geojson, bucket, run_id).await?;
        eprintln!(
            "Manifest uploaded to s3://{}/runs/{}/shards/manifest.json",
            bucket, run_id
        );
    } else {
        eprintln!("Writing GeoJSON to stdout...");
        println!("{}", geojson);
    }

    Ok(())
}

/// Stream the PBF in parallel, map every node to its H3 cell, and keep tallies for each resolution.
fn scan_osm(path: &Path, max_resolution: Resolution) -> Result<ScanResult> {
    let reader = ElementReader::from_path(path)
        .with_context(|| format!("unable to open {}", path.display()))?;

    let max_res_u8 = u8::from(max_resolution);

    // Use par_map_reduce for parallel processing of PBF blocks
    let (counts, node_total) = reader.par_map_reduce(
        // Map function: process each element and return local counts
        |element| {
            let mut local_counts: Vec<HashMap<CellIndex, u64>> =
                (0..=max_res_u8).map(|_| HashMap::new()).collect();
            let mut local_total = 0u64;

            if let Element::DenseNode(node) = element {
                // Convert the node coordinates into an H3 cell at the requested resolution.
                if let Ok(location) = LatLng::new(node.lat(), node.lon()) {
                    let cell = location.to_cell(max_resolution);
                    let max_index = usize::from(max_res_u8);

                    // Count the node for the max-resolution cell
                    *local_counts[max_index].entry(cell).or_insert(0) += 1;

                    // Bubble the update up the tree to parent resolutions
                    let mut current = cell;
                    for parent_res in (0..max_res_u8).rev() {
                        let resolution =
                            Resolution::try_from(parent_res).expect("valid resolution");
                        current = current
                            .parent(resolution)
                            .expect("H3 parent must exist at lower resolution");
                        let idx = usize::from(parent_res);
                        *local_counts[idx].entry(current).or_insert(0) += 1;
                    }

                    local_total = 1;
                }
            }

            (local_counts, local_total)
        },
        // Identity function: create empty state
        || {
            (
                (0..=max_res_u8).map(|_| HashMap::new()).collect::<Vec<_>>(),
                0u64,
            )
        },
        // Reduce function: merge two results
        |mut acc, item| {
            // Merge counts from item into accumulator
            for (res_idx, item_map) in item.0.into_iter().enumerate() {
                for (cell, count) in item_map {
                    *acc.0[res_idx].entry(cell).or_insert(0) += count;
                }
            }
            acc.1 += item.1;
            acc
        },
    )?;

    Ok(ScanResult { counts, node_total })
}

/// Translate the hierarchical counts into the final set of shards.
fn build_shards(
    counts: &[HashMap<CellIndex, u64>],
    max_resolution: Resolution,
    max_nodes: u64,
) -> Vec<Shard> {
    let mut shards = Vec::new();
    let mut oversized = Vec::new();

    if counts.is_empty() {
        return shards;
    }

    // Start splitting from every populated resolution-0 cell.
    if let Some(root_counts) = counts.get(0) {
        for (&cell, _) in root_counts.iter() {
            subdivide(
                cell,
                Resolution::Zero,
                counts,
                max_resolution,
                max_nodes,
                &mut shards,
                &mut oversized,
            );
        }
    }

    if !oversized.is_empty() {
        eprintln!(
            "Warning: {} cells at max resolution exceeded the node threshold (showing up to 5):",
            oversized.len()
        );
        for shard in oversized.iter().take(5) {
            eprintln!(
                "  {} -> {} nodes (max {})",
                shard.cell, shard.node_count, max_nodes
            );
        }
        if oversized.len() > 5 {
            eprintln!("  ... and {} more", oversized.len() - 5);
        }
    }

    shards
}

/// Recursively split a cell until it satisfies the node constraint or we hit max resolution.
fn subdivide(
    cell: CellIndex,
    resolution: Resolution,
    counts: &[HashMap<CellIndex, u64>],
    max_resolution: Resolution,
    max_nodes: u64,
    shards: &mut Vec<Shard>,
    oversized: &mut Vec<Shard>,
) {
    let res_idx = resolution_index(resolution);
    let count = counts
        .get(res_idx)
        .and_then(|map| map.get(&cell).copied())
        .unwrap_or(0);

    if count == 0 {
        return;
    }

    if count <= max_nodes || resolution == max_resolution {
        let shard = Shard {
            cell,
            node_count: count,
        };
        shards.push(shard);
        if count > max_nodes && resolution == max_resolution {
            oversized.push(shard);
        }
        return;
    }

    let child_res_value = u8::from(resolution) + 1;
    let child_resolution = Resolution::try_from(child_res_value).expect("resolution bounds");

    for child in cell.children(child_resolution) {
        let child_idx = resolution_index(child_resolution);
        let child_count = counts
            .get(child_idx)
            .and_then(|map| map.get(&child).copied())
            .unwrap_or(0);
        if child_count > 0 {
            subdivide(
                child,
                child_resolution,
                counts,
                max_resolution,
                max_nodes,
                shards,
                oversized,
            );
        }
    }
}

/// Convert the shard list into a GeoJSON string.
fn generate_geojson(shards: &[Shard]) -> Result<String> {
    let mut features = Vec::with_capacity(shards.len());

    for shard in shards {
        let ring = polygon_ring(shard.cell);
        features.push(Feature {
            feature_type: "Feature",
            properties: Properties {
                h3_index: shard.cell.to_string(),
                resolution: u8::from(shard.cell.resolution()),
                node_count: shard.node_count,
            },
            geometry: Geometry {
                geometry_type: "Polygon",
                coordinates: vec![ring],
            },
        });
    }

    let collection = FeatureCollection {
        feature_type: "FeatureCollection",
        features,
    };

    Ok(serde_json::to_string_pretty(&collection)?)
}

/// Upload the GeoJSON manifest to S3.
async fn upload_to_s3(content: &str, bucket: &str, run_id: &str) -> Result<()> {
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let client = aws_sdk_s3::Client::new(&config);

    let key = format!("runs/{}/shards/manifest.json", run_id);

    client
        .put_object()
        .bucket(bucket)
        .key(&key)
        .body(ByteStream::from(content.as_bytes().to_vec()))
        .content_type("application/json")
        .send()
        .await
        .context("failed to upload manifest to S3")?;

    Ok(())
}

/// Build a single closed polygon ring for the provided cell.
fn polygon_ring(cell: CellIndex) -> Vec<[f64; 2]> {
    let mut ring: Vec<[f64; 2]> = cell
        .boundary()
        .iter()
        .map(|vertex| [vertex.lng(), vertex.lat()])
        .collect();

    if let Some(first) = ring.first().copied() {
        ring.push(first);
    }

    ring
}

/// Helper to turn a resolution into a vector index.
fn resolution_index(resolution: Resolution) -> usize {
    usize::from(u8::from(resolution))
}
