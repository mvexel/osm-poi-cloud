use anyhow::{bail, Context, Result};
use aws_sdk_s3::primitives::ByteStream;
use clap::Parser;
use hashbrown::HashMap;
use osmpbf::{Element, ElementReader};
use serde::Serialize;
use std::f64::consts::PI;
use std::path::{Path, PathBuf};

/// CLI parameters - all can be set via environment variables.
#[derive(Parser, Debug)]
#[command(author, version, about = "Shard an OSM planet file into quadtree tiles")]
struct Args {
    /// Path to the .osm.pbf file to scan.
    #[arg(env = "OSM_FILE")]
    osm_file: PathBuf,

    /// Highest Web Mercator zoom level to consider when splitting tiles.
    #[arg(env = "MAX_ZOOM", default_value = "14")]
    max_zoom: u8,

    /// Maximum number of nodes allowed per shard before splitting.
    #[arg(env = "MAX_NODES_PER_SHARD", default_value = "1000000")]
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
    counts: Vec<HashMap<(u32, u32), u64>>,
    node_total: u64,
}

/// One shard entry combining the cell index with its aggregated count.
#[derive(Clone, Copy)]
struct Shard {
    zoom: u8,
    x: u32,
    y: u32,
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
    shard_id: String,
    z: u8,
    x: u32,
    y: u32,
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

    eprintln!(
        "Scanning {} (max zoom = {})...",
        args.osm_file.display(),
        args.max_zoom
    );
    let scan = scan_osm(&args.osm_file, args.max_zoom)?;
    eprintln!(
        "Scan complete.  {} nodes in {} populated max-zoom tiles.",
        scan.node_total,
        scan.counts[usize::from(args.max_zoom)].len()
    );

    eprintln!(
        "Building shards (max nodes per shard = {})...",
        args.max_nodes
    );
    let shards = build_shards(&scan.counts, args.max_zoom, args.max_nodes);
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
fn scan_osm(path: &Path, max_zoom: u8) -> Result<ScanResult> {
    let reader = ElementReader::from_path(path)
        .with_context(|| format!("unable to open {}", path.display()))?;

    let max_zoom_usize = usize::from(max_zoom);

    // Use par_map_reduce for parallel processing of PBF blocks
    let (counts, node_total) = reader.par_map_reduce(
        // Map function: process each element and return local counts
        |element| {
            let mut local_counts: Vec<HashMap<(u32, u32), u64>> =
                (0..=max_zoom).map(|_| HashMap::new()).collect();
            let mut local_total = 0u64;

            let (lat, lon) = match element {
                Element::DenseNode(node) => (node.lat(), node.lon()),
                Element::Node(node) => (node.lat(), node.lon()),
                _ => return (local_counts, local_total),
            };

            if !(lat.is_finite() && lon.is_finite()) {
                return (local_counts, local_total);
            }

            if let Some((mut x, mut y)) = lon_lat_to_tile(lon, lat, max_zoom) {
                *local_counts[max_zoom_usize].entry((x, y)).or_insert(0) += 1;

                // Bubble up to parent zoom levels by shifting.
                for zoom in (0..max_zoom).rev() {
                    x >>= 1;
                    y >>= 1;
                    *local_counts[usize::from(zoom)].entry((x, y)).or_insert(0) += 1;
                }

                local_total = 1;
            }

            (local_counts, local_total)
        },
        // Identity function: create empty state
        || {
            (
                (0..=max_zoom).map(|_| HashMap::new()).collect::<Vec<_>>(),
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
    counts: &[HashMap<(u32, u32), u64>],
    max_zoom: u8,
    max_nodes: u64,
) -> Vec<Shard> {
    let mut shards = Vec::new();
    let mut oversized = Vec::new();

    if counts.is_empty() {
        return shards;
    }

    // Start splitting from every populated zoom-0 tile.
    if let Some(root_counts) = counts.get(0) {
        for (&(x, y), _) in root_counts.iter() {
            subdivide(
                0,
                x,
                y,
                counts,
                max_zoom,
                max_nodes,
                &mut shards,
                &mut oversized,
            );
        }
    }

    if !oversized.is_empty() {
        eprintln!(
            "Warning: {} tiles at max zoom exceeded the node threshold (showing up to 5):",
            oversized.len()
        );
        for shard in oversized.iter().take(5) {
            eprintln!(
                "  z/x/y {}/{}/{} -> {} nodes (max {})",
                shard.zoom, shard.x, shard.y, shard.node_count, max_nodes
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
    zoom: u8,
    x: u32,
    y: u32,
    counts: &[HashMap<(u32, u32), u64>],
    max_zoom: u8,
    max_nodes: u64,
    shards: &mut Vec<Shard>,
    oversized: &mut Vec<Shard>,
) {
    let res_idx = usize::from(zoom);
    let count = counts
        .get(res_idx)
        .and_then(|map| map.get(&(x, y)).copied())
        .unwrap_or(0);

    if count == 0 {
        return;
    }

    if count <= max_nodes || zoom == max_zoom {
        let shard = Shard {
            zoom,
            x,
            y,
            node_count: count,
        };
        shards.push(shard);
        if count > max_nodes && zoom == max_zoom {
            oversized.push(shard);
        }
        return;
    }

    let child_zoom = zoom + 1;
    let child_idx = usize::from(child_zoom);
    let candidates = [
        (x * 2, y * 2),
        (x * 2 + 1, y * 2),
        (x * 2, y * 2 + 1),
        (x * 2 + 1, y * 2 + 1),
    ];

    for (cx, cy) in candidates {
        let child_count = counts
            .get(child_idx)
            .and_then(|map| map.get(&(cx, cy)).copied())
            .unwrap_or(0);
        if child_count == 0 {
            continue;
        }
        subdivide(
            child_zoom,
            cx,
            cy,
            counts,
            max_zoom,
            max_nodes,
            shards,
            oversized,
        );
    }
}

/// Convert the shard list into a GeoJSON string.
fn generate_geojson(shards: &[Shard]) -> Result<String> {
    let mut features = Vec::with_capacity(shards.len());

    for shard in shards {
        let ring = tile_ring(shard.zoom, shard.x, shard.y);
        let shard_id = format!("{}-{}-{}", shard.zoom, shard.x, shard.y);
        features.push(Feature {
            feature_type: "Feature",
            properties: Properties {
                shard_id,
                z: shard.zoom,
                x: shard.x,
                y: shard.y,
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

fn lon_lat_to_tile(lon: f64, lat: f64, zoom: u8) -> Option<(u32, u32)> {
    if !(lon.is_finite() && lat.is_finite()) {
        return None;
    }

    // Clamp latitude to the Web Mercator limit.
    let lat = lat.clamp(-85.05112878, 85.05112878);
    let n = 2u32.checked_pow(u32::from(zoom))?;

    let x = ((lon + 180.0) / 360.0 * f64::from(n)).floor();
    let lat_rad = lat.to_radians();
    let y = ((1.0 - (lat_rad.tan() + 1.0 / lat_rad.cos()).ln() / PI) / 2.0 * f64::from(n))
        .floor();

    let x = x.clamp(0.0, f64::from(n - 1)) as u32;
    let y = y.clamp(0.0, f64::from(n - 1)) as u32;
    Some((x, y))
}

fn tile_bbox(zoom: u8, x: u32, y: u32) -> (f64, f64, f64, f64) {
    let n = 2u32.pow(u32::from(zoom)) as f64;
    let west = (f64::from(x) / n) * 360.0 - 180.0;
    let east = (f64::from(x + 1) / n) * 360.0 - 180.0;

    let north_rad = (PI * (1.0 - 2.0 * (f64::from(y) / n))).sinh().atan();
    let south_rad = (PI * (1.0 - 2.0 * (f64::from(y + 1) / n))).sinh().atan();
    let north = north_rad.to_degrees();
    let south = south_rad.to_degrees();
    (west, south, east, north)
}

fn tile_ring(zoom: u8, x: u32, y: u32) -> Vec<[f64; 2]> {
    let (west, south, east, north) = tile_bbox(zoom, x, y);
    vec![
        [west, south],
        [east, south],
        [east, north],
        [west, north],
        [west, south],
    ]
}
