import { useRef, useCallback, useEffect, useState } from 'react';
import Map, { Layer, Source } from 'react-map-gl/maplibre';
import type { MapRef } from 'react-map-gl/maplibre';
import type { StyleSpecification } from 'maplibre-gl';
import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import { PMTiles, Protocol } from 'pmtiles';
import type { Bbox, PoiFeature } from '../types';
import mapStyle from '../mapstyles/osm-bright-osmusa.json';
import { CLASS_COLOR_EXPRESSION } from '../constants/poiClasses';

const DEFAULT_STYLE: StyleSpecification =
    (import.meta.env.VITE_MAP_STYLE as StyleSpecification | undefined) || (mapStyle as StyleSpecification);

const PMTILES_URL = import.meta.env.VITE_PMTILES_URL as string | undefined;

let pmtilesProtocol: Protocol | null = null;
const ensurePmtilesProtocol = () => {
    if (!pmtilesProtocol) {
        pmtilesProtocol = new Protocol();
        try {
            maplibregl.addProtocol('pmtiles', pmtilesProtocol.tile);
        } catch (err) {
            // Protocol already registered by a previous hot-reload; ignore.
            console.debug('pmtiles protocol already registered', err);
        }
    }
    return pmtilesProtocol!;
};

interface MapViewProps {
    features: PoiFeature[];
    selectedId?: string;
    selectedClass?: string;
    onBoundsChanged: (bbox: Bbox) => void;
    onSelect: (feature: PoiFeature | null) => void;
}

export function MapView({ features, selectedId, selectedClass, onBoundsChanged, onSelect }: MapViewProps) {
    const mapRef = useRef<MapRef>(null);
    const [mapReady, setMapReady] = useState(false);
    const missingImagesRef = useRef<Set<string>>(new Set());
    const isMobile = window.matchMedia('(max-width: 768px)').matches;
    const pmtilesEnabled = Boolean(PMTILES_URL);
    const pmtilesSourceUrl = pmtilesEnabled ? `pmtiles://${PMTILES_URL}` : undefined;
    const classFilter = selectedClass ? (['==', ['get', 'class'], selectedClass] as any) : (['all'] as any);

    useEffect(() => {
        if (!PMTILES_URL) return;
        const protocol = ensurePmtilesProtocol();
        const pmtiles = new PMTiles(PMTILES_URL);
        protocol.add(pmtiles);
    }, []);

    // Provide transparent fallbacks for missing sprite images so MapLibre doesn't spam errors
    useEffect(() => {
        if (!mapReady) return;
        const map = mapRef.current?.getMap();
        if (!map) return;

        const handleMissing = (event: { id: string }) => {
            const imageId = event.id;
            if (missingImagesRef.current.has(imageId)) return;
            missingImagesRef.current.add(imageId);
            const size = 1;
            const transparent = new Uint8Array(size * size * 4); // defaults to zeros
            map.addImage(
                imageId,
                {
                    width: size,
                    height: size,
                    data: transparent,
                },
                { pixelRatio: 1 }
            );
        };

        map.on('styleimagemissing', handleMissing);
        return () => {
            map.off('styleimagemissing', handleMissing);
        };
    }, [mapReady]);

    const handleMoveEnd = useCallback(() => {
        const map = mapRef.current?.getMap();
        if (!map) return;
        const b = map.getBounds();
        onBoundsChanged([b.getWest(), b.getSouth(), b.getEast(), b.getNorth()]);
    }, [onBoundsChanged]);

    const handleClick = useCallback(
        (event: any) => {
            const map = mapRef.current?.getMap();
            if (!map) return;
            const hits = map.queryRenderedFeatures(event.point, { layers: ['pois-layer'] });
            const hit = hits[0];
            if (hit?.properties) {
                let selected = features.find((f) => f.properties.osm_id === hit.properties.osm_id);
                if (!selected && hit.geometry?.type === 'Point' && Array.isArray(hit.geometry.coordinates)) {
                    selected = {
                        type: 'Feature',
                        geometry: {
                            type: 'Point',
                            coordinates: hit.geometry.coordinates as [number, number],
                        },
                        properties: hit.properties as PoiFeature['properties'],
                    };
                }
                if (selected) {
                    onSelect(selected);
                    return;
                }
            }
            onSelect(null);
        },
        [features, onSelect]
    );

    const geojsonData = {
        type: 'FeatureCollection' as const,
        features,
    };

    return (
        <Map
            ref={mapRef}
            initialViewState={{
                longitude: isMobile ? -86.9023 : -98.5795,
                latitude: isMobile ? 32.3182 : 39.8283,
                zoom: isMobile ? 6 : 3.5,
            }}
            style={{ width: '100%', height: '100%' }}
            mapLib={maplibregl}
            mapStyle={DEFAULT_STYLE}
            onMoveEnd={handleMoveEnd}
            onClick={handleClick}
            onLoad={() => setMapReady(true)}
        >
            {pmtilesEnabled ? (
                <Source id="pois-tiles" type="vector" url={pmtilesSourceUrl}>
                    <Layer
                        id="pois-layer"
                        source="pois-tiles"
                        source-layer="pois"
                        type="circle"
                        paint={{
                            'circle-radius': ['interpolate', ['linear'], ['zoom'], 3, 3, 12, 7],
                            'circle-color': CLASS_COLOR_EXPRESSION,
                            'circle-stroke-color': '#0f172a',
                            'circle-stroke-width': 0.5,
                            'circle-opacity': 0.8,
                        }}
                        filter={classFilter}
                    />
                    <Layer
                        id="pois-selected"
                        source="pois-tiles"
                        source-layer="pois"
                        type="circle"
                        paint={{
                            'circle-radius': 9,
                            'circle-color': '#eab308',
                            'circle-stroke-color': '#854d0e',
                            'circle-stroke-width': 2,
                            'circle-opacity': 0.9,
                        }}
                        filter={['==', ['get', 'osm_id'], selectedId ?? 'none']}
                    />
                </Source>
            ) : (
                <Source id="pois" type="geojson" data={geojsonData}>
                    <Layer
                        id="pois-layer"
                        type="circle"
                        paint={{
                            'circle-radius': ['interpolate', ['linear'], ['zoom'], 3, 3, 12, 7],
                            'circle-color': CLASS_COLOR_EXPRESSION,
                            'circle-stroke-color': '#0f172a',
                            'circle-stroke-width': 0.5,
                            'circle-opacity': 0.85,
                        }}
                        filter={classFilter}
                    />
                    <Layer
                        id="pois-selected"
                        type="circle"
                        paint={{
                            'circle-radius': 9,
                            'circle-color': '#eab308',
                            'circle-stroke-color': '#854d0e',
                            'circle-stroke-width': 2,
                            'circle-opacity': 0.9,
                        }}
                        filter={['==', ['get', 'osm_id'], selectedId ?? 'none']}
                    />
                </Source>
            )}
        </Map>
    );
}

export default MapView;
