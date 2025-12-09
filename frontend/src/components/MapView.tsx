import { useEffect, useRef, useCallback } from 'react';
import Map, { Layer, Source } from 'react-map-gl/maplibre';
import type { MapRef, StyleSpecification } from 'react-map-gl/maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import type { Bbox, PoiFeature } from '../types';
import mapStyle from '../mapstyles/osm-bright-osmusa.json';

const DEFAULT_STYLE: StyleSpecification =
    (import.meta.env.VITE_MAP_STYLE as StyleSpecification | undefined) || (mapStyle as StyleSpecification);

interface MapViewProps {
    features: PoiFeature[];
    selectedId?: string;
    onBoundsChanged: (bbox: Bbox) => void;
    onSelect: (feature: PoiFeature | null) => void;
}

export function MapView({ features, selectedId, onBoundsChanged, onSelect }: MapViewProps) {
    const mapRef = useRef<MapRef>(null);
    const isMobile = window.matchMedia('(max-width: 768px)').matches;

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
                const selected = features.find((f) => f.properties.osm_id === hit.properties.osm_id);
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
            mapStyle={DEFAULT_STYLE}
            onMoveEnd={handleMoveEnd}
            onClick={handleClick}
        >
            <Source id="pois" type="geojson" data={geojsonData}>
                <Layer
                    id="pois-layer"
                    type="circle"
                    paint={{
                        'circle-radius': ['interpolate', ['linear'], ['zoom'], 3, 3, 12, 7],
                        'circle-color': '#0f766e',
                        'circle-stroke-color': '#0d9488',
                        'circle-stroke-width': 1,
                        'circle-opacity': 0.8,
                    }}
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
        </Map>
    );
}

export default MapView;
