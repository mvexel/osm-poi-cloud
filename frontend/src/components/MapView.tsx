import { useEffect, useRef } from 'react';
import maplibregl, { Map as MaplibreMap } from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';
import type { Bbox, PoiFeature } from '../types';

const DEFAULT_STYLE = import.meta.env.VITE_MAP_STYLE ?? 'https://demotiles.maplibre.org/style.json';

interface MapViewProps {
    features: PoiFeature[];
    selectedId?: string;
    onBoundsChanged: (bbox: Bbox) => void;
    onSelect: (feature: PoiFeature | null) => void;
}

export function MapView({ features, selectedId, onBoundsChanged, onSelect }: MapViewProps) {
    const mapRef = useRef<MaplibreMap | null>(null);
    const containerRef = useRef<HTMLDivElement | null>(null);

    useEffect(() => {
        if (mapRef.current || !containerRef.current) return;

        const map = new maplibregl.Map({
            container: containerRef.current,
            style: DEFAULT_STYLE,
            center: [-98.5795, 39.8283],
            zoom: 3.5,
        });

        map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'top-right');

        map.on('load', () => {
            map.addSource('pois', {
                type: 'geojson',
                data: {
                    type: 'FeatureCollection',
                    features: [],
                },
            });

            map.addLayer({
                id: 'pois-layer',
                type: 'circle',
                source: 'pois',
                paint: {
                    'circle-radius': [
                        'interpolate',
                        ['linear'],
                        ['zoom'],
                        3, 3,
                        12, 7
                    ],
                    'circle-color': '#0f766e',
                    'circle-stroke-color': '#0d9488',
                    'circle-stroke-width': 1,
                    'circle-opacity': 0.8,
                },
            });

            map.addLayer({
                id: 'pois-selected',
                type: 'circle',
                source: 'pois',
                paint: {
                    'circle-radius': 9,
                    'circle-color': '#eab308',
                    'circle-stroke-color': '#854d0e',
                    'circle-stroke-width': 2,
                    'circle-opacity': 0.9,
                },
                filter: ['==', ['get', 'id'], 'none'],
            });

            const updateBounds = () => {
                const b = map.getBounds();
                onBoundsChanged([b.getWest(), b.getSouth(), b.getEast(), b.getNorth()]);
            };

            map.on('moveend', updateBounds);
            updateBounds();
        });

        map.on('click', (e) => {
            const hits = map.queryRenderedFeatures(e.point, { layers: ['pois-layer'] });
            const hit = hits[0];
            if (hit && hit.properties) {
                const selected = features.find((f) => f.properties.osm_id === hit.properties.osm_id);
                if (selected) onSelect(selected);
            } else {
                onSelect(null);
            }
        });

        mapRef.current = map;

        return () => {
            map.remove();
            mapRef.current = null;
        };
    }, [onBoundsChanged, onSelect]);

    useEffect(() => {
        const map = mapRef.current;
        if (!map) return;
        const source = map.getSource('pois') as maplibregl.GeoJSONSource | undefined;
        if (source) {
            source.setData({ type: 'FeatureCollection', features });
        }
        map.setFilter('pois-selected', ['==', ['get', 'osm_id'], selectedId ?? 'none']);
    }, [features, selectedId]);

    return <div ref={containerRef} className="map" />;
}

export default MapView;
