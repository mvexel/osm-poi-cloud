import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import MapView from './components/MapView';
import Sidebar from './components/Sidebar';
import { fetchClasses, fetchPois } from './api';
import { FALLBACK_CLASSES } from './constants/poiClasses';
import type { Bbox, PoiFeature } from './types';

const DEFAULT_BBOX: Bbox = [-125, 25, -66, 50];
const API_SET = Boolean((import.meta as any).env?.VITE_API_BASE);
const PMTILES_ONLY = Boolean((import.meta as any).env?.VITE_PMTILES_URL);
const API_ENABLED = API_SET && !PMTILES_ONLY;

function App() {
    const [bbox, setBbox] = useState<Bbox>(DEFAULT_BBOX);
    const [selectedClass, setSelectedClass] = useState<string | undefined>(undefined);
    const [selectedFeature, setSelectedFeature] = useState<PoiFeature | null>(null);

    const classesQuery = useQuery({
        queryKey: ['classes'],
        queryFn: fetchClasses,
        staleTime: 1000 * 60 * 10,
        enabled: API_ENABLED,
    });

    const poisQuery = useQuery({
        queryKey: ['pois', bbox, selectedClass],
        queryFn: () => fetchPois(bbox, selectedClass),
        keepPreviousData: true,
        enabled: API_ENABLED,
    });

    const features = useMemo(() => (API_ENABLED ? poisQuery.data?.features ?? [] : []), [poisQuery.data]);
    const availableClasses = API_ENABLED ? classesQuery.data ?? [] : FALLBACK_CLASSES;
    const showApiMissing = !API_ENABLED && !PMTILES_ONLY;

    return (
        <div className="layout">
            <Sidebar
                classes={availableClasses}
                selectedClass={selectedClass}
                onClassChange={(c) => {
                    setSelectedClass(c);
                    setSelectedFeature(null);
                }}
                selected={selectedFeature}
                isLoading={API_ENABLED ? poisQuery.isFetching : false}
                count={API_ENABLED ? poisQuery.data?.count ?? 0 : 0}
                apiEnabled={API_ENABLED}
                pmtilesOnly={PMTILES_ONLY}
            />
            <div className="main">
                <div className="pill">
                    {PMTILES_ONLY ? 'Viewing PMTiles snapshot directly (no live API calls).' : 'Pan/zoom to load POIs within view'}
                </div>
                <MapView
                    features={features}
                    selectedId={selectedFeature?.properties.osm_id}
                    selectedClass={selectedClass}
                    onBoundsChanged={(b) => setBbox(b)}
                    onSelect={(f) => setSelectedFeature(f)}
                />
                {showApiMissing && (
                    <div className="error">Set VITE_API_BASE in frontend/.env to call the API.</div>
                )}
                {API_ENABLED && poisQuery.isError && (
                    <div className="error">{(poisQuery.error as Error).message}</div>
                )}
                {PMTILES_ONLY && (
                    <div className="muted" style={{ marginTop: '0.5rem' }}>
                        Filters and counts rely on the live API and are disabled while streaming PMTiles.
                    </div>
                )}
            </div>
        </div>
    );
}

export default App;
