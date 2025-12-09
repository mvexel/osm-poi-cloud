import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import MapView from './components/MapView';
import Sidebar from './components/Sidebar';
import { fetchClasses, fetchPois } from './api';
import type { Bbox, PoiFeature } from './types';

const DEFAULT_BBOX: Bbox = [-125, 25, -66, 50];
const API_SET = Boolean((import.meta as any).env?.VITE_API_BASE);

function App() {
    const [bbox, setBbox] = useState<Bbox>(DEFAULT_BBOX);
    const [selectedClass, setSelectedClass] = useState<string | undefined>(undefined);
    const [selectedFeature, setSelectedFeature] = useState<PoiFeature | null>(null);

    const classesQuery = useQuery({
        queryKey: ['classes'],
        queryFn: fetchClasses,
        staleTime: 1000 * 60 * 10,
    });

    const poisQuery = useQuery({
        queryKey: ['pois', bbox, selectedClass],
        queryFn: () => fetchPois(bbox, selectedClass),
        keepPreviousData: true,
    });

    const features = useMemo(() => poisQuery.data?.features ?? [], [poisQuery.data]);

    return (
        <div className="layout">
            <Sidebar
                classes={classesQuery.data ?? []}
                selectedClass={selectedClass}
                onClassChange={(c) => {
                    setSelectedClass(c);
                    setSelectedFeature(null);
                }}
                selected={selectedFeature}
                isLoading={poisQuery.isFetching}
                count={poisQuery.data?.count ?? 0}
            />
            <div className="main">
                <div className="pill">Pan/zoom to load POIs within view</div>
                <MapView
                    features={features}
                    selectedId={selectedFeature?.properties.osm_id}
                    onBoundsChanged={(b) => setBbox(b)}
                    onSelect={(f) => setSelectedFeature(f)}
                />
                {!API_SET && (
                    <div className="error">Set VITE_API_BASE in frontend/.env to call the API.</div>
                )}
                {poisQuery.isError && (
                    <div className="error">{(poisQuery.error as Error).message}</div>
                )}
            </div>
        </div>
    );
}

export default App;
