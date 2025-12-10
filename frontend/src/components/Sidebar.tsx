import { getClassLabel } from '../constants/poiClasses';
import type { ClassItem, PoiFeature } from '../types';

interface SidebarProps {
    classes: ClassItem[];
    selectedClass?: string;
    onClassChange: (value?: string) => void;
    selected?: PoiFeature | null;
    isLoading: boolean;
    count: number;
    apiEnabled: boolean;
    pmtilesOnly: boolean;
}

export function Sidebar({
    classes,
    selectedClass,
    onClassChange,
    selected,
    isLoading,
    count,
    apiEnabled,
    pmtilesOnly,
}: SidebarProps) {
    return (
        <div className="sidebar">
            <div className="panel">
                <div className="panel-header">
                    <div>
                        <h1>OSM POI Explorer</h1>
                        <p className="muted">
                            {pmtilesOnly
                                ? 'Browsing PMTiles snapshot. Filtering happens client-side.'
                                : 'Browse POIs from the AWS-backed API. Pan/zoom to load.'}
                        </p>
                    </div>
                    <div className="badge">
                        {apiEnabled ? (isLoading ? 'Loading…' : `${count} shown`) : pmtilesOnly ? 'PMTiles' : 'Offline'}
                    </div>
                </div>

                <label className="label" htmlFor="class-filter">Class filter</label>
                <select
                    id="class-filter"
                    className="select"
                    value={selectedClass ?? ''}
                    onChange={(e) => onClassChange(e.target.value || undefined)}
                >
                    <option value="">All classes</option>
                    {classes.map((c) => (
                        <option key={c.class} value={c.class}>
                            {getClassLabel(c.class)}
                            {apiEnabled ? ` (${c.count})` : ''}
                        </option>
                    ))}
                </select>
            </div>

            <div className="panel">
                <h2>Details</h2>
                {selected ? (
                    <div className="details">
                        <div className="detail-row">
                            <span className="detail-label">Name</span>
                            <span>{selected.properties.name || '—'}</span>
                        </div>
                        <div className="detail-row">
                            <span className="detail-label">Class</span>
                            <span>{getClassLabel(selected.properties.class)}</span>
                        </div>
                        <div className="detail-row">
                            <span className="detail-label">State</span>
                            <span>{selected.properties.state || '—'}</span>
                        </div>
                        {selected.properties.cuisine && (
                            <div className="detail-row">
                                <span className="detail-label">Cuisine</span>
                                <span>{selected.properties.cuisine}</span>
                            </div>
                        )}
                        {selected.properties.phone && (
                            <div className="detail-row">
                                <span className="detail-label">Phone</span>
                                <span>{selected.properties.phone}</span>
                            </div>
                        )}
                        {selected.properties.website && (
                            <div className="detail-row">
                                <span className="detail-label">Website</span>
                                <a className="link" href={selected.properties.website} target="_blank" rel="noreferrer">Open</a>
                            </div>
                        )}
                        {selected.properties.operator && (
                            <div className="detail-row">
                                <span className="detail-label">Operator</span>
                                <span>{selected.properties.operator}</span>
                            </div>
                        )}
                        {selected.properties.brand && (
                            <div className="detail-row">
                                <span className="detail-label">Brand</span>
                                <span>{selected.properties.brand}</span>
                            </div>
                        )}
                        <div className="detail-row">
                            <span className="detail-label">Coords</span>
                            <span>{selected.geometry.coordinates[1].toFixed(5)}, {selected.geometry.coordinates[0].toFixed(5)}</span>
                        </div>
                    </div>
                ) : (
                    <p className="muted">Click a point to see details.</p>
                )}
            </div>
        </div>
    );
}

export default Sidebar;
