import type { Bbox, ClassItem, PoiResponse } from './types';

const API_BASE = (import.meta.env.VITE_API_BASE as string | undefined)?.replace(/\/$/, '') ?? '';

if (!API_BASE) {
    // eslint-disable-next-line no-console
    console.warn('VITE_API_BASE is not set; API calls will fail. Set it in frontend/.env');
}

export async function fetchClasses(): Promise<ClassItem[]> {
    const res = await fetch(`${API_BASE}/classes`);
    if (!res.ok) throw new Error(`Failed to load classes (${res.status})`);
    const data = await res.json();
    return data.classes ?? [];
}

export async function fetchPois(bbox: Bbox, poiClass?: string, limit = 1500): Promise<PoiResponse> {
    const params = new URLSearchParams({ bbox: bbox.join(','), limit: String(limit) });
    if (poiClass) params.set('class', poiClass);

    const res = await fetch(`${API_BASE}/pois?${params.toString()}`);
    if (!res.ok) throw new Error(`Failed to load POIs (${res.status})`);
    return res.json();
}
