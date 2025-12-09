export type Bbox = [number, number, number, number];

export interface PoiProperties {
    osm_id: string;
    osm_type: string;
    name?: string;
    class: string;
    state?: string;
    amenity?: string;
    shop?: string;
    leisure?: string;
    tourism?: string;
    cuisine?: string;
    opening_hours?: string;
    phone?: string;
    website?: string;
    brand?: string;
    operator?: string;
    tags?: Record<string, unknown>;
}

export interface PoiFeature {
    type: 'Feature';
    geometry: { type: 'Point'; coordinates: [number, number] };
    properties: PoiProperties;
}

export interface PoiResponse {
    type: 'FeatureCollection';
    features: PoiFeature[];
    count: number;
    bbox: Bbox;
}

export interface ClassItem {
    class: string;
    count: number;
}
