import type { ClassItem } from '../types';

type PoiClassMeta = {
    id: string;
    label: string;
    color: string;
};

export const POI_CLASS_META: PoiClassMeta[] = [
    { id: 'restaurant', label: 'Restaurants', color: '#ef4444' },
    { id: 'cafe_bakery', label: 'Cafés & Bakeries', color: '#f97316' },
    { id: 'bar_pub', label: 'Bars & Pubs', color: '#db2777' },
    { id: 'fast_food', label: 'Fast Food', color: '#f59e0b' },
    { id: 'ice_cream', label: 'Ice Cream & Dessert', color: '#fb7185' },
    { id: 'grocery', label: 'Groceries & Convenience', color: '#84cc16' },
    { id: 'specialty_food', label: 'Specialty Food', color: '#65a30d' },
    { id: 'retail', label: 'Retail', color: '#6366f1' },
    { id: 'personal_services', label: 'Personal Services', color: '#a855f7' },
    { id: 'professional_services', label: 'Professional Services', color: '#8b5cf6' },
    { id: 'finance', label: 'Finance & ATMs', color: '#0ea5e9' },
    { id: 'lodging', label: 'Lodging', color: '#14b8a6' },
    { id: 'transport', label: 'Transit', color: '#0f766e' },
    { id: 'auto_services', label: 'Auto Services', color: '#f97316' },
    { id: 'parking', label: 'Parking', color: '#94a3b8' },
    { id: 'healthcare', label: 'Healthcare', color: '#ec4899' },
    { id: 'education', label: 'Education', color: '#22c55e' },
    { id: 'government', label: 'Government', color: '#3b82f6' },
    { id: 'community', label: 'Community', color: '#06b6d4' },
    { id: 'religious', label: 'Religious', color: '#e879f9' },
    { id: 'culture', label: 'Culture & Arts', color: '#c084fc' },
    { id: 'entertainment', label: 'Entertainment', color: '#f87171' },
    { id: 'sports_fitness', label: 'Sports & Fitness', color: '#10b981' },
    { id: 'parks_outdoors', label: 'Parks & Outdoors', color: '#4ade80' },
    { id: 'landmark', label: 'Landmarks', color: '#38bdf8' },
    { id: 'animal_services', label: 'Animal Services', color: '#cbd5f5' },
    { id: 'misc', label: 'Other POIs', color: '#9ca3af' },
];

const CLASS_LABEL_MAP: Record<string, string> = POI_CLASS_META.reduce(
    (acc, meta) => {
        acc[meta.id] = meta.label;
        return acc;
    },
    {} as Record<string, string>
);

const CLASS_COLOR_STOPS = POI_CLASS_META.flatMap((meta) => [meta.id, meta.color]);

export const CLASS_COLOR_EXPRESSION: any = [
    'match',
    ['get', 'class'],
    ...CLASS_COLOR_STOPS,
    '#0f766e',
];

export const FALLBACK_CLASSES: ClassItem[] = POI_CLASS_META.map((meta) => ({
    class: meta.id,
    count: 0,
}));

export const getClassLabel = (id?: string | null) => {
    if (!id) return 'Unknown';
    return CLASS_LABEL_MAP[id] ?? id;
};
