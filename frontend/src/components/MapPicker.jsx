import { useState, useEffect, useRef } from 'react';
import L from 'leaflet';
import 'leaflet/dist/leaflet.css';

// Fix Leaflet default marker icons (vite issue)
delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png',
  iconUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png',
  shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
});

const DEFAULT_CENTER = [39.6550, 66.9597]; // Samarkand
const DEFAULT_ZOOM = 11;

// Yandex Maps tile layer
function yandexTileLayer() {
  return L.tileLayer(
    'https://core-renderer-tiles.maps.yandex.net/tiles?l=map&x={x}&y={y}&z={z}&scale=1&lang=ru_RU',
    { attribution: '© Yandex', maxZoom: 19 }
  );
}

// Reverse geocode using Nominatim
async function reverseGeocode(lat, lng) {
  try {
    const resp = await fetch(
      `https://nominatim.openstreetmap.org/reverse?lat=${lat}&lon=${lng}&format=json&accept-language=uz,ru&zoom=18`
    );
    const data = await resp.json();
    const addr = data.address || {};
    const parts = [];
    const city = addr.city || addr.town || addr.village || '';
    if (city) parts.push(city);
    const road = addr.road || '';
    const neighbourhood = addr.neighbourhood || addr.suburb || '';
    if (road) {
      let street = road;
      if (addr.house_number) street += ' ' + addr.house_number;
      parts.push(street);
    } else if (neighbourhood) {
      parts.push(neighbourhood);
    }
    return parts.join(', ') || data.display_name?.slice(0, 80) || '';
  } catch {
    return '';
  }
}

// Search using 2GIS (excellent coverage for Uzbekistan)
async function searchAddress(query) {
  try {
    // 2GIS catalog search — free, no API key needed for basic suggest
    const resp = await fetch(
      `https://catalog.api.2gis.com/3.0/suggests?q=${encodeURIComponent(query)}&locale=ru_UZ&region_id=116&fields=items.point&key=demo`
    );
    const data = await resp.json();
    if (data.result?.items) {
      return data.result.items
        .filter(item => item.point)
        .slice(0, 5)
        .map(item => ({
          display_name: item.full_name || item.name || '',
          lat: item.point.lat,
          lon: item.point.lon,
        }));
    }
  } catch {}

  // Fallback to Nominatim
  try {
    const resp = await fetch(
      `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(query + ', Samarkand')}&format=json&accept-language=uz,ru&limit=5&countrycodes=uz`
    );
    return await resp.json();
  } catch {
    return [];
  }
}

export default function MapPicker({ initialLat, initialLng, onConfirm, onClose }) {
  const mapRef = useRef(null);
  const mapInstanceRef = useRef(null);
  const markerRef = useRef(null);

  const [address, setAddress] = useState('');
  const [loading, setLoading] = useState(false);
  const [coords, setCoords] = useState({
    lat: initialLat || DEFAULT_CENTER[0],
    lng: initialLng || DEFAULT_CENTER[1],
  });

  // Search state
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [searching, setSearching] = useState(false);
  const [searchFocused, setSearchFocused] = useState(false);
  const searchTimerRef = useRef(null);

  const moveMarker = async (lat, lng, zoom) => {
    setCoords({ lat, lng });
    if (mapInstanceRef.current && markerRef.current) {
      mapInstanceRef.current.setView([lat, lng], zoom || mapInstanceRef.current.getZoom());
      markerRef.current.setLatLng([lat, lng]);
    }
    const addr = await reverseGeocode(lat, lng);
    setAddress(addr);
  };

  // Initialize map
  useEffect(() => {
    if (mapInstanceRef.current) return;

    const map = L.map(mapRef.current, {
      center: [coords.lat, coords.lng],
      zoom: initialLat ? 15 : DEFAULT_ZOOM,
      zoomControl: false,
    });

    yandexTileLayer().addTo(map);
    L.control.zoom({ position: 'topright' }).addTo(map);

    const marker = L.marker([coords.lat, coords.lng], { draggable: true }).addTo(map);

    marker.on('dragend', async () => {
      const pos = marker.getLatLng();
      setCoords({ lat: pos.lat, lng: pos.lng });
      const addr = await reverseGeocode(pos.lat, pos.lng);
      setAddress(addr);
    });

    map.on('click', async (e) => {
      marker.setLatLng(e.latlng);
      setCoords({ lat: e.latlng.lat, lng: e.latlng.lng });
      const addr = await reverseGeocode(e.latlng.lat, e.latlng.lng);
      setAddress(addr);
    });

    mapInstanceRef.current = map;
    markerRef.current = marker;

    reverseGeocode(coords.lat, coords.lng).then(setAddress);
    setTimeout(() => map.invalidateSize(), 100);

    return () => {
      map.remove();
      mapInstanceRef.current = null;
    };
  }, []);

  // Debounced search
  const handleSearchInput = (val) => {
    setSearchQuery(val);
    if (searchTimerRef.current) clearTimeout(searchTimerRef.current);
    if (val.trim().length < 2) {
      setSearchResults([]);
      return;
    }
    searchTimerRef.current = setTimeout(async () => {
      setSearching(true);
      const results = await searchAddress(val.trim());
      setSearchResults(results);
      setSearching(false);
    }, 400);
  };

  const handleSearchSelect = (result) => {
    const lat = parseFloat(result.lat);
    const lng = parseFloat(result.lon);
    moveMarker(lat, lng, 17);
    setSearchQuery('');
    setSearchResults([]);
    setSearchFocused(false);
  };

  const handleConfirm = () => {
    setLoading(true);
    onConfirm(coords.lat, coords.lng, address);
  };

  return (
    <div className="fixed inset-0 z-[200] bg-tg-bg flex flex-col" style={{ paddingTop: 'var(--tg-content-safe-area-inset-top, 0px)' }}>
      {/* Header with search */}
      <div className="bg-tg-secondary px-3 pt-2 pb-1" style={{ paddingTop: 'calc(var(--tg-safe-area-inset-top, 0px) + 8px)' }}>
        <div className="flex items-center gap-2 mb-1.5">
          <button onClick={onClose} className="text-sm text-tg-link whitespace-nowrap">← Orqaga</button>
          <div className="flex-1 relative">
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => handleSearchInput(e.target.value)}
              onFocus={() => setSearchFocused(true)}
              placeholder="🔍 Manzilni qidirish..."
              className="w-full text-sm px-3 py-1.5 rounded-lg bg-tg-bg border-none outline-none"
              style={{ color: 'var(--tg-theme-text-color, #000)' }}
            />
          </div>
          {searchQuery && (
            <button
              onClick={() => { setSearchQuery(''); setSearchResults([]); }}
              className="text-sm text-tg-hint"
            >
              ✕
            </button>
          )}
        </div>
        {/* Search results dropdown */}
        {searchResults.length > 0 && (
          <div className="bg-tg-bg rounded-lg mb-1 max-h-40 overflow-y-auto shadow-sm">
            {searchResults.map((r, i) => (
              <button
                key={i}
                onClick={() => handleSearchSelect(r)}
                className="w-full text-left px-3 py-2 text-xs border-b border-tg-hint/10 active:bg-tg-hint/10"
                style={{ color: 'var(--tg-theme-text-color, #000)' }}
              >
                {(r.display_name || '').slice(0, 80)}
              </button>
            ))}
          </div>
        )}
        {searching && <div className="text-[10px] text-tg-hint text-center py-1">Qidirilmoqda...</div>}
      </div>

      {/* Map */}
      <div className="flex-1 relative min-h-0">
        <div ref={mapRef} className="w-full h-full" />
      </div>

      {/* Compact bottom panel */}
      <div className="bg-tg-secondary px-4 py-2 flex items-center gap-3" style={{ paddingBottom: 'calc(var(--tg-safe-area-inset-bottom, 0px) + 8px)' }}>
        <div className="flex-1 min-w-0">
          <div className="text-[10px] text-tg-hint">Manzil:</div>
          <div className="text-xs font-medium truncate">{address || 'Xaritada tanlang...'}</div>
        </div>
        <button
          onClick={handleConfirm}
          disabled={loading || !address}
          className="bg-green-600 text-white rounded-xl px-5 py-2.5 font-semibold text-sm active:scale-95 transition-transform disabled:opacity-50 whitespace-nowrap"
        >
          {loading ? '...' : '✅ Saqlash'}
        </button>
      </div>
    </div>
  );
}
