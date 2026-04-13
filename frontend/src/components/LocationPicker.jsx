import { useState, useEffect } from 'react';
import t from '../i18n/uz.json';

const API_BASE = '/api';

/**
 * LocationPicker — cascading dropdown for delivery address.
 * Viloyat → District → Mo'ljal
 *
 * Props:
 *   telegramId: number — to load/save the client's saved location
 *   onLocationChange: ({ district_id, moljal_id, district_name, moljal_name }) => void
 *   initialDistrictId: number | null
 *   initialMoljalId: number | null
 */
export default function LocationPicker({ telegramId, onLocationChange, initialDistrictId, initialMoljalId }) {
  const [tree, setTree] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Selection state
  const [selectedViloyat, setSelectedViloyat] = useState(null);
  const [selectedDistrict, setSelectedDistrict] = useState(initialDistrictId || null);
  const [selectedMoljal, setSelectedMoljal] = useState(initialMoljalId || null);
  const [saving, setSaving] = useState(false);

  // Load location tree on mount
  useEffect(() => {
    fetch(`${API_BASE}/locations/tree`)
      .then(r => r.json())
      .then(data => {
        setTree(data.viloyats || []);
        setLoading(false);

        // If initial values provided, set the viloyat from the district's parent
        if (initialDistrictId && data.viloyats) {
          for (const v of data.viloyats) {
            const d = v.districts?.find(d => d.id === initialDistrictId);
            if (d) {
              setSelectedViloyat(v.id);
              break;
            }
          }
        }
      })
      .catch(err => {
        console.error('Failed to load locations:', err);
        setError('Joylashuvlar yuklanmadi');
        setLoading(false);
      });
  }, [initialDistrictId]);

  // Load client's saved location on mount
  useEffect(() => {
    if (!telegramId || initialDistrictId) return;

    fetch(`${API_BASE}/client-location?telegram_id=${telegramId}`)
      .then(r => r.json())
      .then(data => {
        if (data.has_location) {
          setSelectedDistrict(data.district_id);
          setSelectedMoljal(data.moljal_id);
          if (data.viloyat_id) setSelectedViloyat(data.viloyat_id);
          onLocationChange?.({
            district_id: data.district_id,
            moljal_id: data.moljal_id,
            district_name: data.district_name,
            moljal_name: data.moljal_name,
          });
        }
      })
      .catch(() => {});
  }, [telegramId]);

  // Derived: districts and moljals from tree
  const viloyats = tree || [];
  const selectedViloyatObj = viloyats.find(v => v.id === selectedViloyat);
  const districts = selectedViloyatObj?.districts || [];
  const selectedDistrictObj = districts.find(d => d.id === selectedDistrict);
  const moljals = selectedDistrictObj?.moljals || [];

  const handleViloyatChange = (vid) => {
    const id = vid ? parseInt(vid) : null;
    setSelectedViloyat(id);
    setSelectedDistrict(null);
    setSelectedMoljal(null);
    onLocationChange?.({ district_id: null, moljal_id: null });
  };

  const handleDistrictChange = (did) => {
    const id = did ? parseInt(did) : null;
    setSelectedDistrict(id);
    setSelectedMoljal(null);
    const dObj = districts.find(d => d.id === id);
    onLocationChange?.({
      district_id: id,
      moljal_id: null,
      district_name: dObj?.name || null,
      moljal_name: null,
    });
  };

  const handleMoljalChange = (mid) => {
    const id = mid ? parseInt(mid) : null;
    setSelectedMoljal(id);
    const mObj = moljals.find(m => m.id === id);
    const dObj = districts.find(d => d.id === selectedDistrict);
    onLocationChange?.({
      district_id: selectedDistrict,
      moljal_id: id,
      district_name: dObj?.name || null,
      moljal_name: mObj?.name || null,
    });
  };

  const handleSaveLocation = async () => {
    if (!telegramId || !selectedDistrict) return;
    setSaving(true);
    try {
      await fetch(`${API_BASE}/client-location`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          telegram_id: telegramId,
          district_id: selectedDistrict,
          moljal_id: selectedMoljal,
        }),
      });
    } catch (err) {
      console.error('Failed to save location:', err);
    }
    setSaving(false);
  };

  if (loading) {
    return (
      <div className="text-center text-xs text-tg-hint py-2">
        Joylashuvlar yuklanmoqda...
      </div>
    );
  }

  if (error) {
    return (
      <div className="text-center text-xs text-red-400 py-2">{error}</div>
    );
  }

  const selectClass = "w-full rounded-lg px-3 py-2.5 text-sm border-0 outline-none appearance-none";
  const selectStyle = {
    backgroundColor: 'var(--tg-theme-secondary-bg-color)',
    color: 'var(--tg-theme-text-color)',
  };

  return (
    <div className="space-y-2 mb-3">
      <div className="text-xs text-tg-hint font-medium mb-1">
        📍 Yetkazish manzili
      </div>

      {/* Viloyat — pre-select Samarkand if only one is relevant */}
      {viloyats.length > 1 && (
        <select
          value={selectedViloyat || ''}
          onChange={(e) => handleViloyatChange(e.target.value)}
          className={selectClass}
          style={selectStyle}
        >
          <option value="">Viloyatni tanlang</option>
          {viloyats.map(v => (
            <option key={v.id} value={v.id}>{v.name}</option>
          ))}
        </select>
      )}

      {/* District */}
      {(selectedViloyat || viloyats.length === 1) && (
        <select
          value={selectedDistrict || ''}
          onChange={(e) => handleDistrictChange(e.target.value)}
          className={selectClass}
          style={selectStyle}
        >
          <option value="">Tuman/shaharni tanlang</option>
          {(viloyats.length === 1 ? viloyats[0].districts : districts).map(d => (
            <option key={d.id} value={d.id}>{d.name}</option>
          ))}
        </select>
      )}

      {/* Mo'ljal */}
      {selectedDistrict && moljals.length > 0 && (
        <select
          value={selectedMoljal || ''}
          onChange={(e) => handleMoljalChange(e.target.value)}
          className={selectClass}
          style={selectStyle}
        >
          <option value="">Mo'ljalni tanlang (ixtiyoriy)</option>
          {moljals.map(m => (
            <option key={m.id} value={m.id}>{m.name}</option>
          ))}
        </select>
      )}

      {/* Summary + save */}
      {selectedDistrict && (
        <div className="flex items-center justify-between">
          <div className="text-xs text-green-500 font-medium">
            {selectedDistrictObj?.name}
            {selectedMoljal && moljals.find(m => m.id === selectedMoljal)
              ? ` → ${moljals.find(m => m.id === selectedMoljal).name}`
              : ''}
          </div>
          {telegramId && (
            <button
              onClick={handleSaveLocation}
              disabled={saving}
              className="text-xs px-3 py-1 rounded-full bg-tg-button text-tg-button-text disabled:opacity-50"
            >
              {saving ? '...' : 'Saqlash'}
            </button>
          )}
        </div>
      )}
    </div>
  );
}
