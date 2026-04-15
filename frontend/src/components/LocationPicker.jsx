import { useState, useEffect } from 'react';

const API_BASE = '/api';

/**
 * LocationPicker — simplified dropdown for delivery address.
 * Shows Samarkand districts + "Boshqa" option. No mo'ljal level.
 */
export default function LocationPicker({ telegramId, onLocationChange, initialDistrictId }) {
  const [tree, setTree] = useState(null);
  const [loading, setLoading] = useState(true);
  const [selectedDistrict, setSelectedDistrict] = useState(initialDistrictId || null);
  const [isOther, setIsOther] = useState(false);
  const [saving, setSaving] = useState(false);

  // Load location tree on mount
  useEffect(() => {
    fetch(`${API_BASE}/locations/tree`)
      .then(r => r.json())
      .then(data => {
        setTree(data.viloyats || []);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, []);

  // Load client's saved location on mount
  useEffect(() => {
    if (!telegramId || initialDistrictId) return;
    fetch(`${API_BASE}/client-location?telegram_id=${telegramId}`)
      .then(r => r.json())
      .then(data => {
        if (data.manual?.district_id) {
          setSelectedDistrict(data.manual.district_id);
          onLocationChange?.({
            district_id: data.manual.district_id,
            moljal_id: null,
            district_name: data.manual.district_name,
            moljal_name: null,
          });
        }
      })
      .catch(() => {});
  }, [telegramId]);

  // Find Samarkand viloyat and its districts
  const samarkandViloyat = tree?.find(v => v.name?.toLowerCase().includes('samarqand'));
  const districts = samarkandViloyat?.districts || [];

  const handleChange = (val) => {
    if (val === 'other') {
      setIsOther(true);
      setSelectedDistrict(null);
      onLocationChange?.({ district_id: null, moljal_id: null, district_name: 'Boshqa hudud', moljal_name: null });
      return;
    }
    setIsOther(false);
    const id = val ? parseInt(val) : null;
    setSelectedDistrict(id);
    const dObj = districts.find(d => d.id === id);
    onLocationChange?.({
      district_id: id,
      moljal_id: null,
      district_name: dObj?.name || null,
      moljal_name: null,
    });
  };

  const handleSave = async () => {
    if (!telegramId || !selectedDistrict) return;
    setSaving(true);
    try {
      await fetch(`${API_BASE}/client-location`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ telegram_id: telegramId, district_id: selectedDistrict }),
      });
    } catch {}
    setSaving(false);
  };

  if (loading) {
    return <div className="text-center text-xs text-tg-hint py-2">Yuklanmoqda...</div>;
  }

  const selectStyle = {
    backgroundColor: 'var(--tg-theme-secondary-bg-color)',
    color: 'var(--tg-theme-text-color)',
  };

  return (
    <div className="mb-3">
      <div className="text-xs text-tg-hint font-medium mb-1.5">📍 Tuman/shaharni tanlang</div>
      <select
        value={isOther ? 'other' : (selectedDistrict || '')}
        onChange={(e) => handleChange(e.target.value)}
        className="w-full rounded-lg px-3 py-2.5 text-sm border-0 outline-none appearance-none"
        style={selectStyle}
      >
        <option value="">Tanlang...</option>
        {districts.map(d => (
          <option key={d.id} value={d.id}>{d.name}</option>
        ))}
        <option value="other">Boshqa hudud</option>
      </select>

      {(selectedDistrict || isOther) && (
        <div className="flex items-center justify-between mt-1.5">
          <div className="text-xs text-green-500 font-medium">
            {isOther ? 'Boshqa hudud' : districts.find(d => d.id === selectedDistrict)?.name}
          </div>
          {telegramId && selectedDistrict && (
            <button
              onClick={handleSave}
              disabled={saving}
              className="text-[10px] px-3 py-1 rounded-full bg-tg-button text-tg-button-text disabled:opacity-50"
            >
              {saving ? '...' : 'Saqlash'}
            </button>
          )}
        </div>
      )}
    </div>
  );
}
