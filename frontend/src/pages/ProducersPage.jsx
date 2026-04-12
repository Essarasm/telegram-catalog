import { useState, useEffect, useRef } from 'react';
import { fetchProducers } from '../utils/api';
import t from '../i18n/uz.json';

// Module-level cache: keeps producers data alive across remounts
// so returning to the same category doesn't show a loading spinner
// (which would break scroll position restoration).
const _cache = { categoryId: null, data: [] };

export default function ProducersPage({ category, onSelectProducer }) {
  // Initialize from cache if we're returning to the same category
  const cached = category?.id === _cache.categoryId;
  const [producers, setProducers] = useState(cached ? _cache.data : []);
  const [loading, setLoading] = useState(!cached);
  const [error, setError] = useState(null);
  const categoryIdRef = useRef(category?.id);

  useEffect(() => {
    if (!category?.id) return;

    // If we already have cached data for this category, skip the fetch
    if (category.id === _cache.categoryId && _cache.data.length > 0) {
      setProducers(_cache.data);
      setLoading(false);
      return;
    }

    categoryIdRef.current = category.id;
    setLoading(true);
    setError(null);
    fetchProducers(category.id)
      .then(data => {
        // Guard against stale responses if category changed during fetch
        if (categoryIdRef.current !== category.id) return;
        if (Array.isArray(data)) {
          setProducers(data);
          // Update cache
          _cache.categoryId = category.id;
          _cache.data = data;
        } else {
          setError('API returned unexpected format: ' + JSON.stringify(data).slice(0, 100));
          setProducers([]);
        }
        setLoading(false);
      })
      .catch(err => {
        if (categoryIdRef.current !== category.id) return;
        setError('Fetch error: ' + (err.message || String(err)));
        setLoading(false);
      });
  }, [category?.id]);

  if (error) {
    return (
      <div className="text-center py-10">
        <div className="text-red-500 text-sm font-mono mb-2">ProducersPage Error:</div>
        <div className="text-red-400 text-xs font-mono">{error}</div>
      </div>
    );
  }

  if (loading) {
    return <div className="text-center py-10 text-tg-hint text-base">{t.loading}</div>;
  }

  if (producers.length === 0) {
    return <div className="text-center py-10 text-tg-hint text-base">{t.no_producers}</div>;
  }

  return (
    <div>
      <h2 className="text-base font-semibold text-tg-hint uppercase mb-3">
        {t.producers} ({producers.length})
      </h2>
      {/* 2-column grid — taller cards for ~4 visible per screen */}
      <div className="grid grid-cols-2 gap-3">
        {producers.map(prod => (
          <button
            key={prod.id}
            onClick={() => onSelectProducer(prod)}
            className="bg-tg-secondary rounded-xl p-5 text-left active:scale-95 transition-transform flex flex-col justify-between min-h-[110px]"
          >
            <div className="text-base font-semibold leading-snug">{prod.name}</div>
            <div className="text-sm text-tg-hint mt-3">
              {prod.product_count} {t.products_count}
            </div>
          </button>
        ))}
      </div>
    </div>
  );
}
