import { useState, useEffect } from 'react';
import { fetchProducers } from '../utils/api';
import t from '../i18n/uz.json';

export default function ProducersPage({ category, onSelectProducer }) {
  const [producers, setProducers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!category?.id) return;
    setLoading(true);
    setError(null);
    fetchProducers(category.id)
      .then(data => {
        if (Array.isArray(data)) {
          setProducers(data);
        } else {
          setError('API returned unexpected format: ' + JSON.stringify(data).slice(0, 100));
          setProducers([]);
        }
        setLoading(false);
      })
      .catch(err => {
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
