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
        <div className="text-xs text-gray-400 mt-2">category.id={category?.id}</div>
      </div>
    );
  }

  if (loading) {
    return <div className="text-center py-10 text-tg-hint">{t.loading}</div>;
  }

  if (producers.length === 0) {
    return <div className="text-center py-10 text-tg-hint">{t.no_producers}</div>;
  }

  return (
    <div>
      <h2 className="text-sm font-semibold text-tg-hint uppercase mb-3">
        {t.producers} ({producers.length})
      </h2>
      <div className="space-y-2">
        {producers.map(prod => (
          <button
            key={prod.id}
            onClick={() => onSelectProducer(prod)}
            className="w-full bg-tg-secondary rounded-xl p-4 text-left hover:opacity-80 active:scale-[0.98] transition-transform flex items-center justify-between"
          >
            <div>
              <div className="text-sm font-medium">{prod.name}</div>
              <div className="text-xs text-tg-hint mt-0.5">
                {prod.product_count} {t.products_count}
              </div>
            </div>
            <span className="text-tg-hint text-lg">›</span>
          </button>
        ))}
      </div>
    </div>
  );
}
