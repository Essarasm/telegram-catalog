import { useState, useEffect } from 'react';
import { fetchProducers } from '../utils/api';
import t from '../i18n/uz.json';

export default function ProducersPage({ category, onSelectProducer }) {
  const [producers, setProducers] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!category?.id) return;
    setLoading(true);
    fetchProducers(category.id).then(data => {
      setProducers(data);
      setLoading(false);
    });
  }, [category?.id]);

  if (loading) {
    return <div className="text-center py-10 text-tg-hint">{t.loading}</div>;
  }

  if (producers.length === 0) {
    return <div className="text-center py-10 text-tg-hint">{t.no_producers}</div>;
  }

  // If only one producer, could auto-navigate, but showing the list
  // gives users confirmation of where they are
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
