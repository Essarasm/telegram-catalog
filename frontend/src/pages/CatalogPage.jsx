import { useState, useEffect } from 'react';
import { fetchCategories } from '../utils/api';
import t from '../i18n/uz.json';

export default function CatalogPage({ onSelectCategory, onSearch }) {
  const [categories, setCategories] = useState([]);
  const [loading, setLoading] = useState(true);
  const [searchInput, setSearchInput] = useState('');

  useEffect(() => {
    fetchCategories().then(data => {
      setCategories(data);
      setLoading(false);
    });
  }, []);

  const handleSearch = (e) => {
    e.preventDefault();
    if (searchInput.trim()) {
      onSearch(searchInput.trim());
    }
  };

  const icons = t.category_icons;

  if (loading) {
    return <div className="text-center py-10 text-tg-hint">{t.loading}</div>;
  }

  return (
    <div>
      {/* Search */}
      <form onSubmit={handleSearch} className="mb-4">
        <div className="relative">
          <input
            type="text"
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            placeholder={t.search}
            className="w-full bg-tg-secondary rounded-xl px-4 py-3 pr-10 text-sm outline-none focus:ring-2 focus:ring-tg-button"
          />
          <button type="submit" className="absolute right-3 top-1/2 -translate-y-1/2 text-tg-hint">
            🔍
          </button>
        </div>
      </form>

      {/* Categories grid */}
      <h2 className="text-sm font-semibold text-tg-hint uppercase mb-3">{t.categories}</h2>
      <div className="grid grid-cols-2 gap-3">
        {categories.map(cat => (
          <button
            key={cat.id}
            onClick={() => onSelectCategory(cat)}
            className="bg-tg-secondary rounded-xl p-4 text-left hover:opacity-80 active:scale-95 transition-transform"
          >
            <div className="text-2xl mb-2">{icons[cat.name] || '📦'}</div>
            <div className="text-sm font-medium leading-tight">{cat.name}</div>
            <div className="text-xs text-tg-hint mt-1">
              {cat.product_count} {t.products_count}
            </div>
          </button>
        ))}
      </div>
    </div>
  );
}
