import { useState, useEffect, useRef, useCallback } from 'react';
import { fetchCategories, fetchSearchSuggestions, formatPrice, getImageUrl } from '../utils/api';
import t from '../i18n/uz.json';

export default function CatalogPage({ onSelectCategory, onSearch, onSelectProduct, approved }) {
  const [categories, setCategories] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [searchInput, setSearchInput] = useState('');
  const [suggestions, setSuggestions] = useState({ suggestions: [], total_matches: 0 });
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [selectedIdx, setSelectedIdx] = useState(-1);
  const suggestTimer = useRef(null);
  const inputRef = useRef(null);
  const suggestionsRef = useRef(null);

  const flatSuggestions = suggestions.suggestions || [];
  const hasAnySuggestions = flatSuggestions.length > 0;

  const loadCategories = useCallback(() => {
    setError(null);
    setLoading(true);
    fetchCategories()
      .then(data => {
        if (Array.isArray(data)) {
          setCategories(data);
        } else {
          setError('unexpected:' + JSON.stringify(data).slice(0, 100));
        }
        setLoading(false);
      })
      .catch(err => {
        setError('fetch:' + (err.message || String(err)));
        setLoading(false);
      });
  }, []);

  useEffect(() => {
    loadCategories();
  }, [loadCategories]);

  // Debounced suggestion fetching
  const fetchSuggestions = useCallback((query) => {
    if (suggestTimer.current) clearTimeout(suggestTimer.current);
    if (!query || query.length < 2) {
      setSuggestions({ suggestions: [], total_matches: 0 });
      setShowSuggestions(false);
      return;
    }
    suggestTimer.current = setTimeout(async () => {
      const results = await fetchSearchSuggestions(query);
      setSuggestions(results);
      setShowSuggestions((results.suggestions?.length || 0) > 0);
      setSelectedIdx(-1);
    }, 200);
  }, []);

  const handleSuggestionProduct = (s) => {
    setShowSuggestions(false);
    if (onSelectProduct) {
      // Build a product-like object the detail page can consume
      onSelectProduct({
        id: s.id,
        name: s.name_cyrillic || s.text,
        name_display: s.name_display || s.text,
        producer_name: s.producer,
        price_uzs: s.price_uzs,
        price_usd: s.price_usd,
        unit: s.unit,
        stock_status: s.stock_status,
        image_path: s.image_path,
      });
    } else {
      // Fallback: run the search if onSelectProduct isn't wired
      onSearch(s.text);
    }
  };

  const handleSeeAll = () => {
    if (!searchInput.trim()) return;
    setShowSuggestions(false);
    onSearch(searchInput.trim());
  };

  const handleInputChange = (e) => {
    const val = e.target.value;
    setSearchInput(val);
    fetchSuggestions(val.trim());
  };

  const handleSearch = (e) => {
    e.preventDefault();
    if (searchInput.trim()) {
      setShowSuggestions(false);
      onSearch(searchInput.trim());
    }
  };

  const handleKeyDown = (e) => {
    if (!showSuggestions || flatSuggestions.length === 0) return;
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setSelectedIdx(prev => Math.min(prev + 1, flatSuggestions.length - 1));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setSelectedIdx(prev => Math.max(prev - 1, -1));
    } else if (e.key === 'Enter' && selectedIdx >= 0) {
      e.preventDefault();
      handleSuggestionProduct(flatSuggestions[selectedIdx]);
    }
  };

  const handleBlur = () => {
    // Delay to allow click on suggestion
    setTimeout(() => setShowSuggestions(false), 200);
  };

  const icons = t.category_icons;

  if (error) {
    return (
      <div className="text-center py-10 px-4">
        <div className="text-tg-text text-base mb-4">{t.error_connection}</div>
        <button
          onClick={loadCategories}
          className="px-6 py-2 bg-tg-button text-tg-button-text rounded-lg text-sm font-medium"
        >
          {t.error_retry}
        </button>
      </div>
    );
  }

  if (loading) {
    return <div className="text-center py-10 text-tg-hint">{t.loading}</div>;
  }

  return (
    <div>
      {/* Search with autocomplete */}
      <form onSubmit={handleSearch} className="mb-4 relative">
        <div className="relative">
          <input
            ref={inputRef}
            type="text"
            value={searchInput}
            onChange={handleInputChange}
            onKeyDown={handleKeyDown}
            onFocus={() => { if (flatSuggestions.length > 0) setShowSuggestions(true); }}
            onBlur={handleBlur}
            placeholder={t.search}
            className="w-full bg-tg-secondary rounded-xl px-4 py-3 pr-10 text-sm outline-none focus:ring-2 focus:ring-tg-button"
          />
          <button type="submit" className="absolute right-3 top-1/2 -translate-y-1/2 text-tg-hint">
            🔍
          </button>
        </div>

        {/* Autocomplete dropdown — single list, 1C Cyrillic names, rich rows */}
        {showSuggestions && hasAnySuggestions && (
          <div
            ref={suggestionsRef}
            className="absolute left-0 right-0 top-full mt-1 bg-tg-secondary rounded-xl shadow-lg z-50 overflow-hidden border border-tg-hint/20"
          >
            {flatSuggestions.map((s, i) => (
              <SuggestionRow
                key={`${s.id}-${i}`}
                s={s}
                selected={i === selectedIdx}
                approved={approved}
                onClick={() => handleSuggestionProduct(s)}
              />
            ))}
            {/* "See all X results" footer */}
            <button
              type="button"
              onMouseDown={(e) => e.preventDefault()}
              onClick={handleSeeAll}
              className="w-full text-center px-4 py-2.5 text-xs font-semibold text-tg-link bg-tg-bg/40 border-t border-tg-hint/10 active:bg-tg-button/10"
            >
              {t.search_see_all ? t.search_see_all.replace('{n}', suggestions.total_matches || flatSuggestions.length) :
                `Barcha natijalar (${suggestions.total_matches || flatSuggestions.length} ta) →`}
            </button>
          </div>
        )}
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


function SuggestionRow({ s, selected, approved, onClick }) {
  const imgUrl = getImageUrl({ image_path: s.image_path, id: s.id });
  const priceStr = approved ? formatPrice(s.price_usd, s.price_uzs) : null;
  const stockBadge = s.stock_status === 'out_of_stock'
    ? { label: t.stock_out_of_stock, className: 'bg-red-500/15 text-red-500' }
    : s.stock_status === 'low_stock'
      ? { label: t.stock_low_stock, className: 'bg-amber-500/15 text-amber-600' }
      : null;

  return (
    <button
      type="button"
      onMouseDown={(e) => e.preventDefault()}
      onClick={onClick}
      className={`w-full text-left px-3 py-2 flex items-center gap-3 transition-colors ${
        selected ? 'bg-tg-button/15' : 'active:bg-tg-button/10'
      }`}
    >
      {/* Thumbnail */}
      <div className="w-10 h-10 rounded-md bg-tg-bg flex items-center justify-center overflow-hidden shrink-0">
        {imgUrl ? (
          <img src={imgUrl} alt="" className="w-full h-full object-contain" loading="lazy" />
        ) : (
          <span className="text-lg opacity-30">📦</span>
        )}
      </div>

      {/* Name + producer */}
      <div className="flex-1 min-w-0">
        <div className="text-sm font-medium truncate leading-snug">{s.text}</div>
        <div className="text-[11px] text-tg-hint truncate leading-tight">
          {s.producer || '\u00A0'}
        </div>
      </div>

      {/* Price + stock + unit */}
      <div className="text-right shrink-0 flex flex-col items-end gap-0.5">
        {priceStr && <div className="text-xs font-semibold text-tg-link whitespace-nowrap">{priceStr}</div>}
        {stockBadge && (
          <span className={`text-[9px] font-semibold px-1 py-px rounded ${stockBadge.className} whitespace-nowrap`}>
            {stockBadge.label}
          </span>
        )}
        {s.unit && !stockBadge && (
          <div className="text-[10px] text-tg-hint whitespace-nowrap">{s.unit}</div>
        )}
      </div>
    </button>
  );
}
