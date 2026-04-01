import { useState, useEffect, useRef, useCallback } from 'react';
import { fetchCategories, fetchSearchSuggestions } from '../utils/api';
import t from '../i18n/uz.json';

export default function CatalogPage({ onSelectCategory, onSearch }) {
  const [categories, setCategories] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [searchInput, setSearchInput] = useState('');
  const [suggestions, setSuggestions] = useState([]);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [selectedIdx, setSelectedIdx] = useState(-1);
  const suggestTimer = useRef(null);
  const inputRef = useRef(null);
  const suggestionsRef = useRef(null);

  useEffect(() => {
    fetchCategories()
      .then(data => {
        if (Array.isArray(data)) {
          setCategories(data);
        } else {
          setError('Categories API unexpected: ' + JSON.stringify(data).slice(0, 100));
        }
        setLoading(false);
      })
      .catch(err => {
        setError('Fetch error: ' + (err.message || String(err)));
        setLoading(false);
      });
  }, []);

  // Debounced suggestion fetching
  const fetchSuggestions = useCallback((query) => {
    if (suggestTimer.current) clearTimeout(suggestTimer.current);
    if (!query || query.length < 2) {
      setSuggestions([]);
      setShowSuggestions(false);
      return;
    }
    suggestTimer.current = setTimeout(async () => {
      const results = await fetchSearchSuggestions(query);
      setSuggestions(results);
      setShowSuggestions(results.length > 0);
      setSelectedIdx(-1);
    }, 200);
  }, []);

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

  const handleSuggestionClick = (text) => {
    setSearchInput(text);
    setShowSuggestions(false);
    onSearch(text);
  };

  const handleKeyDown = (e) => {
    if (!showSuggestions || suggestions.length === 0) return;
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setSelectedIdx(prev => Math.min(prev + 1, suggestions.length - 1));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setSelectedIdx(prev => Math.max(prev - 1, -1));
    } else if (e.key === 'Enter' && selectedIdx >= 0) {
      e.preventDefault();
      handleSuggestionClick(suggestions[selectedIdx].text);
    }
  };

  const handleBlur = () => {
    // Delay to allow click on suggestion
    setTimeout(() => setShowSuggestions(false), 200);
  };

  const icons = t.category_icons;

  if (error) {
    return (
      <div className="text-center py-10">
        <div className="text-red-500 text-sm font-mono mb-2">CatalogPage Error:</div>
        <div className="text-red-400 text-xs font-mono">{error}</div>
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
            onFocus={() => { if (suggestions.length > 0) setShowSuggestions(true); }}
            onBlur={handleBlur}
            placeholder={t.search}
            className="w-full bg-tg-secondary rounded-xl px-4 py-3 pr-10 text-sm outline-none focus:ring-2 focus:ring-tg-button"
          />
          <button type="submit" className="absolute right-3 top-1/2 -translate-y-1/2 text-tg-hint">
            🔍
          </button>
        </div>

        {/* Autocomplete dropdown */}
        {showSuggestions && suggestions.length > 0 && (
          <div
            ref={suggestionsRef}
            className="absolute left-0 right-0 top-full mt-1 bg-tg-secondary rounded-xl shadow-lg z-50 overflow-hidden border border-tg-hint/20"
          >
            {suggestions.map((s, idx) => (
              <button
                key={idx}
                type="button"
                onMouseDown={(e) => e.preventDefault()}
                onClick={() => handleSuggestionClick(s.text)}
                className={`w-full text-left px-4 py-2.5 text-sm flex items-center gap-2 transition-colors ${
                  idx === selectedIdx ? 'bg-tg-button/15' : 'active:bg-tg-button/10'
                }`}
              >
                <span className="text-tg-hint text-xs">
                  {s.type === 'query' ? '🔍' : '📦'}
                </span>
                <span className="flex-1 truncate">{s.text}</span>
                {s.count && s.count > 1 && (
                  <span className="text-xs text-tg-hint">{s.count}x</span>
                )}
              </button>
            ))}
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
