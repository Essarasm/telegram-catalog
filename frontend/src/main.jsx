import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App'
import './index.css'

// Global error boundary for uncaught React errors
class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { error: null };
  }
  static getDerivedStateFromError(error) {
    return { error: error.message || String(error) };
  }
  componentDidCatch(error, info) {
    console.error('React Error:', error, info);
  }
  render() {
    if (this.state.error) {
      return (
        <div style={{ padding: 20, color: 'red', fontFamily: 'monospace', fontSize: 14 }}>
          <h2>App Error (v3)</h2>
          <p>{this.state.error}</p>
          <button onClick={() => window.location.reload()} style={{ marginTop: 10, padding: '8px 16px' }}>
            Reload
          </button>
        </div>
      );
    }
    return this.props.children;
  }
}

// Catch any uncaught JS errors globally
window.onerror = (msg, src, line, col, err) => {
  const el = document.getElementById('js-error-log');
  if (el) el.textContent += `\nERR: ${msg} (${src}:${line})`;
};

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <ErrorBoundary>
      <App />
    </ErrorBoundary>
  </React.StrictMode>
)
