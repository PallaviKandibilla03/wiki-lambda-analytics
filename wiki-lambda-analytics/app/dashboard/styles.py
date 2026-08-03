"""
Custom CSS styling helpers for the Streamlit dashboard.
"""

CUSTOM_CSS = """
<style>
    .metric-card {
        background-color: #1E1E2E;
        border-radius: 10px;
        padding: 1rem;
        text-align: center;
        border: 1px solid #313244;
    }
    .status-badge-healthy {
        background-color: #1e4620;
        color: #7ee787;
        padding: 4px 12px;
        border-radius: 12px;
        font-weight: 600;
        display: inline-block;
    }
    .status-badge-degraded {
        background-color: #4a1e1e;
        color: #ff7b72;
        padding: 4px 12px;
        border-radius: 12px;
        font-weight: 600;
        display: inline-block;
    }
    .section-header {
        border-bottom: 2px solid #313244;
        padding-bottom: 0.3rem;
        margin-top: 1.5rem;
        margin-bottom: 0.8rem;
    }
    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;
    }
</style>
"""


def inject_custom_css(st) -> None:
    """Inject the dashboard's custom CSS into the given Streamlit module/context."""
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


def status_badge_html(is_healthy: bool, label: str) -> str:
    """Return an HTML span styled as a green/red status badge."""
    css_class = "status-badge-healthy" if is_healthy else "status-badge-degraded"
    icon = "\u2713" if is_healthy else "\u2717"
    return f'<span class="{css_class}">{icon} {label}</span>'
