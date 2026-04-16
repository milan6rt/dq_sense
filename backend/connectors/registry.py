"""
Connector registry — single source of truth for all available connectors.
Add a new connector here and it automatically appears in the UI and API.
"""

from typing import Dict, Type
from .base import BaseConnector, ConnectorType
from .postgresql import PostgreSQLConnector
from .fabric import FabricConnector

# Registry: connector_type → connector class
CONNECTOR_REGISTRY: Dict[str, Type[BaseConnector]] = {
    ConnectorType.POSTGRESQL: PostgreSQLConnector,
    ConnectorType.FABRIC:     FabricConnector,
}


def get_connector_class(connector_type: str) -> Type[BaseConnector]:
    """Return the connector class for a given type string. Raises ValueError if unknown."""
    cls = CONNECTOR_REGISTRY.get(connector_type)
    if not cls:
        available = list(CONNECTOR_REGISTRY.keys())
        raise ValueError(f"Unknown connector type '{connector_type}'. Available: {available}")
    return cls


def build_connector(connector_type: str, config: dict) -> BaseConnector:
    """Instantiate and return a connector with the provided (decrypted) config."""
    cls = get_connector_class(connector_type)
    return cls(config)


def list_connector_types() -> list:
    """Return metadata for all registered connectors (for UI)."""
    return [
        {
            "type":          ct,
            "display_name":  cls.display_name,
            "icon":          cls.icon,
            "fields":        cls.required_fields(),
        }
        for ct, cls in CONNECTOR_REGISTRY.items()
    ]
